use axum::{Router, extract::State, http::StatusCode, routing::get};
use fred::prelude::*;
use fred::types::RedisConfig;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::BuildError;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct SwitchMasterEvent {
    sentinel_id: usize,
    new_addr: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    ready: Arc<AtomicBool>,
    prom_handler: PrometheusHandle,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("error,sentinel_bridge=info"));

    tracing_subscriber::fmt()
        .with_target(true)
        .json()
        .flatten_event(true)
        .with_env_filter(env_filter)
        .init();

    let http_addr = env::var("HTTP_BIND_ADDR").unwrap_or("0.0.0.0:8080".to_string());
    let bind_addr = env::var("PROXY_BIND_ADDR").unwrap_or("0.0.0.0:6379".to_string());

    let master_name = env::var("MASTER_NAME")
        .map_err(|_| "FATAL: MASTER_NAME environment variable is missing")?;
    let sentinel_env = env::var("SENTINEL_ADDRS")
        .map_err(|_| "FATAL: SENTINEL_ADDRS environment variable is missing")?;

    let sentinels: Vec<String> = sentinel_env
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if sentinels.is_empty() {
        return Err("FATAL: SENTINEL_ADDRS contains no valid endpoints".into());
    }

    let quorum_size = (sentinels.len() / 2) + 1;

    let metrics_handle = setup_metrics().expect("failed to setup metrics recorder");

    info!(
        bind_addr = bind_addr,
        master_name = master_name,
        quorum_size = quorum_size,
        sentinels_count = sentinels.len(),
        "starting proxy",
    );

    let ready = Arc::new(AtomicBool::new(false));

    tokio::spawn(run_http_server(http_addr, ready.clone(), metrics_handle));

    let verified_master = bootstrap_quorum_master(&sentinels, &master_name, quorum_size).await;

    info!(
        current_master = verified_master.to_string(),
        "bootstrap finished"
    );

    ready.store(true, Ordering::Relaxed);

    let (event_tx, event_rx) = mpsc::channel::<SwitchMasterEvent>(100);
    let (state_tx, state_rx) = watch::channel(verified_master);

    for (id, url) in sentinels.iter().enumerate() {
        let tx = event_tx.clone();
        let name = master_name.clone();
        tokio::spawn(run_sentinel_subscriber(id, url.to_owned(), name, tx));
    }

    tokio::spawn(run_quorum_coordinator(quorum_size, event_rx, state_tx));

    let listener = TcpListener::bind(&bind_addr).await?;
    info!("proxy listening on {}", bind_addr);

    let shutdown_token = CancellationToken::new();

    tokio::spawn(wait_for_shutdown(shutdown_token.clone(), ready));

    let mut connection_tasks = JoinSet::new();

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (client_stream, _) = match accept_result {
                    Ok(val) => val,
                    Err(e) => {
                        error!(error = e.to_string(), "failed to accept connection");
                        continue;
                    }
                };

                let current_state_rx = state_rx.clone();
                let c_token = shutdown_token.clone();

                connection_tasks.spawn(handle_client_connection(c_token, client_stream, current_state_rx));
            }

            _ = shutdown_token.cancelled() => {
                break;
            }
        }
    }

    info!("waiting for active connections to drain...");
    while let Some(_) = connection_tasks.join_next().await {}

    info!("shutting down, bye bye!");
    Ok(())
}

fn setup_metrics() -> Result<PrometheusHandle, BuildError> {
    let metrics_handle = PrometheusBuilder::new()
        .idle_timeout(
            metrics_util::MetricKindMask::ALL,
            Some(std::time::Duration::from_secs(600)),
        )
        .install_recorder()?;

    let upkeep_handle = metrics_handle.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            upkeep_handle.run_upkeep();
        }
    });

    Ok(metrics_handle)
}

async fn run_http_server(
    bind_addr: String,
    ready: Arc<AtomicBool>,
    prom_handler: PrometheusHandle,
) {
    let state = AppState {
        ready,
        prom_handler,
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let addr_clone = bind_addr.clone();
    let listener = TcpListener::bind(addr_clone)
        .await
        .expect("failed to bind http server");

    info!("http server listening on {}", bind_addr);

    if let Err(e) = axum::serve(listener, app).await {
        error!(error = e.to_string(), "http server error");
    }
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn ready_handler(State(state): State<AppState>) -> StatusCode {
    if state.ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics_handler(State(state): State<AppState>) -> String {
    state.prom_handler.render()
}

async fn wait_for_shutdown(token: CancellationToken, ready: Arc<AtomicBool>) {
    let mut sigint =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();

    let mut sigquit = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::quit()).unwrap();

    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

    tokio::select! {
        _ = async {
            sigint.recv().await;
        } => {},
        _ = async {
            sigquit.recv().await;
        } => {},
        _ = async {
            sigterm.recv().await;
        } => {},
    }

    info!("received termination signal");
    ready.store(false, Ordering::Relaxed);

    info!("sleeping for 5s...");

    sleep(Duration::from_secs(5)).await;

    info!("starting shutdown...");
    token.cancel();
}

async fn handle_client_connection(
    c_token: CancellationToken,
    mut client_stream: TcpStream,
    mut state_rx: watch::Receiver<SocketAddr>,
) {
    let master_addr = *state_rx.borrow();

    let backend_label = master_addr.to_string();
    counter!("proxy_connections_total", "backend" => backend_label.clone()).increment(1);
    gauge!("proxy_active_connections", "backend" => backend_label.clone()).increment(1.0);

    match TcpStream::connect(master_addr).await {
        Ok(mut backend_stream) => {
            let _ = client_stream.set_nodelay(true);
            let _ = backend_stream.set_nodelay(true);

            let invalidation_trigger = async {
                let current_addr = *state_rx.borrow_and_update();
                if current_addr != master_addr {
                    return;
                }

                loop {
                    if state_rx.changed().await.is_err() {
                        break;
                    }

                    let new_addr = *state_rx.borrow_and_update();

                    if new_addr != master_addr {
                        break;
                    }
                }
            };

            tokio::select! {
                _ = copy_bidirectional(&mut client_stream, &mut backend_stream) => {
                    counter!("proxy_connections_closed_total",
                        "reason" => "client_disconnect",
                        "backend" => backend_label.clone()
                    ).increment(1);
                }
                _ = invalidation_trigger => {
                    counter!("proxy_connections_closed_total",
                        "reason" => "failover_severed",
                        "backend" => backend_label.clone()
                    ).increment(1);
                }
                _ = c_token.cancelled() => {
                    counter!("proxy_connections_closed_total",
                        "reason" => "graceful_shutdown",
                        "backend" => backend_label.clone()
                    ).increment(1);
                }
            }
        }
        Err(e) => {
            counter!("proxy_backend_connection_errors_total",
                "backend" => backend_label.clone()
            )
            .increment(1);

            error!(
                master_addr = master_addr.to_string(),
                error = e.to_string(),
                "failed to connect to backend",
            )
        }
    }

    gauge!("proxy_active_connections", "backend" => backend_label.clone()).decrement(1.0);
}

/// Concurrently queries all Sentinels and waits until a strict majority agree on the master.
async fn bootstrap_quorum_master(
    sentinels: &[String],
    master_name: &str,
    quorum_size: usize,
) -> SocketAddr {
    loop {
        let mut tasks = JoinSet::new();

        for url in sentinels {
            let url_cloned = url.clone();
            let name_cloned = master_name.to_string();

            tasks.spawn(async move { bootstrap_single_master(&url_cloned, &name_cloned).await });
        }

        let mut master_votes: HashMap<SocketAddr, usize> = HashMap::new();

        while let Some(res) = tasks.join_next().await {
            if let Ok(Ok(addr)) = res {
                let count = master_votes.entry(addr).or_insert(0);
                *count += 1;

                if *count >= quorum_size {
                    return addr;
                }
            }
        }

        error!("failed to reach quorum, retrying in 2 seconds...");
        sleep(Duration::from_secs(2)).await;
    }
}

/// Helper function to query a single Sentinel
async fn bootstrap_single_master(
    sentinel_url: &str,
    master_name: &str,
) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let config = RedisConfig::from_url(sentinel_url)?;
    let client = Builder::from_config(config).build()?;
    client.init().await?;

    let addr_parts: Vec<String> = client
        .custom(
            fred::types::CustomCommand::new_static(
                "SENTINEL",
                fred::types::ClusterHash::Random,
                false,
            ),
            vec!["get-master-addr-by-name", master_name],
        )
        .await?;

    let _ = client.quit().await;

    if addr_parts.len() == 2 {
        let addr: SocketAddr = format!("{}:{}", addr_parts[0], addr_parts[1]).parse()?;
        Ok(addr)
    } else {
        Err("Invalid response format".into())
    }
}

/// Aggregates events from the Sentinel streams and enforces quorum rules.
async fn run_quorum_coordinator(
    quorum: usize,
    mut event_rx: mpsc::Receiver<SwitchMasterEvent>,
    state_tx: watch::Sender<SocketAddr>,
) {
    let mut master_votes: HashMap<SocketAddr, HashSet<usize>> = HashMap::new();

    while let Some(event) = event_rx.recv().await {
        let votes = master_votes.entry(event.new_addr).or_default();
        votes.insert(event.sentinel_id);

        let current_master = *state_tx.borrow();

        if votes.len() >= quorum && current_master != event.new_addr {
            info!(
                current_master = event.new_addr.to_string(),
                old_master = current_master.to_string(),
                "new master elected"
            );
            let _ = state_tx.send(event.new_addr);
            master_votes.clear();
        }
    }
}

async fn run_sentinel_subscriber(
    id: usize,
    url: String,
    master_name: String,
    tx: mpsc::Sender<SwitchMasterEvent>,
) {
    loop {
        let config = match RedisConfig::from_url(&url) {
            Ok(c) => c,
            Err(e) => {
                error!(
                    sentinel = id,
                    error = e.to_string(),
                    "error creating config",
                );
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let mut builder = Builder::from_config(config);
        builder.set_policy(fred::types::ReconnectPolicy::new_constant(0, 0));

        let client = match builder.build_subscriber_client() {
            Ok(c) => c,
            Err(e) => {
                error!(
                    sentinel = id,
                    error = e.to_string(),
                    "error creating client",
                );
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        if let Err(e) = client.init().await {
            error!(sentinel = id, error = e.to_string(), "connection failed",);
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        if let Err(e) = client.subscribe(vec!["+switch-master"]).await {
            error!(
                sentinel = id,
                error = e.to_string(),
                "error setting up subscriber",
            );
            let _ = client.quit().await;
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        let mut message_stream = client.message_rx();

        while let Ok(msg) = message_stream.recv().await {
            let channel = msg.channel.to_string();

            if let Some(payload) = msg.value.as_string() {
                if channel == "+switch-master" {
                    let parts: Vec<&str> = payload.split_whitespace().collect();

                    if parts.len() >= 5 && parts[0] == master_name {
                        let new_ip = parts[3];
                        let new_port = parts[4];
                        if let Ok(new_addr) =
                            format!("{}:{}", new_ip, new_port).parse::<SocketAddr>()
                        {
                            let _ = tx
                                .send(SwitchMasterEvent {
                                    sentinel_id: id,
                                    new_addr,
                                })
                                .await;
                        }
                    }
                }
            }
        }

        error!(sentinel = id, "disconnected, reconnecting...",);
        let _ = client.quit().await;
        sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_coordinator_unpauses_on_quorum_switch_master() {
        let dummy_ip = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let dummy_ip2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 6379);

        let (event_tx, event_rx) = mpsc::channel::<SwitchMasterEvent>(100);
        let (state_tx, mut state_rx) = watch::channel(dummy_ip);

        tokio::spawn(run_quorum_coordinator(2, event_rx, state_tx));

        event_tx
            .send(SwitchMasterEvent {
                sentinel_id: (0),
                new_addr: (dummy_ip2),
            })
            .await
            .unwrap();

        assert_eq!(*state_rx.borrow(), dummy_ip);

        event_tx
            .send(SwitchMasterEvent {
                sentinel_id: (1),
                new_addr: (dummy_ip2),
            })
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), state_rx.changed()).await;

        assert!(result.is_ok(), "Coordinator failed to update state in time");

        assert_eq!(*state_rx.borrow(), dummy_ip2);
    }

    #[tokio::test]
    async fn test_coordinator_unpauses_on_non_quorum_switch_master_ip() {
        let dummy_ip = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let dummy_ip2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 6379);
        let dummy_ip3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 6379);

        let (event_tx, event_rx) = mpsc::channel::<SwitchMasterEvent>(100);
        let (state_tx, state_rx) = watch::channel(dummy_ip);

        tokio::spawn(run_quorum_coordinator(2, event_rx, state_tx));

        event_tx
            .send(SwitchMasterEvent {
                sentinel_id: (0),
                new_addr: (dummy_ip2),
            })
            .await
            .unwrap();

        assert_eq!(*state_rx.borrow(), dummy_ip);

        event_tx
            .send(SwitchMasterEvent {
                sentinel_id: (1),
                new_addr: (dummy_ip3),
            })
            .await
            .unwrap();

        assert!(
            !state_rx.has_changed().unwrap(),
            "Coordinator should not have updated the state"
        );

        assert_eq!(*state_rx.borrow(), dummy_ip);
    }
}
