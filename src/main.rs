use arc_swap::{ArcSwap, ArcSwapAny};
use axum::{Router, http::StatusCode, routing::get};
use clap::Parser;
use metrics::{Counter, Gauge, counter, describe_counter, describe_gauge, gauge};
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use redis::RedisError;
use redis::aio::{ConnectionManager, ConnectionManagerConfig, PubSub};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader, copy_buf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{OnceCell, mpsc, mpsc::Receiver, mpsc::Sender};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "sentinel-bridge", version, about)]
struct Config {
    #[arg(long, env = "LOG_LEVEL", default_value = "INFO")]
    log_level: tracing::Level,

    #[arg(long, env = "HTTP_BIND_ADDR", default_value = "0.0.0.0:8080")]
    http_bind_addr: String,

    #[arg(long, env = "PROXY_BIND_ADDR", default_value = "0.0.0.0:6379")]
    proxy_bind_addr: String,

    #[arg(long, env = "MASTER_NAME")]
    master_name: String,

    #[arg(long, env = "SENTINEL_ADDRS")]
    sentinel_addrs: String,

    #[arg(long, env = "BACKEND_DIAL_TIMEOUT", default_value = "3s", value_parser = humantime::parse_duration)]
    backend_dial_timeout: Duration,

    #[arg(long, env = "TERMINATION_GRACE_PERIOD", default_value = "5s", value_parser = humantime::parse_duration)]
    termination_grace_period: Duration,

    #[arg(long, env = "CONNECTION_DRAIN_TIMEOUT", default_value = "30s", value_parser = humantime::parse_duration)]
    connection_drain_timeout: Duration,

    #[arg(long, env = "RECONCILE_INTERVAL", default_value = "10s", value_parser = humantime::parse_duration)]
    reconcile_interval: Duration,
}

#[derive(Debug)]
struct SwitchMasterEvent {
    sentinel_id: usize,
    new_addr: SocketAddr,
}

#[derive(Clone)]
struct ProxyMetrics {
    connections_total: Counter,
    active_connections: Gauge,
    backend_connection_errors: Counter,
    closed_graceful: Counter,
    closed_severed: Counter,
    closed_client_disconnect: Counter,
    closed_server_disconnect: Counter,
}

struct ProxyState {
    addr: SocketAddr,
    ctx: CancellationToken,
    metrics: ProxyMetrics,
}

impl ProxyState {
    fn new(addr: SocketAddr) -> Self {
        let addr_str = addr.to_string();
        Self {
            addr,
            ctx: CancellationToken::new(),
            metrics: ProxyMetrics {
                connections_total: counter!("proxy_connections_total", "backend" => addr_str.clone()),
                active_connections: gauge!("proxy_active_connections", "backend" => addr_str.clone()),
                backend_connection_errors: counter!("proxy_backend_connection_errors_total", "backend" => addr_str.clone()),
                closed_graceful: counter!("proxy_connections_closed_total", "backend" => addr_str.clone(), "reason" => "graceful_shutdown"),
                closed_severed: counter!("proxy_connections_closed_total", "backend" => addr_str.clone(), "reason" => "failover_severed"),
                closed_client_disconnect: counter!("proxy_connections_closed_total", "backend" => addr_str.clone(), "reason" => "client_disconnect"),
                closed_server_disconnect: counter!("proxy_connections_closed_total", "backend" => addr_str, "reason" => "server_disconnect"),
            },
        }
    }
}

struct Sentinel {
    client: redis::Client,
    conn: OnceCell<ConnectionManager>,
}

impl Sentinel {
    fn new(addr: &str) -> Result<Self, RedisError> {
        return Ok(Self {
            client: redis::Client::open(addr)?,
            conn: OnceCell::new(),
        });
    }

    async fn conn(&self) -> Result<ConnectionManager, RedisError> {
        return self
            .conn
            .get_or_try_init(|| {
                let config = ConnectionManagerConfig::new()
                    .set_connection_timeout(Duration::from_secs(1))
                    .set_response_timeout(Duration::from_secs(3))
                    .set_factor(2)
                    .set_max_delay(1000)
                    .set_number_of_retries(3)
                    .set_tcp_settings(
                        redis::io::tcp::TcpSettings::default()
                            .set_user_timeout(Duration::from_secs(2)),
                    );
                self.client.get_connection_manager_with_config(config)
            })
            .await
            .cloned();
    }

    async fn pubsub(&self, channels: &[&str]) -> Result<PubSub, RedisError> {
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut pubsub = self.client.get_async_pubsub().await?;
            pubsub.subscribe(channels).await?;
            Ok(pubsub)
        })
        .await
        .map_err(|_| {
            RedisError::from(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "pubsub connect/subscribe timed out",
            ))
        })?
    }
}

#[derive(Clone, Copy)]
struct Dialer {
    timeout: Duration,
}

impl Dialer {
    async fn dial(&self, ctx: &CancellationToken, addr: SocketAddr) -> std::io::Result<TcpStream> {
        tokio::time::timeout(
            self.timeout,
            ctx.run_until_cancelled(TcpStream::connect(addr)),
        )
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "dial timed out"))?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Interrupted, "context cancelled"))?
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse();

    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_max_level(config.log_level)
        .init();

    let token = CancellationToken::new();
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigquit = signal(SignalKind::quit())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    let token_clone = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigquit.recv() => {}
            _ = sigterm.recv() => {}
        }
        token_clone.cancel();
    });
    let _guard = token.clone().drop_guard();

    if let Err(e) = run(token, config).await {
        error!("{e}");
        std::process::exit(1);
    }
    return Ok(());
}

async fn run(ctx: CancellationToken, config: Config) -> Result<(), Box<dyn std::error::Error>> {
    if config.reconcile_interval.is_zero() {
        return Err("RECONCILE_INTERVAL must be greater than 0".into());
    }

    let mut sentinels: Vec<Arc<Sentinel>> = Vec::new();
    let mut connected_sentinels = 0;
    for s in config.sentinel_addrs.split(',') {
        let addr = s.trim();
        if addr.is_empty() {
            continue;
        }

        let sentinel = Arc::new(
            Sentinel::new(addr).map_err(|e| format!("error parsing sentinel url, err: {e}"))?,
        );

        match sentinel.conn().await {
            Err(e) => warn!(error = %e, "failed initial connect to sentinel"),
            Ok(_) => connected_sentinels += 1,
        }
        sentinels.push(sentinel);
    }

    if sentinels.is_empty() {
        return Err("SENTINEL_ADDRS contains no valid endpoints".into());
    }

    let quorum_size = (sentinels.len() / 2) + 1;

    if connected_sentinels < quorum_size {
        return Err(format!(
            "not enough sentinels available to reach quorum, available: {connected_sentinels}, required_quorum: {quorum_size}, expected_total: {}",
            sentinels.len()
        )
        .into());
    }

    let http_listener = TcpListener::bind(&config.http_bind_addr)
        .await
        .map_err(|e| {
            format!(
                "failed to bind listener, addr: {}, err: {e}",
                config.http_bind_addr
            )
        })?;

    info!("http server listening on {}", config.http_bind_addr);

    let proxy_listener = bind_proxy_listener(config.proxy_bind_addr.clone())
        .await
        .map_err(|e| {
            format!(
                "failed to bind listener, addr: {}, err: {e}",
                config.proxy_bind_addr
            )
        })?;

    info!(
        bind_addr = config.proxy_bind_addr,
        master_name = config.master_name,
        quorum_size = quorum_size,
        sentinels_count = sentinels.len(),
        "starting proxy",
    );

    let ready = Arc::new(AtomicBool::new(false));

    let run_ctx = CancellationToken::new();
    let _guard = run_ctx.clone().drop_guard();

    let prom_handle = setup_metrics(run_ctx.clone())?;

    tokio::spawn(run_http_server(
        run_ctx.clone(),
        http_listener,
        ready.to_owned(),
        prom_handle,
    ));

    let bootstrap_start = tokio::time::Instant::now();
    let verified_master = loop {
        match resolve_master_by_quorum(
            ctx.clone(),
            &sentinels,
            config.master_name.clone(),
            quorum_size,
        )
        .await
        {
            Ok(addr) => break addr,
            Err(err) => {
                if ctx.is_cancelled() {
                    return Err("context cancelled".into());
                }
                if bootstrap_start.elapsed() > Duration::from_secs(30) {
                    return Err(format!(
                        "bootstrap timed out waiting for sentinel quorum (30s): {err}"
                    )
                    .into());
                }
                sleep(Duration::from_secs(2)).await;
            }
        }
    };

    info!(verified_master = %verified_master, "bootstrap finished");

    ready.store(true, Ordering::Relaxed);

    let (event_tx, event_rx) = mpsc::channel::<SwitchMasterEvent>(sentinels.len() * 2);
    let state_pointer = Arc::new(ArcSwap::from_pointee(ProxyState::new(verified_master)));

    for (i, s) in sentinels.iter().enumerate() {
        tokio::spawn(run_sentinel_subscriber(
            run_ctx.clone(),
            i,
            s.clone(),
            config.master_name.clone(),
            event_tx.clone(),
        ));
    }

    tokio::spawn(run_quorum_coordinator(
        run_ctx.clone(),
        quorum_size,
        event_rx,
        state_pointer.clone(),
    ));

    tokio::spawn(run_reconciliation_loop(
        run_ctx.clone(),
        sentinels,
        config.master_name.clone(),
        quorum_size,
        event_tx,
        state_pointer.clone(),
        config.reconcile_interval,
    ));

    let backend_dialer = Dialer {
        timeout: config.backend_dial_timeout,
    };

    let accept_ctx = CancellationToken::new();
    let accept_ctx_clone = accept_ctx.clone();
    let ctx_clone = ctx.clone();
    let run_ctx_clone = run_ctx.clone();

    let tracker = TaskTracker::new();
    let tracker_clone = tracker.clone();

    let accept_task = tokio::spawn(async move {
        while let Some(accept_result) = accept_ctx_clone
            .run_until_cancelled(proxy_listener.accept())
            .await
        {
            let (client_stream, _) = match accept_result {
                Ok(val) => val,
                Err(e) => {
                    error!(error = %e, "failed to accept connection");
                    sleep(Duration::from_millis(10)).await;
                    continue;
                }
            };

            tracker_clone.spawn(handle_client_connection(
                ctx_clone.clone(),
                run_ctx_clone.clone(),
                backend_dialer,
                client_stream,
                state_pointer.clone(),
            ));
        }
    });

    ctx.cancelled().await;

    info!("received termination signal");
    ready.store(false, Ordering::Relaxed);

    info!(
        period_ms = config.termination_grace_period.as_millis() as u64,
        "entering graceful shutdown delay"
    );
    sleep(config.termination_grace_period).await;

    info!("starting shutdown...");
    accept_ctx.cancel();
    accept_task.await?;
    tracker.close();

    info!("waiting for active connections to drain...");
    if tokio::time::timeout(config.connection_drain_timeout, tracker.wait())
        .await
        .is_err()
    {
        warn!(
            timeout_ms = config.connection_drain_timeout.as_millis() as u64,
            "connection drain timeout reached, closing active connections..."
        );
        run_ctx.cancel();
        tracker.wait().await;
    }

    info!("shutting down, bye bye!");
    return Ok(());
}

fn setup_metrics(ctx: CancellationToken) -> Result<PrometheusHandle, BuildError> {
    let prom_handle = PrometheusBuilder::new().install_recorder()?;

    describe_counter!(
        "proxy_connections_total",
        "Total number of connections opened to the backend"
    );
    describe_counter!(
        "proxy_connections_closed_total",
        "Total number of connections closed"
    );
    describe_counter!(
        "proxy_backend_connection_errors_total",
        "Total number of failed connection attempts to backend"
    );
    describe_gauge!(
        "proxy_active_connections",
        "Current number of active connections to the backend"
    );

    let upkeep_handle = prom_handle.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        ticker.tick().await;
        while ctx.run_until_cancelled(ticker.tick()).await.is_some() {
            upkeep_handle.run_upkeep();
        }
    });

    return Ok(prom_handle);
}

async fn bind_proxy_listener(bind_addr: String) -> Result<TcpListener, Box<dyn std::error::Error>> {
    let proxy_addr: SocketAddr = bind_addr.parse()?;
    let socket = if proxy_addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(proxy_addr)?;
    let proxy_listener = socket.listen(4096)?; // Set accept queue to 4096 instead of default 128
    return Ok(proxy_listener);
}

async fn run_http_server(
    ctx: CancellationToken,
    listener: TcpListener,
    ready: Arc<AtomicBool>,
    prom_handle: PrometheusHandle,
) {
    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route(
            "/ready",
            get(move || async move {
                if ready.load(Ordering::Relaxed) {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route("/metrics", get(move || async move { prom_handle.render() }));

    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(ctx.cancelled_owned())
        .await
    {
        error!(error = %e, "http server error");
    }
}

async fn handle_client_connection(
    ctx: CancellationToken,
    run_ctx: CancellationToken,
    dialer: Dialer,
    mut client_stream: TcpStream,
    state_pointer: Arc<ArcSwapAny<Arc<ProxyState>>>,
) {
    let current_state = state_pointer.load();
    let master_addr = current_state.addr;
    let metrics = &current_state.metrics;

    let mut backend_stream = match dialer.dial(&current_state.ctx, master_addr).await {
        Ok(val) => val,
        Err(e) => {
            metrics.backend_connection_errors.increment(1);
            error!(%master_addr, error = %e, "failed to connect to backend");
            return;
        }
    };

    let _ = client_stream.set_nodelay(true);
    let _ = backend_stream.set_nodelay(true);

    metrics.connections_total.increment(1);
    metrics.active_connections.increment(1.0);

    let (client_read, mut client_write) = client_stream.split();
    let (backend_read, mut backend_write) = backend_stream.split();

    let mut client_read = BufReader::with_capacity(2048, client_read);
    let mut backend_read = BufReader::with_capacity(4096, backend_read);

    let mut client_to_backend = std::pin::pin!(async {
        let _ = copy_buf(&mut client_read, &mut backend_write).await;
        let _ = backend_write.shutdown().await;
    });

    let mut backend_to_client = std::pin::pin!(async {
        let _ = copy_buf(&mut backend_read, &mut client_write).await;
        let _ = client_write.shutdown().await;
    });

    tokio::select! {
        _ = current_state.ctx.cancelled() => {
            metrics.closed_severed.increment(1);
        }
        _ = run_ctx.cancelled() => {
            metrics.closed_severed.increment(1);
        }
        backend_exit = async {
            tokio::select! {
                _ = &mut client_to_backend => {
                    backend_to_client.await;
                    false
                }
                _ = &mut backend_to_client => {
                    client_to_backend.await;
                    true
                }
            }
        } => {
            if ctx.is_cancelled() {
                metrics.closed_graceful.increment(1);
            } else if backend_exit {
                metrics.closed_server_disconnect.increment(1);
            } else {
                metrics.closed_client_disconnect.increment(1);
            }
        }
    }

    metrics.active_connections.decrement(1.0);
}

async fn run_quorum_coordinator(
    ctx: CancellationToken,
    quorum_size: usize,
    mut switch_event_receiver: Receiver<SwitchMasterEvent>,
    state_pointer: Arc<ArcSwapAny<Arc<ProxyState>>>,
) {
    struct VoteEntry {
        addr: SocketAddr,
        t: tokio::time::Instant,
    }

    let mut sentinel_votes: HashMap<usize, VoteEntry> = HashMap::new();

    while let Some(Some(event)) = ctx.run_until_cancelled(switch_event_receiver.recv()).await {
        let current_state = state_pointer.load();

        if current_state.addr == event.new_addr {
            sentinel_votes.remove(&event.sentinel_id);
            continue;
        }

        let now = tokio::time::Instant::now();
        sentinel_votes.insert(
            event.sentinel_id,
            VoteEntry {
                addr: event.new_addr,
                t: now,
            },
        );

        let mut vote_count = 0;
        sentinel_votes.retain(|_, v| {
            if now.duration_since(v.t) > Duration::from_secs(120) {
                return false; // delete expired entry
            }
            if v.addr == event.new_addr {
                vote_count += 1;
            }
            true // keep entry
        });

        if vote_count < quorum_size || current_state.addr == event.new_addr {
            continue;
        }

        info!(current_master = %event.new_addr, old_master = %current_state.addr, "new master elected");

        let new_state = ProxyState::new(event.new_addr);
        state_pointer.store(Arc::new(new_state));

        current_state.ctx.cancel();

        sentinel_votes.clear();
    }
}

async fn run_sentinel_subscriber(
    ctx: CancellationToken,
    id: usize,
    sentinel: Arc<Sentinel>,
    master_name: String,
    switch_event_sender: Sender<SwitchMasterEvent>,
) {
    const DEFAULT_BACKOFF: Duration = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_secs(5);
    let mut backoff = DEFAULT_BACKOFF;

    loop {
        let pubsub = match sentinel.pubsub(&["+new-epoch", "+switch-master"]).await {
            Ok(x) => x,
            Err(e) => {
                error!(sentinel = id, error = %e, "error creating pubsub connection");
                tokio::select! {
                    _ = sleep(backoff) => {}
                    _ = ctx.cancelled() => return,
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        let start_time = tokio::time::Instant::now();
        let (mut sink, mut stream) = pubsub.split();
        let mut ping_ticker = tokio::time::interval(Duration::from_secs(5));
        ping_ticker.tick().await;

        loop {
            let msg = tokio::select! {
                _ = ctx.cancelled() => return,
                _ = ping_ticker.tick() => {
                    match tokio::time::timeout(Duration::from_secs(1), sink.ping::<()>()).await {
                        Ok(Ok(())) => continue,
                        Ok(Err(_)) | Err(_) => break, // connection dead, reconnect
                    }
                }
                maybe_msg = stream.next() => match maybe_msg {
                    Some(m) => m,
                    None => break, // stream disconnected
                },
            };

            let Ok(payload) = msg.get_payload::<String>() else {
                continue;
            };

            match msg.get_channel_name() {
                "+new-epoch" => {
                    info!(message = payload, "new epoch detected");
                }
                "+switch-master" => {
                    // Format: <master-name> <old-ip> <old-port> <new-ip> <new-port>
                    let mut parts = payload.split_whitespace();
                    let (Some(name), Some(ip_str), Some(port_str)) =
                        (parts.next(), parts.nth(2), parts.next())
                    else {
                        error!(
                            sentinel = id,
                            payload, "unexpected format of +switch-master event payload"
                        );
                        continue;
                    };

                    if name != master_name {
                        continue;
                    }

                    let ip = match ip_str.parse::<IpAddr>() {
                        Ok(x) => x,
                        Err(e) => {
                            error!(sentinel = id, error = %e, "failed to parse new master ip");
                            continue;
                        }
                    };
                    let port = match port_str.parse::<u16>() {
                        Ok(x) => x,
                        Err(e) => {
                            error!(sentinel = id, error = %e, "failed to parse new master port");
                            continue;
                        }
                    };

                    let new_addr = SocketAddr::new(ip, port);
                    match ctx
                        .run_until_cancelled(switch_event_sender.send(SwitchMasterEvent {
                            sentinel_id: id,
                            new_addr,
                        }))
                        .await
                    {
                        Some(Ok(())) => {}
                        Some(Err(_)) | None => return, // channel closed or ctx cancelled
                    }
                }
                _ => {}
            }
        }

        if start_time.elapsed() > MAX_BACKOFF {
            backoff = DEFAULT_BACKOFF;
        }

        error!(
            sentinel = id,
            reconnecting_in_ms = backoff.as_millis() as u64,
            "sentinel subscriber disconnected"
        );

        tokio::select! {
            _ = sleep(backoff) => {}
            _ = ctx.cancelled() => return,
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn run_reconciliation_loop(
    ctx: CancellationToken,
    sentinels: Vec<Arc<Sentinel>>,
    master_name: String,
    quorum_size: usize,
    switch_event_sender: Sender<SwitchMasterEvent>,
    state_pointer: Arc<ArcSwapAny<Arc<ProxyState>>>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // consume the immediate t=0 tick

    while ctx.run_until_cancelled(ticker.tick()).await.is_some() {
        let current_quorum_master = match resolve_master_by_quorum(
            ctx.clone(),
            &sentinels,
            master_name.clone(),
            quorum_size,
        )
        .await
        {
            Ok(master) => master,
            Err(e) => {
                info!(error = %e, "background master reconciliation failed to reach quorum");
                continue;
            }
        };

        let current_state = state_pointer.load();
        if current_state.addr == current_quorum_master {
            continue;
        }

        warn!(
            current_proxy_target = %current_state.addr,
            actual_quorum_master = %current_quorum_master,
            "background poller detected master drift (missed pub/sub event)"
        );

        for i in 0..sentinels.len() {
            let event = SwitchMasterEvent {
                sentinel_id: i,
                new_addr: current_quorum_master,
            };
            match ctx
                .run_until_cancelled(switch_event_sender.send(event))
                .await
            {
                Some(Ok(())) => {}
                Some(Err(_)) | None => return, // channel closed or ctx cancelled
            }
        }
    }
}

async fn resolve_master_by_quorum(
    ctx: CancellationToken,
    sentinels: &[Arc<Sentinel>],
    master_name: String,
    quorum_size: usize,
) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let mut tasks = JoinSet::new();

    for (i, s) in sentinels.iter().enumerate() {
        let ctx = ctx.clone();
        let s = s.clone();
        let master_name = master_name.clone();
        tasks.spawn(async move { (i, query_sentinel_for_master(ctx, s, master_name).await) });
    }

    let mut master_votes: HashMap<SocketAddr, usize> = HashMap::new();

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok((_, Ok(addr))) => {
                let count = master_votes.entry(addr).or_insert(0);
                *count += 1;
                if *count >= quorum_size {
                    tasks.detach_all();
                    return Ok(addr);
                }
            }
            Ok((i, Err(e))) => {
                debug!(sentinel = i, error = %e, "error getting master address");
            }
            Err(e) => {
                error!(error = %e, "sentinel query task panicked");
            }
        }
    }

    Err("failed to reach quorum".into())
}

async fn query_sentinel_for_master(
    ctx: CancellationToken,
    sentinel: Arc<Sentinel>,
    master_name: String,
) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let query = async {
        let mut conn = sentinel.conn().await?;

        let (ip, port): (String, u16) = redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(&master_name)
            .query_async(&mut conn)
            .await?;

        let addr = SocketAddr::new(ip.parse::<IpAddr>()?, port);
        return Ok(addr);
    };

    return ctx
        .run_until_cancelled(query)
        .await
        .ok_or("context cancelled")?;
}
