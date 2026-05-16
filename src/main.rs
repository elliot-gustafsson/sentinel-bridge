use fred::prelude::*;
use fred::types::RedisConfig;
use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};

#[derive(Clone, Copy, Debug, PartialEq)]
enum ProxyState {
    Paused,
    Active(SocketAddr),
}

#[derive(Debug)]
enum SentinelEvent {
    NewEpoch {
        sentinel_id: usize,
        epoch: u64,
    },
    SwitchMaster {
        sentinel_id: usize,
        new_addr: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = env::var("PROXY_BIND_ADDR").unwrap_or("127.0.0.1:8080".to_string());

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

    println!(
        "Starting Proxy... Bind Address: {}. Master Name: {}. Quorum Size:  {} out of {}",
        bind_addr,
        master_name,
        quorum_size,
        sentinels.len()
    );

    let verified_master = bootstrap_quorum_master(&sentinels, &master_name, quorum_size).await;
    println!("bootstrapped current master: {}", verified_master);

    let initial_state = ProxyState::Active(verified_master);

    let (event_tx, event_rx) = mpsc::channel::<SentinelEvent>(100);
    let (state_tx, state_rx) = watch::channel(initial_state);

    for (id, url) in sentinels.into_iter().enumerate() {
        let tx = event_tx.clone();
        let name = master_name.clone();
        tokio::spawn(async move {
            run_sentinel_subscriber(id, url, name, tx).await;
        });
    }

    tokio::spawn(async move {
        run_quorum_coordinator(quorum_size, event_rx, state_tx).await;
    });

    let listener = TcpListener::bind(&bind_addr).await?;
    println!("ready to accept connections on {}", bind_addr);

    loop {
        let (mut client_stream, _) = match listener.accept().await {
            Ok(val) => val,
            Err(e) => {
                eprintln!("failed to accept connection: {}", e);
                continue;
            }
        };

        let mut current_state_rx = state_rx.clone();

        tokio::spawn(async move {
            // hang incoming connections until the proxy is Active
            let master_addr = loop {
                let state = *current_state_rx.borrow();
                match state {
                    ProxyState::Active(addr) => break addr,
                    ProxyState::Paused => {
                        if current_state_rx.changed().await.is_err() {
                            return;
                        }
                    }
                }
            };

            // connect to the backend master
            match TcpStream::connect(master_addr).await {
                Ok(mut backend_stream) => {
                    let _ = client_stream.set_nodelay(true);
                    let _ = backend_stream.set_nodelay(true);

                    let invalidation_trigger = async {
                        let mut rx = current_state_rx.clone();
                        loop {
                            if rx.changed().await.is_err() {
                                break;
                            }

                            let state = *rx.borrow();
                            let should_drain = match state {
                                ProxyState::Paused => true, // +new-epoch hit quorum
                                ProxyState::Active(new_addr) if new_addr != master_addr => true,
                                _ => false,
                            };

                            if should_drain {
                                // election event detected
                                sleep(Duration::from_millis(100)).await;
                                break; // exit and drop sockets
                            }
                        }
                    };

                    tokio::select! {
                        res = copy_bidirectional(&mut client_stream, &mut backend_stream) => {
                            if let Err(e) = res {
                                if e.kind() != std::io::ErrorKind::ConnectionReset {}
                            }
                        }
                        _ = invalidation_trigger => {}
                    }
                }
                Err(e) => eprintln!("failed to connect to backend {}: {}", master_addr, e),
            }
        });
    }
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
            // match &res {
            //     Ok(Ok(addr)) => println!("Bootstrap response: Success -> {}", addr),
            //     Ok(Err(e)) => println!("Bootstrap response: Sentinel Error -> {}", e),
            //     Err(e) => println!("Bootstrap response: Task Panic/Cancel -> {}", e),
            // }

            if let Ok(Ok(addr)) = res {
                let count = master_votes.entry(addr).or_insert(0);
                *count += 1;

                if *count >= quorum_size {
                    return addr;
                }
            }
        }

        eprintln!("failed to reach quorum, retrying in 2 seconds...");
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
    mut event_rx: mpsc::Receiver<SentinelEvent>,
    state_tx: watch::Sender<ProxyState>,
) {
    let mut current_epoch: u64 = 0;
    let mut epoch_votes: HashSet<usize> = HashSet::new();
    let mut master_votes: HashMap<SocketAddr, HashSet<usize>> = HashMap::new();

    while let Some(event) = event_rx.recv().await {
        match event {
            SentinelEvent::NewEpoch { sentinel_id, epoch } => {
                if epoch > current_epoch {
                    println!("epoch {} starting, clearing previous votes.", epoch);
                    current_epoch = epoch;
                    epoch_votes.clear();
                    master_votes.clear();
                }

                if epoch == current_epoch {
                    epoch_votes.insert(sentinel_id);
                    if epoch_votes.len() >= quorum && *state_tx.borrow() != ProxyState::Paused {
                        println!(
                            "quorum reached for epoch {}, pausing proxy and closing active connections.",
                            epoch
                        );
                        let _ = state_tx.send(ProxyState::Paused);
                    }
                }
            }
            SentinelEvent::SwitchMaster {
                sentinel_id,
                new_addr,
            } => {
                let votes = master_votes.entry(new_addr).or_default();
                votes.insert(sentinel_id);

                if votes.len() >= quorum && *state_tx.borrow() != ProxyState::Active(new_addr) {
                    println!(
                        "quorum reached for master {}, resuming proxy traffic.",
                        new_addr
                    );
                    let _ = state_tx.send(ProxyState::Active(new_addr));
                    master_votes.clear();
                }
            }
        }
    }
}

async fn run_sentinel_subscriber(
    id: usize,
    url: String,
    master_name: String,
    tx: mpsc::Sender<SentinelEvent>,
) {
    loop {
        let config = match RedisConfig::from_url(&url) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("sentinel[{}] error creating config: {}", id, e);
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let client = match Builder::from_config(config).build_subscriber_client() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("sentinel[{}] error creating client: {}", id, e);
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        if let Err(e) = client.init().await {
            eprintln!("sentinel[{}] connection failed: {}", id, e);
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        if let Err(e) = client.subscribe(vec!["+new-epoch", "+switch-master"]).await {
            eprintln!("sentinel[{}] subscribe err: {}", id, e);
            let _ = client.quit().await;
            sleep(Duration::from_secs(2)).await;
            continue;
        }

        let mut message_stream = client.message_rx();

        while let Ok(msg) = message_stream.recv().await {
            let channel = msg.channel.to_string();

            if let Some(payload) = msg.value.as_string() {
                if channel == "+new-epoch" {
                    if let Ok(epoch) = payload.trim().parse::<u64>() {
                        let _ = tx
                            .send(SentinelEvent::NewEpoch {
                                sentinel_id: id,
                                epoch,
                            })
                            .await;
                    }
                } else if channel == "+switch-master" {
                    let parts: Vec<&str> = payload.split_whitespace().collect();
                    if parts.len() >= 5 && parts[0] == master_name {
                        let new_ip = parts[3];
                        let new_port = parts[4];
                        if let Ok(new_addr) =
                            format!("{}:{}", new_ip, new_port).parse::<SocketAddr>()
                        {
                            let _ = tx
                                .send(SentinelEvent::SwitchMaster {
                                    sentinel_id: id,
                                    new_addr,
                                })
                                .await;
                        }
                    }
                }
            }
        }

        eprintln!("sentinel[{}] disconnected, reconnecting...", id);
        let _ = client.quit().await;
        sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_coordinator_pauses_on_quorum_epoch() {
        let dummy_ip = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let initial_state = ProxyState::Active(dummy_ip);

        let (event_tx, event_rx) = mpsc::channel::<SentinelEvent>(100);
        let (state_tx, mut state_rx) = watch::channel(initial_state);

        tokio::spawn(async move {
            run_quorum_coordinator(2, event_rx, state_tx).await;
        });

        event_tx
            .send(SentinelEvent::NewEpoch {
                sentinel_id: 0,
                epoch: 5,
            })
            .await
            .unwrap();

        assert_eq!(*state_rx.borrow(), ProxyState::Active(dummy_ip));

        event_tx
            .send(SentinelEvent::NewEpoch {
                sentinel_id: 1,
                epoch: 5,
            })
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), state_rx.changed()).await;

        assert!(result.is_ok(), "Coordinator failed to update state in time");

        assert_eq!(*state_rx.borrow(), ProxyState::Paused);
    }

    #[tokio::test]
    async fn test_coordinator_pauses_on_non_quorum_epoch() {
        let dummy_ip = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let initial_state = ProxyState::Active(dummy_ip);

        let (event_tx, event_rx) = mpsc::channel::<SentinelEvent>(100);
        let (state_tx, state_rx) = watch::channel(initial_state);

        tokio::spawn(async move {
            run_quorum_coordinator(2, event_rx, state_tx).await;
        });

        event_tx
            .send(SentinelEvent::NewEpoch {
                sentinel_id: 0,
                epoch: 5,
            })
            .await
            .unwrap();

        assert_eq!(*state_rx.borrow(), ProxyState::Active(dummy_ip));

        event_tx
            .send(SentinelEvent::NewEpoch {
                sentinel_id: 1,
                epoch: 6,
            })
            .await
            .unwrap();

        assert!(
            !state_rx.has_changed().unwrap(),
            "Coordinator should not have updated the state"
        );

        assert_eq!(*state_rx.borrow(), ProxyState::Active(dummy_ip));
    }

    #[tokio::test]
    async fn test_coordinator_unpauses_on_quorum_switch_master() {
        let dummy_ip = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let initial_state = ProxyState::Paused;

        let (event_tx, event_rx) = mpsc::channel::<SentinelEvent>(100);
        let (state_tx, mut state_rx) = watch::channel(initial_state);

        tokio::spawn(async move {
            run_quorum_coordinator(2, event_rx, state_tx).await;
        });

        event_tx
            .send(SentinelEvent::SwitchMaster {
                sentinel_id: (0),
                new_addr: (dummy_ip),
            })
            .await
            .unwrap();

        assert_eq!(*state_rx.borrow(), ProxyState::Paused);

        event_tx
            .send(SentinelEvent::SwitchMaster {
                sentinel_id: (1),
                new_addr: (dummy_ip),
            })
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), state_rx.changed()).await;

        assert!(result.is_ok(), "Coordinator failed to update state in time");

        assert_eq!(*state_rx.borrow(), ProxyState::Active(dummy_ip));
    }

    #[tokio::test]
    async fn test_coordinator_unpauses_on_non_quorum_switch_master_ip() {
        let dummy_ip = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let dummy_ip2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 6379);
        let initial_state = ProxyState::Paused;

        let (event_tx, event_rx) = mpsc::channel::<SentinelEvent>(100);
        let (state_tx, state_rx) = watch::channel(initial_state);

        tokio::spawn(async move {
            run_quorum_coordinator(2, event_rx, state_tx).await;
        });

        event_tx
            .send(SentinelEvent::SwitchMaster {
                sentinel_id: (0),
                new_addr: (dummy_ip),
            })
            .await
            .unwrap();

        assert_eq!(*state_rx.borrow(), ProxyState::Paused);

        event_tx
            .send(SentinelEvent::SwitchMaster {
                sentinel_id: (1),
                new_addr: (dummy_ip2),
            })
            .await
            .unwrap();

        assert!(
            !state_rx.has_changed().unwrap(),
            "Coordinator should not have updated the state"
        );

        assert_eq!(*state_rx.borrow(), ProxyState::Paused);
    }
}
