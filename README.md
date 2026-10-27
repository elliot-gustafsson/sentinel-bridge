# Sentinel Bridge

Sentinel Bridge is an intelligent, quorum-aware TCP proxy designed to sit between client applications and a Valkey/Redis Sentinel cluster.

It abstracts Sentinel failover logic away from the client. Clients connect to the proxy via a standard TCP connection, and the proxy autonomously discovers the current master database, routes traffic to it, and gracefully manages connections during cluster failovers.

## Architecture and Behavior
1. **Quorum Bootstrapping**: On startup, the proxy queries all configured Sentinels simultaneously. It will not begin accepting client traffic until a strict majority (quorum) of Sentinels agree on the current master address.
2. **Event Monitoring**: The proxy establishes persistent `Pub/Sub` connections to all Sentinels. It actively listens for `+switch-master` and `+new-epoch` events, and runs a background reconciliation loop to detect any missed events.
3. **Data Plane**: Client connections are streamed directly to the current master for high-throughput, low-latency communication.
4. **Failover Invalidation**: When a quorum of Sentinels report a `+switch-master` event (indicating a new master has been elected), the Quorum Coordinator updates the active backend route. The proxy immediately severs all active client connections routed to the old master, forcing clients to reconnect.
5. **Traffic Resumption**: As soon as clients reconnect, their new TCP streams are automatically routed to the newly promoted master.

## Configuration
The application can be configured via environment variables or CLI flags.

| Variable | CLI Flag | Description | Default | Example |
| :--- | :--- | :--- | :--- | :--- |
| `LOG_LEVEL` | `--log-level` | Maximum log level (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`). | `INFO` | `DEBUG` |
| `PROXY_BIND_ADDR` | `--proxy-bind-addr` | The `IP:PORT` the proxy listens on for client TCP traffic. | `0.0.0.0:6379` | `0.0.0.0:6379` |
| `HTTP_BIND_ADDR` | `--http-bind-addr` | The `IP:PORT` for the HTTP server (Readiness probes and Metrics). | `0.0.0.0:8080` | `0.0.0.0:8080` |
| `MASTER_NAME` | `--master-name` | The name of the Valkey/Redis master group monitored by Sentinel. | *(Required)* | `mymaster` |
| `SENTINEL_ADDRS` | `--sentinel-addrs` | A comma-separated list of Sentinel connection URLs. | *(Required)* | `redis://10.0.0.1:26379,redis://10.0.0.2:26379` |
| `BACKEND_DIAL_TIMEOUT` | `--backend-dial-timeout` | Timeout for establishing TCP connections to the master backend. | `3s` | `5s` |
| `TERMINATION_GRACE_PERIOD` | `--termination-grace-period` | Delay after receiving a termination signal before stopping new connections. | `5s` | `10s` |
| `CONNECTION_DRAIN_TIMEOUT` | `--connection-drain-timeout` | Maximum time to wait for active connections to drain before forcefully closing them. | `30s` | `60s` |
| `RECONCILE_INTERVAL` | `--reconcile-interval` | Interval for background polling of Sentinels to detect master drift. | `10s` | `15s` |

## Observability

### Logging
The application uses the `tracing` ecosystem. Logs are output as structured JSON to `stdout`.
Log levels can be configured using the `LOG_LEVEL` environment variable or `--log-level` flag (default: `INFO`).

### Metrics and Readiness
The proxy exposes an HTTP server on `HTTP_BIND_ADDR` with three endpoints:
* `/ready`: Returns HTTP 200 `OK` only when the proxy has successfully bootstrapped quorum and is actively routing traffic. Returns HTTP 503 otherwise.
* `/health`: A basic liveness probe.
* `/metrics`: Exposes Prometheus-formatted metrics.

**Available Prometheus Metrics:**
* `proxy_connections_total` (Counter): Total number of connections opened to the backend, labeled by `backend`.
* `proxy_active_connections` (Gauge): Current number of active connections to the backend, labeled by `backend`.
* `proxy_connections_closed_total` (Counter): Total number of connections closed, labeled by `backend` and `reason` (`client_disconnect`, `server_disconnect`, `failover_severed`, `graceful_shutdown`).
* `proxy_backend_connection_errors_total` (Counter): Total number of failed connection attempts to backend, labeled by `backend`.

## Graceful Shutdown
The application intercepts `SIGINT`, `SIGTERM`, and `SIGQUIT` signals.
To support zero-downtime deployments in Kubernetes environments, the shutdown sequence executes as follows:
1. Receives termination signal and marks `/ready` as unhealthy (`503`).
2. Sleeps for `TERMINATION_GRACE_PERIOD` (default `5s`) to allow upstream LoadBalancers/kube-proxy to remove the pod IP.
3. Stops accepting new incoming TCP connections.
4. Waits up to `CONNECTION_DRAIN_TIMEOUT` (default `30s`) for all currently active streams to finish data transfer and close cleanly, after which any remaining connections are closed.

## Building and Running
The project requires Rust to build. Dependencies are managed via Cargo.
```bash
# Build the optimized release binary
make build

# Run locally
make localrun

# A multistage `Containerfile` is provided to build the application into a minimal, `distroless` Debian image.
make container TAG=myregistry/sentinel-bridge:latest
