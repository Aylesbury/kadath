# About

Client Agent library for Aylesbury.

***WIP***

## Modes

The agent runs in one of two modes, selected via `AGENT_MODE`:

- **connector** (default) — serves a single connector. The database
  connection is injected locally (`DB_URL`) and the agent authenticates with
  a connector-scoped token. Credentials never leave your network.
- **admin** (alias: `platform`) — privileged platform runner. Serves every
  credentials-mode connector of its engine, across all tenants. No `DB_URL`
  is configured; each job arrives with a `connection` block resolved by the
  broker, and the agent authenticates with a platform-scoped token issued by
  the broker's provisioning API.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_MODE` | `connector` | `connector` or `admin` |
| `SERVER_ADDR` | `localhost:9001` | Broker gRPC endpoint |
| `SERVER_TLS` | `false` | Dial the broker with TLS |
| `AUTH_TOKEN` | — | Agent token (scope must match the mode) |
| `CONNECTOR_ID` | — | Connector ID (connector mode) |
| `DB_URL` | — | Database DSN (connector mode only) |
| `DB_TYPE` | inferred | Engine type when the DSN scheme is ambiguous |
| `DB_SSLMODE` | `disable` | SSL mode for the local database connection |

## Building

Engines are selected with build tags. Client deployments can build a slim
single-engine binary; platform runners build all engines into one binary and
dispatch per job based on the connection type:

```sh
make build-all        # every engine (platform runners)
make build-postgres   # or: make build-mysql
```

Currently implemented engines: `postgresql` (also serves `redshift`) and
`mysql`. Jobs for engines that are not compiled in fail with a clear error
instead of stalling.

## Docker images

Images are published per engine variant to GitHub Container Registry on
every push to `main` (and versioned on `v*` tags):

```
ghcr.io/aylesbury/kadath:all        # also tagged :latest
ghcr.io/aylesbury/kadath:postgres
ghcr.io/aylesbury/kadath:mysql
```

To build locally:

```sh
docker build -t kadath:all .                                  # all engines
docker build --build-arg ENGINE=postgres -t kadath:postgres . # single engine
```

Run it by passing configuration as environment variables:

```sh
docker run --rm \
  -e AGENT_MODE=connector \
  -e SERVER_ADDR=broker.example.com:9001 \
  -e AUTH_TOKEN=... \
  -e DB_URL=postgresql://user:pass@db.internal:5432/mydb \
  ghcr.io/aylesbury/kadath:postgres
```
