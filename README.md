# kfk — Kafka CLI

**kfk** is a pure Rust command-line tool for Apache Kafka cluster management. It supports topic operations, message produce/consume, consumer group management, and secure connections (TLS, SASL).

[![Crates.io](https://img.shields.io/crates/v/kfk.svg)](https://crates.io/crates/kfk)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

---

## Features

- **Cluster management** — List brokers, describe cluster metadata
- **Topic operations** — List, describe, create, delete topics
- **Produce messages** — Send records from stdin (text or JSON)
- **Consume messages** — Tail messages with offset control and header filtering
- **Consumer groups** — List, describe, commit offsets, delete groups
- **Authentication** — SASL/PLAIN, SCRAM-SHA-256/512
- **Encryption** — TLS with custom CA, client certs, and insecure mode
- **Config profiles** — Save multiple cluster configs in `~/.kfk/config.toml`
- **Shell completions** — Generate completions for bash, zsh, fish, etc.

---

## Installation

### via Cargo (recommended)

```bash
cargo install kfk
```

### From source

```bash
git clone https://github.com/zzdong/kfk.git
cd kfk
cargo build --release
# binary at target/release/kfk
```

---

## Quick Start

### 1. Connect to a plaintext cluster

```bash
# List brokers
kfk --brokers localhost:9092 node ls

# List topics
kfk --brokers localhost:9092 topic ls

# Create a topic
kfk --brokers localhost:9092 topic create my-topic -p 3 -r 1

# Describe a topic
kfk --brokers localhost:9092 topic describe my-topic
```

### 2. Produce messages

```bash
# Text input (each line is one record)
echo "hello world" | kfk --brokers localhost:9092 produce my-topic

# JSON input
echo '{"key":"user1","message":"login event"}' | \
  kfk --brokers localhost:9092 produce my-topic --input json

# With explicit key
echo "message with key" | \
  kfk --brokers localhost:9092 produce my-topic --key my-key

# With headers
echo "important message" | \
  kfk --brokers localhost:9092 produce my-topic \
    --header event:critical --header env:prod
```

### 3. Consume messages

```bash
# Tail 10 messages from earliest offset
kfk --brokers localhost:9092 consume my-topic \
    --offset earliest --tail 10

# Continuous consumption with a consumer group
kfk --brokers localhost:9092 consume my-topic \
    --group my-consumer-group
```

---

## Authentication & Security

### SASL/PLAIN

```bash
kfk --brokers localhost:9094 \
    --sasl-username admin \
    --sasl-password admin-secret \
    topic ls
```

The security protocol is automatically upgraded to `SASL_PLAINTEXT` when SASL credentials are provided.

### SASL/SCRAM

```bash
kfk --brokers localhost:9094 \
    --sasl-mechanism SCRAM-SHA-256 \
    --sasl-username user \
    --sasl-password pass \
    consume my-topic
```

### TLS

```bash
# TLS with default system CA
kfk --brokers broker:9093 --tls node ls

# TLS with custom CA
kfk --brokers broker:9093 \
    --tls \
    --tls-ca /etc/ssl/certs/ca.pem \
    node ls

# TLS with mTLS (client certificate)
kfk --brokers broker:9093 \
    --tls \
    --tls-cert client.pem \
    --tls-key client.key \
    node ls

# Insecure TLS (skip certificate verification)
kfk --brokers broker:9093 --tls --tls-insecure node ls
```

### SASL + TLS

```bash
kfk --brokers broker:9094 \
    --tls \
    --sasl-username admin \
    --sasl-password admin-secret \
    topic ls
```

---

## Configuration Profiles

Save frequently used cluster settings to avoid repeating flags:

```bash
# Add a cluster config
kfk config add-cluster prod \
    --brokers broker1:9092,broker2:9092 \
    --security-protocol SASL_PLAINTEXT \
    --sasl-mechanism PLAIN \
    --sasl-username admin \
    --sasl-password admin-secret

# Switch to a config
kfk config select prod

# List all configs
kfk config list

# Then use without --brokers
kfk topic ls
```

Configs are stored in `~/.kfk/config.toml`.

---

## Command Reference

| Command | Description |
|---------|-------------|
| `node ls` | List all brokers |
| `topic ls` | List all topics |
| `topic create <name>` | Create a topic |
| `topic describe <name>` | Describe topic partitions |
| `topic delete <name>` | Delete a topic |
| `produce <topic>` | Produce messages (reads stdin) |
| `consume <topic>` | Consume messages |
| `group ls` | List consumer groups |
| `group describe <id>` | Describe a consumer group |
| `group commit <id>` | Commit/reset offset |
| `group delete <id>` | Delete a consumer group |
| `config add-cluster <name>` | Save a cluster config |
| `config remove-cluster <name>` | Remove a config |
| `config select <name>` | Switch active config |
| `config list` | List all configs |
| `completion <shell>` | Generate shell completion |

### Global Options

| Flag | Description |
|------|-------------|
| `-b, --brokers` | Broker addresses (comma separated) |
| `-c, --cluster` | Cluster config name |
| `-v, --verbose` | Verbose output |
| `--security-protocol` | `PLAINTEXT` / `SSL` / `SASL_PLAINTEXT` / `SASL_SSL` |
| `--sasl-mechanism` | `PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512` |
| `--sasl-username` | SASL username |
| `--sasl-password` | SASL password |
| `--tls` | Enable TLS |
| `--tls-ca` | TLS CA certificate file |
| `--tls-cert` | TLS client certificate file |
| `--tls-key` | TLS client key file |
| `--tls-insecure` | Skip certificate verification |

---

## Shell Completions

```bash
# Bash
kfk completion bash > /etc/bash_completion.d/kfk

# Zsh
kfk completion zsh > /usr/share/zsh/site-functions/_kfk

# Fish
kfk completion fish > ~/.config/fish/completions/kfk.fish
```

---

## Building from Source

```bash
git clone https://github.com/zzdong/kfk.git
cd kfk
cargo build --release
```

Minimum supported Rust version: **1.85**

---

## License

This project is dual-licensed under either:

- **MIT License** ([LICENSE-MIT](LICENSE-MIT))
- **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE))

at your option.
