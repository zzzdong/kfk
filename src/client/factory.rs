use std::net::ToSocketAddrs;
use std::time::Duration;

use kafka_client::{AutoOffsetReset, ConsumerConfig, KafkaClient, PartitionRouting, ProducerConfig};

use crate::config::{ClusterConfig, SecurityProtocolType};

use super::CliResult;

/// Create KafkaClient from cluster config
pub async fn create_client(config: &ClusterConfig) -> CliResult<KafkaClient> {
    let addrs: Vec<std::net::SocketAddr> = config
        .brokers
        .iter()
        .map(|b| {
            b.to_socket_addrs()
                .unwrap_or_else(|_| panic!("Invalid broker address: {b}"))
                .next()
                .expect("No address resolved")
        })
        .collect();

    let mut builder = KafkaClient::builder(addrs)
        .with_client_id("kfk-cli")
        .with_metadata_ttl(Duration::from_secs(30));

    builder = match config.security_protocol {
        SecurityProtocolType::Plaintext => builder.with_plaintext(),
        SecurityProtocolType::Ssl => {
            if let Some(tls) = &config.tls {
                let domain = tls.cert_file.as_deref().unwrap_or("localhost").to_string();
                builder.with_tls(domain)
            } else {
                builder.with_tls("localhost")
            }
        }
        SecurityProtocolType::SaslPlaintext | SecurityProtocolType::SaslSsl => {
            if let Some(sasl) = &config.sasl {
                let mechanism = match sasl.mechanism {
                    crate::config::SaslMechanism::Plain => {
                        kafka_client::SaslMechanismType::Plain
                    }
                    crate::config::SaslMechanism::ScramSha256 => {
                        kafka_client::SaslMechanismType::ScramSha256
                    }
                    crate::config::SaslMechanism::ScramSha512 => {
                        kafka_client::SaslMechanismType::ScramSha512
                    }
                };
                builder.with_sasl(mechanism, &sasl.username, &sasl.password)
            } else {
                builder
            }
        }
    };

    builder.build().await.map_err(|e| format!("Failed to connect: {e}"))
}

/// Create a consumer with given group_id and offset strategy
pub async fn create_consumer(
    client: &KafkaClient,
    group_id: &str,
    offset: AutoOffsetReset,
) -> CliResult<kafka_client::Consumer> {
    let config = ConsumerConfig {
        group_id: group_id.to_string(),
        auto_commit: true,
        auto_commit_interval_ms: 5000,
        auto_offset_reset: offset,
        min_bytes: 1,
        max_bytes: 10 * 1024 * 1024,
        partition_max_bytes: 1024 * 1024,
        max_wait_ms: 1000,
        session_timeout_ms: 45000,
        rebalance_timeout_ms: 60000,
        heartbeat_interval_ms: 3000,
        partition_assignment_strategy: kafka_client::PartitionAssignmentStrategy::Range,
    };
    Ok(client.consumer(config))
}

/// Create a producer
pub async fn create_producer(client: &KafkaClient) -> CliResult<kafka_client::Producer> {
    let config = ProducerConfig {
        acks: 1,
        timeout_ms: 5000,
        routing: PartitionRouting::default(),
        retries: 3,
        batch_size: 16384,
        linger_ms: 50,
    };
    client.producer(config).await.map_err(|e| format!("Failed to create producer: {e}"))
}
