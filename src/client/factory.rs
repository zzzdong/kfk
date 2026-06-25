use std::net::ToSocketAddrs;
use std::time::Duration;

use kafka_client::SaslMechanismType;
use kafka_client::{
    AutoOffsetReset, ConsumerConfig, KafkaClient, PartitionRouting, ProducerConfig, TlsConfig,
};

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

    let builder = KafkaClient::builder(addrs)
        .with_client_id("kfk-cli")
        .with_metadata_ttl(Duration::from_secs(30));

    let builder = apply_security(builder, config)?;

    builder
        .build()
        .await
        .map_err(|e| format!("Failed to connect: {e}"))
}

fn apply_security(
    builder: kafka_client::KafkaClientBuilder,
    config: &ClusterConfig,
) -> CliResult<kafka_client::KafkaClientBuilder> {
    Ok(match config.security_protocol {
        SecurityProtocolType::Plaintext => builder.with_plaintext(),
        SecurityProtocolType::Ssl => {
            let tls_cfg = build_kafka_tls_config(&config.tls);
            builder.with_tls_config(tls_cfg)
        }
        SecurityProtocolType::SaslPlaintext => {
            let (mechanism, username, password) = require_sasl_creds(&config.sasl)?;
            builder.with_sasl(mechanism, username, password)
        }
        SecurityProtocolType::SaslSsl => {
            let tls_cfg = build_kafka_tls_config(&config.tls);
            let (mechanism, username, password) = require_sasl_creds(&config.sasl)?;
            builder.with_sasl_tls(tls_cfg, mechanism, username, password)
        }
    })
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
        auto_commit_interval: Duration::from_millis(5000),
        auto_offset_reset: offset,
        min_bytes: 1,
        max_bytes: 10 * 1024 * 1024,
        partition_max_bytes: 1024 * 1024,
        max_wait: Duration::from_millis(1000),
        session_timeout: Duration::from_millis(45000),
        rebalance_timeout: Duration::from_millis(60000),
        heartbeat_interval: Duration::from_millis(3000),
        partition_assignment_strategy: kafka_client::PartitionAssignmentStrategy::Range,
    };
    Ok(client.consumer(config))
}

/// Create a producer
#[allow(dead_code)]
pub async fn create_producer(client: &KafkaClient) -> CliResult<kafka_client::Producer> {
    let config = ProducerConfig {
        acks: 1,
        timeout_ms: 5000,
        routing: PartitionRouting::default(),
        retries: 3,
        batch_size: 16384,
        linger_ms: 50,
    };
    client
        .producer(config)
        .await
        .map_err(|e| format!("Failed to create producer: {e}"))
}

/// Map our config TLS to kafka_client TlsConfig
fn build_kafka_tls_config(tls: &Option<crate::config::TlsConfig>) -> TlsConfig {
    match tls {
        Some(cfg) => TlsConfig {
            verify_certificate: !cfg.insecure,
            domain: cfg.cert_file.as_deref().unwrap_or("localhost").to_string(),
            ca_cert_path: cfg.ca_file.clone(),
            client_cert_path: cfg.cert_file.clone(),
            client_key_path: cfg.key_file.clone(),
        },
        None => TlsConfig {
            domain: "localhost".to_string(),
            ..Default::default()
        },
    }
}

/// Extract SASL credentials or return an error
fn require_sasl_creds(
    sasl: &Option<crate::config::SaslConfig>,
) -> Result<(SaslMechanismType, &str, &str), String> {
    let sasl = sasl.as_ref().ok_or_else(|| {
        "SASL credentials required but not provided (--sasl-username / --sasl-password)".to_string()
    })?;
    let mechanism = match sasl.mechanism {
        crate::config::SaslMechanism::Plain => SaslMechanismType::Plain,
        crate::config::SaslMechanism::ScramSha256 => SaslMechanismType::ScramSha256,
        crate::config::SaslMechanism::ScramSha512 => SaslMechanismType::ScramSha512,
    };
    Ok((mechanism, &sasl.username, &sasl.password))
}
