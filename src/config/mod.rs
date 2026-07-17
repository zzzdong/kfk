mod loader;
mod model;

pub use loader::*;
pub use model::*;

use crate::cli::args::KafkaConnectionArgs;

/// Get a cluster config, either from config file or from ad-hoc --brokers flag
pub fn resolve_cluster(
    cluster_name: Option<&str>,
    brokers_override: Option<&str>,
    connection: &KafkaConnectionArgs,
    security_protocol: &SecurityProtocolType,
) -> Result<(String, ClusterConfig), String> {
    // If --brokers is provided, create an ad-hoc cluster
    if let Some(brokers_str) = brokers_override {
        let broker_list: Vec<String> = brokers_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let broker_list = if broker_list.is_empty() {
            vec!["127.0.0.1:9092".to_string()]
        } else {
            broker_list
        };

        let sasl_config = connection.build_sasl_config();
        let tls_config = connection.build_tls_config();
        let mut proto = security_protocol.clone();

        // Auto-upgrade security protocol based on provided credentials
        if sasl_config.is_some() && proto == SecurityProtocolType::Plaintext && !connection.tls {
            proto = SecurityProtocolType::SaslPlaintext;
        }
        if connection.tls && proto == SecurityProtocolType::Plaintext {
            proto = if sasl_config.is_some() {
                SecurityProtocolType::SaslSsl
            } else {
                SecurityProtocolType::Ssl
            };
        }

        return Ok((
            "adhoc".to_string(),
            ClusterConfig {
                brokers: broker_list,
                security_protocol: proto,
                sasl: sasl_config,
                tls: tls_config,
            },
        ));
    }

    // Otherwise, load from config file
    let name = cluster_name.unwrap_or("");
    get_cluster(name)
}
