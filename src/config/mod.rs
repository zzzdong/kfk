mod loader;
mod model;

pub use loader::*;
pub use model::*;

/// Get a cluster config, either from config file or from ad-hoc --brokers flag
pub fn resolve_cluster(
    cluster_name: Option<&str>,
    brokers_override: Option<&str>,
) -> Result<(String, ClusterConfig), String> {
    // If --brokers is provided, create an ad-hoc cluster
    if let Some(brokers_str) = brokers_override {
        let broker_list: Vec<String> = brokers_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if broker_list.is_empty() {
            return Err("No brokers specified".to_string());
        }
        return Ok((
            "adhoc".to_string(),
            ClusterConfig {
                brokers: broker_list,
                security_protocol: SecurityProtocolType::Plaintext,
                sasl: None,
                tls: None,
            },
        ));
    }

    // Otherwise, load from config file
    let name = cluster_name.unwrap_or("");
    get_cluster(name)
}
