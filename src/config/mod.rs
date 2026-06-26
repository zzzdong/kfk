mod loader;
mod model;

pub use loader::*;
pub use model::*;

/// Auth and TLS params passed via CLI (ad-hoc cluster)
#[derive(Debug, Clone, Default)]
pub struct ClusterCliParams {
    pub security_protocol: SecurityProtocolType,
    pub mechanism: Option<SaslMechanism>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls_enabled: bool,
    pub tls_ca: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_insecure: bool,
}

/// Get a cluster config, either from config file or from ad-hoc --brokers flag
pub fn resolve_cluster(
    cluster_name: Option<&str>,
    brokers_override: Option<&str>,
    cli_params: &ClusterCliParams,
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
        let sasl_config = build_sasl_config(cli_params);
        let tls_config = build_tls_config(cli_params);
        let mut proto = cli_params.security_protocol.clone();
        // Auto-upgrade: SASL creds + plain protocol → SASL_PLAINTEXT
        if sasl_config.is_some()
            && proto == SecurityProtocolType::Plaintext
            && !cli_params.tls_enabled
        {
            proto = SecurityProtocolType::SaslPlaintext;
        }
        // Auto-upgrade: tls enabled + plain protocol → SSL
        if cli_params.tls_enabled && proto == SecurityProtocolType::Plaintext {
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

fn build_sasl_config(params: &ClusterCliParams) -> Option<SaslConfig> {
    let username = params.username.as_ref()?;
    let password = params.password.as_ref()?;
    let mechanism = params.mechanism.clone().unwrap_or(SaslMechanism::Plain);
    Some(SaslConfig {
        mechanism,
        username: username.clone(),
        password: password.clone(),
    })
}

fn build_tls_config(params: &ClusterCliParams) -> Option<TlsConfig> {
    if !params.tls_enabled {
        return None;
    }
    Some(TlsConfig {
        insecure: params.tls_insecure,
        ca_file: params.tls_ca.clone(),
        cert_file: params.tls_cert.clone(),
        key_file: params.tls_key.clone(),
    })
}
