mod loader;
mod model;

pub use loader::*;
pub use model::*;

/// Auth and TLS params passed via CLI (ad-hoc cluster)
#[derive(Debug, Clone, Default)]
pub struct ClusterCliParams {
    pub security_protocol: String,
    pub mechanism: Option<String>,
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
        if broker_list.is_empty() {
            return Err("No brokers specified".to_string());
        }
        let sasl_config = build_sasl_config(cli_params);
        let tls_config = build_tls_config(cli_params);
        let mut proto = parse_security_protocol(&cli_params.security_protocol);
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

fn parse_security_protocol(s: &str) -> SecurityProtocolType {
    match s.to_uppercase().as_str() {
        "SSL" => SecurityProtocolType::Ssl,
        "SASL_PLAINTEXT" => SecurityProtocolType::SaslPlaintext,
        "SASL_SSL" => SecurityProtocolType::SaslSsl,
        _ => SecurityProtocolType::Plaintext,
    }
}

fn build_sasl_config(params: &ClusterCliParams) -> Option<SaslConfig> {
    let username = params.username.as_ref()?;
    let password = params.password.as_ref()?;
    let mechanism = match params.mechanism.as_deref() {
        Some("SCRAM-SHA-256") => SaslMechanism::ScramSha256,
        Some("SCRAM-SHA-512") => SaslMechanism::ScramSha512,
        _ => SaslMechanism::Plain,
    };
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
