use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub current_cluster: Option<String>,
    pub clusters: HashMap<String, ClusterConfig>,
}

impl Default for AppConfig {
    fn default() -> Self {
        let mut clusters = HashMap::new();
        clusters.insert(
            "local".to_string(),
            ClusterConfig {
                brokers: vec!["127.0.0.1:9092".to_string()],
                security_protocol: SecurityProtocolType::Plaintext,
                sasl: None,
                tls: None,
            },
        );
        Self {
            current_cluster: Some("local".to_string()),
            clusters,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterConfig {
    pub brokers: Vec<String>,
    #[serde(default)]
    pub security_protocol: SecurityProtocolType,
    pub sasl: Option<SaslConfig>,
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, clap::ValueEnum)]
pub enum SecurityProtocolType {
    #[default]
    #[serde(rename = "PLAINTEXT")]
    Plaintext,
    #[serde(rename = "SSL")]
    Ssl,
    #[serde(rename = "SASL_PLAINTEXT")]
    SaslPlaintext,
    #[serde(rename = "SASL_SSL")]
    SaslSsl,
}

impl std::fmt::Display for SecurityProtocolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plaintext => write!(f, "PLAINTEXT"),
            Self::Ssl => write!(f, "SSL"),
            Self::SaslPlaintext => write!(f, "SASL_PLAINTEXT"),
            Self::SaslSsl => write!(f, "SASL_SSL"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SaslConfig {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
    /// Keytab file path (for GSSAPI/Kerberos authentication)
    #[serde(default)]
    pub keytab: Option<String>,
    /// KDC host (for GSSAPI authentication)
    #[serde(default)]
    pub kdc_host: Option<String>,
    /// KDC port (for GSSAPI authentication, default: 88)
    #[serde(default)]
    pub kdc_port: Option<u16>,
    /// Broker hostname for Kerberos service principal (for GSSAPI authentication)
    #[serde(default)]
    pub broker_hostname: Option<String>,
    /// Kerberos service name (for GSSAPI authentication, default: kafka)
    #[serde(default)]
    pub service_name: Option<String>,
    /// Kerberos realm (for GSSAPI authentication, extracted from principal if not set)
    #[serde(default)]
    pub realm: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, clap::ValueEnum)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    /// SASL/GSSAPI (Kerberos) authentication
    Gssapi,
}

impl std::fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain => write!(f, "PLAIN"),
            Self::ScramSha256 => write!(f, "SCRAM-SHA-256"),
            Self::ScramSha512 => write!(f, "SCRAM-SHA-512"),
            Self::Gssapi => write!(f, "GSSAPI"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub insecure: bool,
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}
