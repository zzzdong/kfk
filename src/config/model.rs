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

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
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
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl std::fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain => write!(f, "PLAIN"),
            Self::ScramSha256 => write!(f, "SCRAM-SHA-256"),
            Self::ScramSha512 => write!(f, "SCRAM-SHA-512"),
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
