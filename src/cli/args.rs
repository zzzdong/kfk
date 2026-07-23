use crate::config::{SaslConfig, SaslMechanism, SecurityProtocolType, TlsConfig};
use clap::{Args, Parser, Subcommand, ValueEnum};

/// Output format for consume command
#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum OutputFormat {
    Text,
    Pretty,
}

/// Offset value for consume command
#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum OffsetValue {
    Earliest,
    Latest,
}

impl std::fmt::Display for OffsetValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Earliest => write!(f, "earliest"),
            Self::Latest => write!(f, "latest"),
        }
    }
}

/// Input format for produce command
#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum InputFormat {
    Text,
    Json,
}

/// Kafka CLI - a command line tool for Kafka cluster management
#[derive(Parser, Debug)]
#[command(name = "kfk", author, version, about, long_about = None)]
pub struct Cli {
    /// Broker addresses (comma separated), overrides config
    #[arg(short, long)]
    pub brokers: Option<String>,

    /// Cluster name (temporary override)
    #[arg(short, long)]
    pub cluster: Option<String>,

    /// Verbosity level (repeatable: -v, -vv, -vvv, -vvvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Security protocol
    #[arg(long, value_enum, default_value_t = SecurityProtocolType::Plaintext)]
    pub security_protocol: SecurityProtocolType,

    #[command(flatten)]
    pub connection: Box<KafkaConnectionArgs>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Cluster configuration management
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },

    /// List all configured clusters (shorthand)
    Configs,

    /// Cluster node operations
    Node {
        #[command(subcommand)]
        action: NodeAction,
    },

    /// List all brokers (shorthand)
    Nodes,

    /// Topic operations
    Topic {
        #[command(subcommand)]
        action: TopicAction,
    },

    /// List all topics (shorthand)
    Topics,

    /// Produce messages to a topic (reads from stdin)
    Produce(ProduceArgs),

    /// Consume messages from a topic
    Consume(ConsumeArgs),

    /// Consumer group operations
    Group {
        #[command(subcommand)]
        action: GroupAction,
    },

    /// List all consumer groups (shorthand)
    Groups,

    /// Generate shell completion script
    Completion { shell: clap_complete::Shell },

    /// Generate shell completion script (hidden, for clap_complete integration)
    #[command(hide = true)]
    Completions { shell: clap_complete::Shell },
}

#[derive(Args, Clone, Debug)]
pub struct ProduceArgs {
    /// Topic to produce to
    pub topic: String,

    /// Message key
    #[arg(long)]
    pub key: Option<String>,

    /// Target partition
    #[arg(long)]
    pub partition: Option<i32>,

    /// Headers (key:value)
    #[arg(long = "header", value_parser = parse_key_val)]
    pub headers: Vec<(String, String)>,

    /// Input format
    #[arg(long, value_enum, default_value_t = InputFormat::Text)]
    pub input: InputFormat,
}

#[derive(Args, Clone, Debug)]
pub struct ConsumeArgs {
    /// Topic to consume from
    pub topic: String,

    /// Consumer group ID (use "none" for direct assign, "random" to auto-generate)
    #[arg(short, long, default_value = "none")]
    pub group: String,

    /// Output format
    #[arg(short, long, value_enum, default_value_t = OutputFormat::Pretty)]
    pub output: OutputFormat,

    /// Offset: earliest | latest
    #[arg(long, value_enum, default_value_t = OffsetValue::Latest)]
    pub offset: OffsetValue,

    /// Partition to consume from
    #[arg(long)]
    pub partition: Option<i32>,

    /// Filter by header (key:value)
    #[arg(long = "header", value_parser = parse_key_val)]
    pub headers: Vec<(String, String)>,

    /// Number of messages to consume then exit
    #[arg(long)]
    pub tail: Option<usize>,
}

/// Unified Kafka connection arguments shared by global CLI and `add-cluster`.
///
/// Used via `#[command(flatten)]` in both [`Cli`] and [`ConfigAction::AddCluster`].
#[derive(Args, Debug, Clone)]
pub struct KafkaConnectionArgs {
    /// SASL mechanism
    #[arg(long, value_enum)]
    pub sasl_mechanism: Option<SaslMechanism>,

    /// SASL username (or Kerberos principal for GSSAPI)
    #[arg(long)]
    pub sasl_username: Option<String>,

    /// SASL password (not required for GSSAPI)
    #[arg(long)]
    pub sasl_password: Option<String>,

    /// Keytab file path (for GSSAPI/Kerberos authentication)
    #[arg(long)]
    pub sasl_keytab: Option<String>,

    /// KDC host (for GSSAPI authentication)
    #[arg(long)]
    pub kdc_host: Option<String>,

    /// KDC port (for GSSAPI authentication, default: 88)
    #[arg(long, default_value_t = 88)]
    pub kdc_port: u16,

    /// Broker hostname for Kerberos service principal (for GSSAPI authentication)
    #[arg(long)]
    pub broker_hostname: Option<String>,

    /// Kerberos service name (for GSSAPI authentication, default: kafka)
    #[arg(long)]
    pub sasl_service_name: Option<String>,

    /// Kerberos realm (for GSSAPI authentication, extracted from principal if not set)
    #[arg(long)]
    pub sasl_realm: Option<String>,

    /// Enable TLS
    #[arg(long)]
    pub tls: bool,

    /// TLS CA file path
    #[arg(long)]
    pub tls_ca: Option<String>,

    /// TLS cert file path
    #[arg(long)]
    pub tls_cert: Option<String>,

    /// TLS key file path
    #[arg(long)]
    pub tls_key: Option<String>,

    /// Disable TLS certificate verification (insecure)
    #[arg(long)]
    pub tls_insecure: bool,
}

impl KafkaConnectionArgs {
    /// Build `SaslConfig` from CLI args.
    ///
    /// Returns `None` unless authentication can be determined: explicitly set
    /// `sasl_mechanism`, or both `sasl_username` and `sasl_password` provided
    /// (in which case `sasl_mechanism` defaults to `Plain`).
    pub fn build_sasl_config(&self) -> Option<SaslConfig> {
        let mechanism = match self.sasl_mechanism.clone() {
            Some(m) => m,
            None => {
                if self.sasl_username.is_some() && self.sasl_password.is_some() {
                    SaslMechanism::Plain
                } else {
                    return None;
                }
            }
        };
        let username = self.sasl_username.as_ref()?;
        let password = if mechanism == SaslMechanism::Gssapi {
            self.sasl_password.clone().unwrap_or_default()
        } else {
            self.sasl_password.clone()?
        };
        Some(SaslConfig {
            mechanism: mechanism.clone(),
            username: username.clone(),
            password,
            keytab: self.sasl_keytab.clone(),
            kdc_host: self.kdc_host.clone(),
            kdc_port: Some(self.kdc_port),
            broker_hostname: self.broker_hostname.clone(),
            service_name: self.sasl_service_name.clone(),
            realm: self.sasl_realm.clone(),
        })
    }

    /// Build `TlsConfig` from CLI args. Returns `None` if TLS is not enabled.
    pub fn build_tls_config(&self) -> Option<TlsConfig> {
        if !self.tls {
            return None;
        }
        Some(TlsConfig {
            insecure: self.tls_insecure,
            ca_file: self.tls_ca.clone(),
            cert_file: self.tls_cert.clone(),
            key_file: self.tls_key.clone(),
        })
    }
}

#[derive(Subcommand, Debug, Clone)]
pub enum ConfigAction {
    /// Add a new cluster configuration
    AddCluster {
        /// Cluster name
        name: String,

        /// Broker addresses (comma separated)
        #[arg(short, long)]
        brokers: String,

        /// Security protocol
        #[arg(long, value_enum, default_value_t = SecurityProtocolType::Plaintext)]
        security_protocol: SecurityProtocolType,

        #[command(flatten)]
        auth_args: Box<KafkaConnectionArgs>,
    },

    /// Remove a cluster configuration
    RemoveCluster { name: String },

    /// Select the active cluster
    SelectCluster { name: String },

    /// List all configured clusters
    List,
}

#[derive(Subcommand, Debug)]
pub enum NodeAction {
    /// List all brokers
    Ls,
}

#[derive(Subcommand, Debug)]
pub enum TopicAction {
    /// List all topics
    Ls,

    /// Describe a topic
    Describe { topic: String },

    /// Create a topic
    Create {
        topic: String,

        /// Number of partitions
        #[arg(short, long, default_value = "1")]
        partitions: i32,

        /// Replication factor
        #[arg(short, long, default_value_t = 1_i16)]
        replication_factor: i16,
    },

    /// Delete a topic
    Delete { topic: String },
}

#[derive(Subcommand, Debug)]
pub enum GroupAction {
    /// List all consumer groups
    Ls,

    /// Describe a consumer group
    Describe {
        /// Group ID
        group: String,
    },

    /// Show committed offsets and lag for consumer group(s)
    Offsets {
        /// Group ID (omit to show offsets for all groups)
        group: Option<String>,

        /// Only show offsets for this topic
        #[arg(short = 't', long)]
        topic: Option<String>,
    },

    /// Commit/reset offset for a consumer group
    Commit {
        /// Group ID
        group: String,

        /// Topic to commit
        #[arg(short = 't', long)]
        topic: String,

        /// Offset: earliest, latest, or a numeric offset
        #[arg(long, default_value = "latest")]
        offset: String,

        /// Partition number (required unless --all-partitions is set)
        #[arg(long)]
        partition: Option<i32>,

        /// Apply to all partitions
        #[arg(long)]
        all_partitions: bool,
    },

    /// Delete a consumer group
    Delete { group: String },
}

/// Parse key:value pairs
fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find(':')
        .ok_or_else(|| format!("Invalid KEY:VALUE format: {s}"))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}
