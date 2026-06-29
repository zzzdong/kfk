use crate::config::{SaslMechanism, SecurityProtocolType};
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

    /// SASL mechanism
    #[arg(long, value_enum)]
    pub sasl_mechanism: Option<SaslMechanism>,

    /// SASL username
    #[arg(long)]
    pub sasl_username: Option<String>,

    /// SASL password
    #[arg(long)]
    pub sasl_password: Option<String>,

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

    /// Cluster node operations
    Node {
        #[command(subcommand)]
        action: NodeAction,
    },

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

    /// Consumer group ID (use "random" to auto-generate)
    #[arg(short, long, default_value = "random")]
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

        /// SASL mechanism
        #[arg(long, value_enum)]
        sasl_mechanism: Option<SaslMechanism>,

        /// SASL username
        #[arg(long)]
        sasl_username: Option<String>,

        /// SASL password
        #[arg(long)]
        sasl_password: Option<String>,

        /// Enable TLS
        #[arg(long)]
        tls: bool,

        /// TLS CA file path
        #[arg(long)]
        tls_ca: Option<String>,

        /// TLS cert file path
        #[arg(long)]
        tls_cert: Option<String>,

        /// TLS key file path
        #[arg(long)]
        tls_key: Option<String>,
    },

    /// Remove a cluster configuration
    RemoveCluster { name: String },

    /// Select the active cluster
    SelectCluster { name: String },

    /// List all configured clusters (default)
    List,
}

#[derive(Subcommand, Debug)]
pub enum NodeAction {
    /// List all brokers (default)
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
    /// List all consumer groups (default)
    Ls,

    /// Describe a consumer group
    Describe {
        /// Group ID
        group: String,
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
