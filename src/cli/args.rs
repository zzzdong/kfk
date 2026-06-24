use clap::{Args, Parser, Subcommand};

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

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

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
    Completion {
        shell: clap_complete::Shell,
    },

    /// Generate shell completion script (hidden, for clap_complete integration)
    #[command(hide = true)]
    Completions {
        shell: clap_complete::Shell,
    },
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

    /// Input format: text | json-each-row
    #[arg(long, default_value = "text")]
    pub input: String,
}

#[derive(Args, Clone, Debug)]
pub struct ConsumeArgs {
    /// Topic to consume from
    pub topic: String,

    /// Consumer group ID
    #[arg(short, long, default_value = "kfk-cli")]
    pub group: String,

    /// Output format: text | json-each-row
    #[arg(short, long, default_value = "text")]
    pub output: String,

    /// Offset: earliest | latest | <N>
    #[arg(long, default_value = "latest")]
    pub offset: String,

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

        /// Security protocol: PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
        #[arg(long, default_value = "PLAINTEXT")]
        security_protocol: String,

        /// SASL mechanism: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
        #[arg(long)]
        sasl_mechanism: Option<String>,

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
    RemoveCluster {
        name: String,
    },

    /// Select the active cluster
    SelectCluster {
        name: String,
    },

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
    /// List all topics (default)
    Ls,

    /// Describe a topic
    Describe {
        topic: String,
    },

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
    Delete {
        topic: String,
    },
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

        /// Offset: earliest | latest | <N>
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
    Delete {
        group: String,
    },
}

/// Parse key:value pairs
fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s.find(':').ok_or_else(|| format!("Invalid KEY:VALUE format: {s}"))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}
