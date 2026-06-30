mod cli;
mod client;
mod commands;
mod config;

use clap::Parser;
use cli::args::{Cli, Commands, ConfigAction, GroupAction, NodeAction, TopicAction};
use client::{AdminClient, create_client};
use config::ClusterCliParams;
use config::resolve_cluster;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging based on verbosity level
    // -v: warn, -vv: info, -vvv: debug, -vvvv: trace
    {
        let level = match cli.verbose {
            0 => "", // no logging (error! still shows via stderr)
            1 => "warn",
            2 => "info",
            3 => "debug",
            _ => "trace",
        };
        if !level.is_empty() {
            let _ = tracing_subscriber::fmt().with_env_filter(level).try_init();
        }
    }

    // Dispatch commands, some don't need a cluster connection
    match &cli.command {
        Commands::Config { action } => {
            commands::config::handle_config(action.clone()).await;
            return;
        }
        Commands::Configs => {
            commands::config::handle_config(ConfigAction::List).await;
            return;
        }
        Commands::Completion { shell } | Commands::Completions { shell } => {
            generate_completion(*shell);
            return;
        }
        _ => {} // fall through to connect
    }

    // Build cluster connection for commands that need it
    let cli_params = ClusterCliParams {
        security_protocol: cli.security_protocol,
        mechanism: cli.sasl_mechanism,
        username: cli.sasl_username,
        password: cli.sasl_password,
        tls_enabled: cli.tls,
        tls_ca: cli.tls_ca,
        tls_cert: cli.tls_cert,
        tls_key: cli.tls_key,
        tls_insecure: cli.tls_insecure,
    };

    let (_, cluster_cfg) =
        resolve_cluster(cli.cluster.as_deref(), cli.brokers.as_deref(), &cli_params)
            .unwrap_or_else(|e| {
                eprintln!("ERROR: {e}");
                std::process::exit(1);
            });

    let client = create_client(&cluster_cfg).await.unwrap_or_else(|e| {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    });

    let admin = AdminClient::new(client);
    match cli.command {
        Commands::Config { .. }
        | Commands::Configs
        | Commands::Completion { .. }
        | Commands::Completions { .. } => {
            unreachable!()
        }
        Commands::Node { action } => commands::node::handle_node(action, admin).await,
        Commands::Nodes => commands::node::handle_node(NodeAction::Ls, admin).await,
        Commands::Topic { action } => commands::topic::handle_topic(action, admin).await,
        Commands::Topics => commands::topic::handle_topic(TopicAction::Ls, admin).await,
        Commands::Produce(args) => commands::produce::handle_produce(args, admin).await,
        Commands::Consume(args) => commands::consume::handle_consume(args, admin).await,
        Commands::Group { action } => commands::group::handle_group(action, admin).await,
        Commands::Groups => commands::group::handle_group(GroupAction::Ls, admin).await,
    }
}

fn generate_completion(shell: clap_complete::Shell) {
    use clap::CommandFactory;
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_string();
    clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
}
