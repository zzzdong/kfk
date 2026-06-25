mod cli;
mod client;
mod commands;
mod config;

use clap::Parser;
use cli::args::{Cli, Commands};
use client::{AdminClient, create_client};
use config::ClusterCliParams;
use config::resolve_cluster;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging if verbose
    if cli.verbose {
        let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    }

    // Handle config command separately (doesn't need a cluster connection)
    if let Commands::Config { action } = &cli.command {
        commands::config::handle_config(action.clone()).await;
        return;
    }

    // Handle completion commands separately (doesn't need a cluster connection)
    if let Commands::Completion { shell } = &cli.command {
        generate_completion(*shell);
        return;
    }
    if let Commands::Completions { shell } = &cli.command {
        generate_completion(*shell);
        return;
    }

    // For all other commands, create a cluster connection
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
    let cluster_result =
        resolve_cluster(cli.cluster.as_deref(), cli.brokers.as_deref(), &cli_params);

    match cluster_result {
        Ok((_, cluster_cfg)) => {
            let client_result = create_client(&cluster_cfg).await;
            match client_result {
                Ok(kafka_client) => {
                    let admin = AdminClient::new(kafka_client);
                    execute_command(cli.command, admin).await;
                }
                Err(e) => {
                    eprintln!("ERROR: {e}");
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("ERROR: {e}");
            std::process::exit(1);
        }
    }
}

async fn execute_command(command: Commands, admin: AdminClient) {
    match command {
        Commands::Config { .. } | Commands::Completion { .. } | Commands::Completions { .. } => {
            unreachable!("handled above")
        }
        Commands::Node { action } => {
            commands::node::handle_node(action, admin).await;
        }
        Commands::Topic { action } => {
            commands::topic::handle_topic(action, admin).await;
        }
        Commands::Produce(args) => {
            commands::produce::handle_produce(args, admin).await;
        }
        Commands::Consume(args) => {
            commands::consume::handle_consume(args, admin).await;
        }
        Commands::Group { action } => {
            commands::group::handle_group(action, admin).await;
        }
    }
}

fn generate_completion(shell: clap_complete::Shell) {
    use clap::CommandFactory;
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_string();
    clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
}
