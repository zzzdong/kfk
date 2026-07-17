use crate::cli::args::{ConfigAction, KafkaConnectionArgs};
use crate::cli::output;
use crate::config::{ClusterConfig, SecurityProtocolType, TlsConfig, load_config, save_config};

pub async fn handle_config(action: ConfigAction) {
    match action {
        ConfigAction::AddCluster {
            name,
            brokers,
            security_protocol,
            auth_args,
        } => add_cluster(&name, &brokers, security_protocol, &auth_args),
        ConfigAction::RemoveCluster { name } => remove_cluster(&name),
        ConfigAction::SelectCluster { name } => select_cluster(&name),
        ConfigAction::List => list_clusters(),
    }
}

fn add_cluster(
    name: &str,
    brokers: &str,
    security_protocol: SecurityProtocolType,
    auth_args: &KafkaConnectionArgs,
) {
    let mut config = load_config();

    let sasl = auth_args.build_sasl_config();

    let tls_cfg = if auth_args.tls
        || security_protocol == SecurityProtocolType::Ssl
        || security_protocol == SecurityProtocolType::SaslSsl
    {
        // For saved config, always use secure TLS (insecure is a runtime-only concern)
        Some(TlsConfig {
            insecure: false,
            ca_file: auth_args.tls_ca.clone(),
            cert_file: auth_args.tls_cert.clone(),
            key_file: auth_args.tls_key.clone(),
        })
    } else {
        None
    };

    let broker_list: Vec<String> = brokers
        .split([',', ' '])
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .collect();

    if broker_list.is_empty() {
        output::print_err("At least one broker address is required");
        return;
    }

    config.clusters.insert(
        name.to_string(),
        ClusterConfig {
            brokers: broker_list,
            security_protocol,
            sasl,
            tls: tls_cfg,
        },
    );

    // Auto-select if first cluster
    if config.current_cluster.is_none() {
        config.current_cluster = Some(name.to_string());
    }

    match save_config(&config) {
        Ok(()) => output::print_ok(format!("Cluster '{name}' added")),
        Err(e) => output::print_err(e),
    }
}

fn remove_cluster(name: &str) {
    let mut config = load_config();
    if config.clusters.remove(name).is_some() {
        if config.current_cluster.as_deref() == Some(name) {
            config.current_cluster = config.clusters.keys().next().cloned();
        }
        match save_config(&config) {
            Ok(()) => output::print_ok(format!("Cluster '{name}' removed")),
            Err(e) => output::print_err(e),
        }
    } else {
        output::print_err(format!("Cluster '{name}' not found"));
    }
}

fn select_cluster(name: &str) {
    let mut config = load_config();
    if config.clusters.contains_key(name) {
        config.current_cluster = Some(name.to_string());
        match save_config(&config) {
            Ok(()) => output::print_ok(format!("Switched to cluster '{name}'")),
            Err(e) => output::print_err(e),
        }
    } else {
        output::print_err(format!("Cluster '{name}' not found"));
    }
}

fn list_clusters() {
    let config = load_config();
    if config.clusters.is_empty() {
        output::print_msg("No clusters configured.");
        return;
    }

    let current = config.current_cluster.as_deref().unwrap_or("");
    let mut clusters: Vec<(&String, &ClusterConfig)> = config.clusters.iter().collect();
    clusters.sort_by(|a, b| a.0.cmp(b.0));

    println!("CLUSTERS");
    println!("───────");
    for (name, cfg) in &clusters {
        let active = if *name == current { " ◄" } else { "" };
        let auth = if cfg.sasl.is_some() { " (SASL)" } else { "" };
        let tls = if cfg.tls.is_some() || cfg.security_protocol != SecurityProtocolType::Plaintext {
            " (TLS)"
        } else {
            ""
        };
        println!(
            "  {name} ─ {brokers}{auth}{tls}{active}",
            brokers = cfg.brokers.join(", "),
        );
    }
}
