use super::model::AppConfig;
use std::path::PathBuf;

const CONFIG_DIR: &str = ".kfk";
const CONFIG_FILE: &str = "config.toml";

/// Get config file path (~/.kfk/config.toml)
pub fn config_path() -> PathBuf {
    let home = dirs::home_dir().expect("Cannot find home directory");
    home.join(CONFIG_DIR).join(CONFIG_FILE)
}

/// Load config from file, returns default if not found
pub fn load_config() -> AppConfig {
    let path = config_path();
    if path.exists() {
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        toml::from_str(&content).unwrap_or_default()
    } else {
        AppConfig::default()
    }
}

/// Save config to file
pub fn save_config(config: &AppConfig) -> Result<(), String> {
    let path = config_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("Failed to create config dir: {e}"))?;
    }
    let content =
        toml::to_string_pretty(config).map_err(|e| format!("Failed to serialize config: {e}"))?;
    std::fs::write(&path, content).map_err(|e| format!("Failed to write config: {e}"))?;
    Ok(())
}

/// Get active cluster config by name
pub fn get_cluster(name: &str) -> Result<(String, super::model::ClusterConfig), String> {
    let config = load_config();
    let cluster_name = if name.is_empty() {
        config.current_cluster.as_deref().unwrap_or("local")
    } else {
        name
    };
    let cluster = config
        .clusters
        .get(cluster_name)
        .ok_or_else(|| format!("Cluster '{cluster_name}' not found"))?;
    Ok((cluster_name.to_string(), cluster.clone()))
}
