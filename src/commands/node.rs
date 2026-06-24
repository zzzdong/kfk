use crate::cli::args::NodeAction;
use crate::cli::output::{self, TableRow};
use crate::client::{AdminClient, BrokerInfo, CliResult};

pub async fn handle_node(action: NodeAction, admin: AdminClient) {
    match action {
        NodeAction::Ls => list_nodes(admin).await,
    }
}

async fn list_nodes(admin: AdminClient) {
    let result = list_nodes_impl(admin).await;
    match result {
        Ok(brokers) => {
            output::print_items(&brokers, output::OutputFormat::Table);
        }
        Err(e) => output::print_err(e),
    }
}

async fn list_nodes_impl(admin: AdminClient) -> CliResult<Vec<BrokerInfo>> {
    let brokers = admin.list_brokers().await?;
    admin.close().await?;
    Ok(brokers)
}

impl TableRow for BrokerInfo {
    fn headers(&self) -> Vec<String> {
        vec![
            "ID".to_string(),
            "HOST".to_string(),
            "PORT".to_string(),
            "CONTROLLER".to_string(),
        ]
    }
    fn row(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            self.host.clone(),
            self.port.to_string(),
            if self.is_controller { "true" } else { "" }.to_string(),
        ]
    }
}
