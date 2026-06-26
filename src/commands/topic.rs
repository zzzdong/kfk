use std::collections::HashMap;

use serde::Serialize;

use crate::cli::args::TopicAction;
use crate::cli::output::{self, TableRow};
use crate::client::{AdminClient, CliResult, PartitionInfo, TopicInfo};

pub async fn handle_topic(action: TopicAction, admin: AdminClient) {
    let result = match action {
        TopicAction::Ls => list_topics(admin).await,
        TopicAction::Describe { topic } => describe_topic(&topic, admin).await,
        TopicAction::Create {
            topic,
            partitions,
            replication_factor,
        } => create_topic(&topic, partitions, replication_factor, admin).await,
        TopicAction::Delete { topic } => delete_topic(&topic, admin).await,
    };

    match result {
        Ok(_) => {}
        Err(e) => output::print_err(e),
    }
}

async fn list_topics(admin: AdminClient) -> CliResult<()> {
    let topics = admin.list_topics().await?;
    admin.close().await?;
    output::print_items(&topics, output::OutputFormat::Table);
    Ok(())
}

async fn describe_topic(topic: &str, admin: AdminClient) -> CliResult<()> {
    let detail = admin.describe_topic(topic).await?;
    let brokers = admin.list_brokers().await?;
    admin.close().await?;

    // Build broker ID → "host:port" lookup
    let broker_map: HashMap<i32, String> = brokers
        .iter()
        .map(|b| (b.id, format!("{}:{}", b.host, b.port)))
        .collect();

    println!("Topic: {}\n", detail.name);
    let rows: Vec<PartitionRow> = detail
        .partitions
        .iter()
        .map(|p| PartitionRow {
            info: p.clone(),
            broker_map: broker_map.clone(),
        })
        .collect();
    output::print_items(&rows, output::OutputFormat::Table);
    Ok(())
}

async fn create_topic(
    topic: &str,
    partitions: i32,
    replication_factor: i16,
    admin: AdminClient,
) -> CliResult<()> {
    admin
        .create_topic(topic, partitions, replication_factor)
        .await?;
    admin.close().await?;
    output::print_ok(format!(
        "Topic '{topic}' created (partitions={partitions}, rf={replication_factor})"
    ));
    Ok(())
}

async fn delete_topic(topic: &str, admin: AdminClient) -> CliResult<()> {
    admin.delete_topic(topic).await?;
    admin.close().await?;
    output::print_ok(format!("Topic '{topic}' deleted"));
    Ok(())
}

impl TableRow for TopicInfo {
    fn headers(&self) -> Vec<String> {
        vec![
            "NAME".to_string(),
            "PARTITIONS".to_string(),
            "REPLICATION".to_string(),
        ]
    }
    fn row(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.partitions.to_string(),
            self.replication_factor.to_string(),
        ]
    }
}

#[derive(Serialize)]
struct PartitionRow {
    info: PartitionInfo,
    broker_map: HashMap<i32, String>,
}

impl TableRow for PartitionRow {
    fn headers(&self) -> Vec<String> {
        vec![
            "ID".to_string(),
            "LEADER".to_string(),
            "REPLICAS".to_string(),
            "ISR".to_string(),
        ]
    }
    fn row(&self) -> Vec<String> {
        let addr = |id: i32| {
            self.broker_map
                .get(&id)
                .map(|s| s.as_str())
                .unwrap_or("?")
                .to_string()
        };
        let replicas: Vec<String> = self
            .info
            .replicas
            .iter()
            .map(|b| {
                if *b == self.info.leader {
                    format!("{}({})*", b, addr(*b))
                } else {
                    format!("{}({})", b, addr(*b))
                }
            })
            .collect();
        vec![
            self.info.id.to_string(),
            format!("{}({})", self.info.leader, addr(self.info.leader)),
            replicas.join(", "),
            self.info
                .isr
                .iter()
                .map(|b| format!("{}({})", b, addr(*b)))
                .collect::<Vec<_>>()
                .join(", "),
        ]
    }
}
