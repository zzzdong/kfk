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
    admin.close().await?;
    println!("Topic: {}\n", detail.name);
    output::print_items(&detail.partitions, output::OutputFormat::Table);
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

impl TableRow for PartitionInfo {
    fn headers(&self) -> Vec<String> {
        vec![
            "ID".to_string(),
            "LEADER".to_string(),
            "REPLICAS".to_string(),
            "ISR".to_string(),
        ]
    }
    fn row(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            self.leader.to_string(),
            format!("{:?}", self.replicas),
            format!("{:?}", self.isr),
        ]
    }
}
