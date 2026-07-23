use crate::cli::args::GroupAction;
use crate::cli::output::{self, TableRow};
use crate::client::{AdminClient, CliResult};
use kafka_client::admin::OffsetCommitSpec;

pub async fn handle_group(action: GroupAction, admin: AdminClient) {
    let result = match action {
        GroupAction::Ls => list_groups(admin).await,
        GroupAction::Describe { group } => describe_group(&group, admin).await,
        GroupAction::Commit {
            group,
            topic,
            offset,
            partition,
            all_partitions,
        } => commit_offset(&group, &topic, &offset, partition, all_partitions, admin).await,
        GroupAction::Offsets { group, topic } => show_offsets(group, topic, admin).await,
        GroupAction::Delete { group } => delete_group(&group, admin).await,
    };

    match result {
        Ok(_) => {}
        Err(e) => output::print_err(e),
    }
}

async fn list_groups(admin: AdminClient) -> CliResult<()> {
    let groups = admin.list_groups().await?;
    admin.close().await?;
    output::print_items(&groups, output::OutputFormat::Table);
    Ok(())
}

async fn describe_group(group: &str, admin: AdminClient) -> CliResult<()> {
    let detail = admin.describe_group(group).await?;
    admin.close().await?;
    println!("Group: {}\n", detail.group_id);
    if !detail.members.is_empty() {
        output::print_items(&detail.members, output::OutputFormat::Table);
    } else {
        output::print_msg("  No active members.");
    }
    Ok(())
}

async fn commit_offset(
    group: &str,
    topic: &str,
    offset: &str,
    partition: Option<i32>,
    all_partitions: bool,
    admin: AdminClient,
) -> CliResult<()> {
    let offset_value: i64 = match offset.to_lowercase().as_str() {
        "earliest" => -2,
        "latest" => -1,
        n => n.parse().map_err(|_| format!("Invalid offset: {n}"))?,
    };

    if all_partitions {
        let detail = admin.describe_topic(topic).await?;
        let specs: Vec<OffsetCommitSpec> = detail
            .partitions
            .iter()
            .map(|p| OffsetCommitSpec {
                topic: topic.to_string(),
                partition: p.id,
                offset: offset_value,
                metadata: None,
            })
            .collect();

        admin
            .client()
            .admin()
            .commit_offsets(group, &specs)
            .await
            .map_err(|e| format!("Failed to commit offset: {e}"))?;

        output::print_ok(format!(
            "Committed offset '{offset}' for group '{group}' on topic '{topic}' (all partitions)"
        ));
    } else if let Some(partition_id) = partition {
        let specs = [OffsetCommitSpec {
            topic: topic.to_string(),
            partition: partition_id,
            offset: offset_value,
            metadata: None,
        }];

        admin
            .client()
            .admin()
            .commit_offsets(group, &specs)
            .await
            .map_err(|e| format!("Failed to commit offset: {e}"))?;

        output::print_ok(format!(
            "Committed offset '{offset}' for group '{group}' on topic '{topic}' partition {partition_id}"
        ));
    } else {
        return Err("Must specify --partition or --all-partitions".to_string());
    }

    admin.close().await?;
    Ok(())
}

async fn delete_group(group: &str, admin: AdminClient) -> CliResult<()> {
    admin
        .client()
        .admin()
        .delete_group(group)
        .await
        .map_err(|e| format!("Failed to delete group: {e}"))?;

    output::print_ok(format!("Group '{group}' deleted"));
    admin.close().await?;
    Ok(())
}

async fn show_offsets(
    group: Option<String>,
    topic: Option<String>,
    admin: AdminClient,
) -> CliResult<()> {
    let mut offsets = match group {
        Some(ref g) => admin.fetch_group_offsets(g).await?,
        None => {
            let groups = admin.list_groups().await?;
            let mut collected = Vec::new();
            for g in groups {
                match admin.fetch_group_offsets(&g.group_id).await {
                    Ok(mut offs) => collected.append(&mut offs),
                    Err(e) => eprintln!("Warning: skipping group '{}': {e}", g.group_id),
                }
            }
            collected
        }
    };
    admin.close().await?;

    if let Some(t) = &topic {
        offsets.retain(|o| &o.topic == t);
    }

    if offsets.is_empty() {
        let group_desc = group
            .map(|g| format!(" for group '{g}'"))
            .unwrap_or_else(|| " for any group".to_string());
        let scope = topic
            .map(|t| format!(" on topic '{t}'"))
            .unwrap_or_default();
        output::print_msg(format!("No committed offsets found{group_desc}{scope}"));
        return Ok(());
    }

    output::print_items(&offsets, output::OutputFormat::Table);
    Ok(())
}

impl TableRow for crate::client::GroupInfo {
    fn headers(&self) -> Vec<String> {
        vec![
            "GROUP ID".to_string(),
            "PROTOCOL".to_string(),
            "STATE".to_string(),
        ]
    }
    fn row(&self) -> Vec<String> {
        vec![
            self.group_id.clone(),
            self.protocol.clone(),
            self.state.clone(),
        ]
    }
}

impl TableRow for crate::client::GroupMember {
    fn headers(&self) -> Vec<String> {
        vec![
            "MEMBER ID".to_string(),
            "CLIENT ID".to_string(),
            "CLIENT HOST".to_string(),
        ]
    }
    fn row(&self) -> Vec<String> {
        vec![
            self.member_id.clone(),
            self.client_id.clone(),
            self.client_host.clone(),
        ]
    }
}

impl TableRow for crate::client::GroupOffsetInfo {
    fn headers(&self) -> Vec<String> {
        vec![
            "GROUP".to_string(),
            "TOPIC".to_string(),
            "PARTITION".to_string(),
            "COMMITTED".to_string(),
            "LOG-END".to_string(),
            "LAG".to_string(),
            "METADATA".to_string(),
        ]
    }
    fn row(&self) -> Vec<String> {
        let log_end = if self.log_end_offset < 0 {
            "-".to_string()
        } else {
            self.log_end_offset.to_string()
        };
        let lag = if self.lag < 0 {
            "-".to_string()
        } else {
            self.lag.to_string()
        };
        vec![
            self.group.clone(),
            self.topic.clone(),
            self.partition.to_string(),
            self.committed_offset.to_string(),
            log_end,
            lag,
            self.metadata.clone(),
        ]
    }
}
