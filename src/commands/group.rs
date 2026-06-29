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
