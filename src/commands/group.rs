use kafka_client::protocol::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};

use crate::cli::args::GroupAction;
use crate::cli::output::{self, TableRow};
use crate::client::{AdminClient, CliResult};

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

    let make_topic = |partitions: Vec<OffsetCommitRequestPartition>| OffsetCommitRequestTopic {
        name: topic.to_string(),
        topic_id: uuid::Uuid::nil(),
        partitions,
    };

    if all_partitions {
        let detail = admin.describe_topic(topic).await?;
        let partition_ids: Vec<i32> = detail.partitions.iter().map(|p| p.id).collect();

        let req = OffsetCommitRequest {
            group_id: group.to_string(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![make_topic(
                partition_ids
                    .iter()
                    .map(|&p| OffsetCommitRequestPartition {
                        partition_index: p,
                        committed_offset: offset_value,
                        committed_leader_epoch: -1,
                        committed_metadata: None,
                    })
                    .collect(),
            )],
        };

        admin
            .cluster
            .send_to_any_broker::<_, kafka_client::protocol::OffsetCommitResponse>(&req)
            .await
            .map_err(|e| format!("Failed to commit offset: {e}"))?;

        output::print_ok(format!(
            "Committed offset '{offset}' for group '{group}' on topic '{topic}' (all partitions)"
        ));
    } else if let Some(partition_id) = partition {
        let req = OffsetCommitRequest {
            group_id: group.to_string(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![make_topic(vec![OffsetCommitRequestPartition {
                partition_index: partition_id,
                committed_offset: offset_value,
                committed_leader_epoch: -1,
                committed_metadata: None,
            }])],
        };

        admin
            .cluster
            .send_to_any_broker::<_, kafka_client::protocol::OffsetCommitResponse>(&req)
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
    let req = kafka_client::protocol::DeleteGroupsRequest {
        groups_names: vec![group.to_string()],
    };

    admin
        .cluster
        .send_to_any_broker::<_, kafka_client::protocol::DeleteGroupsResponse>(&req)
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
