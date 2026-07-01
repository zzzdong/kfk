use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest};
use kafka_client::protocol::{
    CreateTopicsRequest, DescribeGroupsRequest, DescribeGroupsResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, ListGroupsRequest, ListGroupsResponse,
};
use kafka_client::{Client, Producer, ProducerConfig};
use std::time::Duration;

use super::CliResult;
use super::types::*;

/// AdminClient wraps the unified `Client` for CLI usage.
///
/// Provides convenience methods for cluster administration that go beyond
/// the built-in `admin::AdminClient` (e.g. offset commit, find-coordinator flow).
pub struct AdminClient {
    client: Client,
}

impl AdminClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Get the underlying Client (for low-level `send_to_any_broker` etc.)
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Create a producer from this client
    pub async fn create_producer(&self) -> Producer {
        let config = ProducerConfig::new()
            .with_acks(1)
            .with_timeout(5000)
            .with_retries(3)
            .with_batch_size(16384)
            .with_linger(50);
        self.client.producer(config).await
    }

    /// Refresh cluster metadata
    pub async fn refresh_metadata(&self) -> CliResult<()> {
        self.client
            .refresh_metadata()
            .await
            .map_err(|e| format!("Failed to refresh metadata: {e}"))
    }

    /// Send a request to any available broker (low-level)
    pub async fn send_to_any<Req, Resp>(&self, request: &Req) -> CliResult<Resp>
    where
        Req: kafka_client::protocol::Request,
        Resp: kafka_client::protocol::Response,
    {
        self.client
            .send_to_any_broker(request)
            .await
            .map_err(|e| format!("Request failed: {e}"))
    }

    /// List all topics
    pub async fn list_topics(&self) -> CliResult<Vec<TopicInfo>> {
        self.refresh_metadata().await?;
        let topics = self.client.metadata().get_all_topics().await;
        let mut result = Vec::new();
        for t in &topics {
            if let Some(name) = &t.name {
                let rf = t
                    .partitions
                    .first()
                    .map(|p| p.replica_nodes.len() as i32)
                    .unwrap_or(0);
                result.push(TopicInfo {
                    name: name.clone(),
                    partitions: t.partitions.len(),
                    replication_factor: rf,
                });
            }
        }
        result.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(result)
    }

    /// Describe a specific topic
    pub async fn describe_topic(&self, topic_name: &str) -> CliResult<TopicDetail> {
        self.refresh_metadata().await?;
        let tm = self
            .client
            .metadata()
            .get_topic(topic_name)
            .await
            .ok_or_else(|| format!("Topic '{topic_name}' not found"))?;

        let partitions = tm
            .partitions
            .iter()
            .map(|p| PartitionInfo {
                id: p.partition_index,
                leader: p.leader_id,
                replicas: p.replica_nodes.clone(),
                isr: p.isr_nodes.clone(),
            })
            .collect();

        Ok(TopicDetail {
            name: topic_name.to_string(),
            partitions,
            configs: vec![],
        })
    }

    /// Create a topic
    pub async fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        replication_factor: i16,
    ) -> CliResult<()> {
        let req = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions,
                replication_factor,
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 10000,
            validate_only: false,
        };

        let resp: kafka_client::protocol::CreateTopicsResponse = self.send_to_any(&req).await?;

        for t in &resp.topics {
            if t.error_code != 0 && t.error_code != 36 {
                return Err(format!(
                    "Failed to create topic '{}': error code {}",
                    t.name, t.error_code
                ));
            }
        }
        Ok(())
    }

    /// Delete a topic
    pub async fn delete_topic(&self, name: &str) -> CliResult<()> {
        let req = DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(name.to_string()),
                topic_id: uuid::Uuid::nil(),
            }],
            topic_names: vec![name.to_string()],
            timeout_ms: 10000,
        };

        let resp: kafka_client::protocol::DeleteTopicsResponse = self.send_to_any(&req).await?;

        for t in &resp.responses {
            if t.error_code != 0 {
                return Err(format!(
                    "Failed to delete topic '{}': error code {}",
                    t.name.as_deref().unwrap_or("unknown"),
                    t.error_code
                ));
            }
        }
        Ok(())
    }

    /// List all brokers
    pub async fn list_brokers(&self) -> CliResult<Vec<BrokerInfo>> {
        self.refresh_metadata().await?;
        let brokers = self.client.metadata().get_all_brokers().await;
        let mut result: Vec<BrokerInfo> = brokers
            .into_iter()
            .map(|b| BrokerInfo {
                id: b.node_id,
                host: b.host,
                port: b.port,
                rack: None,
                is_controller: false,
            })
            .collect();

        // Refresh metadata to get controller info
        self.refresh_metadata().await?;
        if let Some(controller) = result.first().cloned()
            && let Some(b) = result.iter_mut().find(|b| b.id == controller.id) {
                b.is_controller = true;
            }

        result.sort_by_key(|a| a.id);
        Ok(result)
    }

    /// List all consumer groups
    pub async fn list_groups(&self) -> CliResult<Vec<GroupInfo>> {
        let req = ListGroupsRequest {
            states_filter: vec![],
            types_filter: vec![],
        };

        let resp: ListGroupsResponse = self.send_to_any(&req).await?;

        let mut groups: Vec<GroupInfo> = resp
            .groups
            .into_iter()
            .map(|g| GroupInfo {
                group_id: g.group_id,
                protocol: g.protocol_type,
                state: g.group_state,
                members: 0,
            })
            .collect();
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        Ok(groups)
    }

    /// Describe a consumer group
    pub async fn describe_group(&self, group_id: &str) -> CliResult<GroupDetail> {
        let mut retries = 0u32;
        let max_retries = 30u32;
        let coord_resp: FindCoordinatorResponse = loop {
            retries += 1;
            if retries > max_retries {
                return Err("Group coordinator not available after retries".to_string());
            }
            let req = FindCoordinatorRequest {
                key: group_id.to_string(),
                key_type: 0,
                coordinator_keys: vec![group_id.to_string()],
            };
            let resp: FindCoordinatorResponse = self.send_to_any(&req).await?;

            // error_code 15 = GROUP_COORDINATOR_NOT_AVAILABLE (retryable)
            if resp.error_code == 15 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            if let Some(coord) = resp.coordinators.first()
                && coord.error_code == 15 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            break resp;
        };

        // For v4+ responses, host/port are in coordinators array
        let (node_id, host, port) = if !coord_resp.host.is_empty() {
            (coord_resp.node_id, coord_resp.host.clone(), coord_resp.port)
        } else if let Some(coord) = coord_resp.coordinators.first() {
            (coord.node_id, coord.host.clone(), coord.port)
        } else {
            return Err("Failed to find group coordinator".to_string());
        };

        let coordinator = Some(BrokerInfo {
            id: node_id,
            host,
            port,
            rack: None,
            is_controller: false,
        });

        let req = DescribeGroupsRequest {
            groups: vec![group_id.to_string()],
            include_authorized_operations: false,
        };

        let resp: DescribeGroupsResponse = self.send_to_any(&req).await?;

        let group = resp
            .groups
            .into_iter()
            .next()
            .ok_or_else(|| format!("Group '{group_id}' not found"))?;

        let members = group
            .members
            .into_iter()
            .map(|m| GroupMember {
                member_id: m.member_id,
                client_id: m.client_id,
                client_host: m.client_host,
                assignment: Vec::new(),
            })
            .collect();

        Ok(GroupDetail {
            group_id: group_id.to_string(),
            state: group.group_state,
            coordinator,
            members,
        })
    }

    /// Close the client
    pub async fn close(self) -> CliResult<()> {
        self.client
            .close()
            .await
            .map_err(|e| format!("Failed to close: {e}"))
    }
}
