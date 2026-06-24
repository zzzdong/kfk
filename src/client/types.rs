use serde::Serialize;

/// CLI-friendly data types, mapped from kafka_client protocol types

#[derive(Debug, Clone, Serialize)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: usize,
    pub replication_factor: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub configs: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub is_controller: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupInfo {
    pub group_id: String,
    pub protocol: String,
    pub state: String,
    pub members: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupDetail {
    pub group_id: String,
    pub state: String,
    pub coordinator: Option<BrokerInfo>,
    pub members: Vec<GroupMember>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignment: Vec<TopicPartition>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partitions: Vec<i32>,
}
