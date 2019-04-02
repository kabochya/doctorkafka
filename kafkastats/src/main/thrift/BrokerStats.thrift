namespace java com.pinterest.doctorkafka.thrift

enum BrokerError {
    DiskFailure,
    JmxConnectionFailure,
    JmxQueryFailure,
    KafkaServerPropertiesFailure,
    MissingKafkaProcess,
    MultipleKafkaProcesses,
    UnknownError
}

struct TopicPartition{
    1: optional string topic;
    2: optional i32 partition;
}

struct ReplicaStats{
    1: required i64 timestamp;
    2: optional string topic;
    3: optional i32 partition;

    4: optional bool isLeader;
    5: optional bool inReassignment; // involved in partition reassignment
    6: optional bool underReplicated;

    7: optional bool bytesIn1MinMeanRate;
    8: optional bool bytesIn5MinMeanRate;
    9: optional i64 bytesIn15MinMeanRate;
    10: optional i64 bytesOut1MinMeanRate;
    11: optional i64 bytesOut5MinMeanRate;
    12: optional i64 bytesOut15MinMeanRate;
    13: optional i64 startOffset;
    14: optional i64 endOffset;
    15: optional double cpuUsage;
    16: optional i64 logSizeInBytes;
    17: optional i32  numLogSegments;
}


struct BrokerStats {
    1: required i64 timestamp = 0;
    2: required i32 id = -1;
    3: optional string name;
    4: optional string zkUrl;
    5: optional string kafkaVersion;
    6: optional string statsVersion;
    7: optional bool hasFailure;
    8: optional BrokerError failureReason;

    // instance info
    9: optional string availabilityZone;
    10: optional string instanceType;
    11: optional string amiId;
    12: optional string rackId;
    13: optional string logFilesPath;

    14: optional double  cpuUsage = 0.0;
    15: optional i64 freeDiskSpaceInBytes = -1;
    16: optional i64 totalDiskSpaceInBytes = -1;

    17: optional i64 leadersBytesIn1MinRate = 0;
    18: optional i64 leadersBytesOut1MinRate = 0;
    19: optional i64 leadersBytesIn5MinRate = 0;
    20: optional i64 leadersBytesOut5MinRate = 0;
    21: optional i64 leadersBytesIn15MinRate = 0;
    22: optional i64 leadersBytesOut15MinRate = 0;

    23: optional i64 sysBytesIn1MinRate = -1;
    24: optional i64 sysBytesOut1MinRate = -1;
    25: optional i32 numReplicas = 0;
    26: optional i32 numLeaders = 0;

    // the traffic metrics for leader replicas. here all the rate unit is bytes per second.
    27: optional map<string, i64> topicsBytesIn1MinRate;
    28: optional map<string, i64> topicsBytesOut1MinRate;

    29: optional map<string, i64> topicsBytesIn5MinRate;
    30: optional map<string, i64> topicsBytesOut5MinRate;

    31: optional map<string, i64> topicsBytesIn15MinRate;
    32: optional map<string, i64> topicsBytesOut15MinRate;

    33: optional list<TopicPartition> leaderReplicaStats;
    34: optional list<TopicPartition> leaderReplicas;
    35: optional list<TopicPartition> followerReplicas;
    36: optional list<TopicPartition> inReassignmentReplicas;
}

struct OperationAction {
    1: required i64 timestamp = 0;
    2: optional string clusterName;
    3: optional string description;
}