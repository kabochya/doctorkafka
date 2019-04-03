package com.pinterest.doctorkafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pinterest.doctorkafka.thrift.ReplicaStats;
import com.pinterest.doctorkafka.replicastats.ReplicaStatsManager;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ReplicaStatsManagerTest {
  private static final String ZKURL = "zk001/cluster1";
  private static final String TOPIC = "nginx_log";
  private List<ReplicaStats> replicaStats = new ArrayList<ReplicaStats>();

  private void initialize() {
    ReplicaStats stats1 = new ReplicaStats()
        .setTimestamp(1502951705179L)
        .setTopic(TOPIC)
        .setPartition(21)
        .setIsLeader(true)
        .setInReassignment(true)
        .setUnderReplicated(true)
        .setBytesIn1MinMeanRate(9481888L)
        .setBytesIn5MinMeanRate(9686567L)
        .setBytesIn15MinMeanRate(9838856L)
        .setBytesOut1MinMeanRate(34313663L)
        .setBytesOut5MinMeanRate(31282690L)
        .setBytesOut15MinMeanRate(22680562L)
        .setStartOffset(154167421996L)
        .setEndOffset(154253138423L)
        .setCpuUsage(8.937965169074621)
        .setLogSizeInBytes(72999272025L)
        .setNumLogSegments(68);

    ReplicaStats stats2 = new ReplicaStats()
        .setTimestamp(1502951705179L)
        .setTopic(TOPIC)
        .setPartition(51)
        .setIsLeader(true)
        .setInReassignment(true)
        .setUnderReplicated(false)
        .setBytesIn1MinMeanRate(9539926L)
        .setBytesIn5MinMeanRate(9745859L)
        .setBytesIn15MinMeanRate(9899080L)
        .setBytesOut1MinMeanRate(34523697L)
        .setBytesOut5MinMeanRate(31474171L)
        .setBytesOut15MinMeanRate(22819389L)
        .setStartOffset(154167145733L)
        .setEndOffset(154253367479L)
        .setCpuUsage(8.992674338022038)
        .setLogSizeInBytes(73446100348L)
        .setNumLogSegments(69);

    replicaStats.add(stats1);
    replicaStats.add(stats2);
  }

  @Test
  public void updateReplicaReassignmentTimestampTest() throws Exception {
    initialize();
    ReplicaStatsManager.updateReplicaReassignmentTimestamp(ZKURL, replicaStats.get(0));
    ReplicaStatsManager.updateReplicaReassignmentTimestamp(ZKURL, replicaStats.get(1));

    TopicPartition topicPartition = new TopicPartition(TOPIC, 21);

    assertEquals(ReplicaStatsManager.replicaReassignmentTimestamps.get(ZKURL).size(), 2);
    assertEquals(
        (long)ReplicaStatsManager.replicaReassignmentTimestamps.get(ZKURL).get(topicPartition),
        1502951705179L);
  }
}
