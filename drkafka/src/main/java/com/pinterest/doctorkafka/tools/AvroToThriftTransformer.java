package com.pinterest.doctorkafka.tools;

import com.pinterest.doctorkafka.thrift.BrokerError;
import com.pinterest.doctorkafka.thrift.BrokerStats;
import com.pinterest.doctorkafka.thrift.ReplicaStats;
import com.pinterest.doctorkafka.thrift.ThriftTopicPartition;
import com.pinterest.doctorkafka.util.OperatorUtil;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class AvroToThriftTransformer {

  private static final Logger LOG = LogManager.getLogger(AvroToThriftTransformer.class);
  private static final String SRC_ZOOKEEPER = "src-zk";
  private static final String DST_ZOOKEEPER = "dst-zk";
  private static final String AVRO_TOPIC = "avro-topic";
  private static final String THRIFT_TOPIC = "thrift-topic";
  private static final String BACKTRACK_SEC = "backtrack-time";
  private static final String GROUP_ID = "avro_to_thrift_transformer";
  private static final SecurityProtocol SECURITY_PROTOCOL = SecurityProtocol.PLAINTEXT;
  private static final Options options = new Options();
  static volatile boolean keepRunning = true;
  private static List<Thread> threads = new ArrayList<>();
  private static List<Transformer> transformers = new ArrayList<>();

  private static CommandLine parseCommandLine(String[] args) {
    Option srcZk = new Option(null, SRC_ZOOKEEPER, true, "source zookeeper connection string");
    Option dstZk = new Option(null, DST_ZOOKEEPER, true, "destination zookeeper connection string");
    Option avroTopic = new Option(null, AVRO_TOPIC, true, "topic that has avro brokerStats");
    Option thriftTopic = new Option(null, THRIFT_TOPIC, true, "topic that has thrift brokerStats");
    options
        .addOption(srcZk)
        .addOption(dstZk)
        .addOption(avroTopic)
        .addOption(thriftTopic);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException | NumberFormatException e) {
      printUsageAndExit();
    }

    if (
        !cmd.hasOption(SRC_ZOOKEEPER) || !cmd.hasOption(DST_ZOOKEEPER)
            || !cmd.hasOption(AVRO_TOPIC) || !cmd.hasOption(THRIFT_TOPIC)
    )
    {
      printUsageAndExit();
    }
    return cmd;
  }

  private static void printUsageAndExit() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("AvroToThriftTransformer", options);
    System.exit(1);
  }

  public static void main(String[] args){
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        shutdown();
      }
    });

    CommandLine commandLine = parseCommandLine(args);
    String srcZkUrl = commandLine.getOptionValue(SRC_ZOOKEEPER);
    String dstZkUrl = commandLine.getOptionValue(DST_ZOOKEEPER);
    String avroTopic = commandLine.getOptionValue(AVRO_TOPIC);
    String thriftTopic = commandLine.getOptionValue(THRIFT_TOPIC);

    Map<String, String> consumerConfigs = new HashMap<>();
    consumerConfigs.put("auto.offset.reset", "earliest");
    consumerConfigs.put("enable.auto.commit", "false");
    Properties srcProps = OperatorUtil.createKafkaConsumerProperties(
        srcZkUrl,
        GROUP_ID,
        SECURITY_PROTOCOL,
        consumerConfigs
    );
    Properties dstProps = OperatorUtil.createKafkaProducerProperties(dstZkUrl, SECURITY_PROTOCOL);

    KafkaConsumer<byte[],byte[]> tmpConsumer = new KafkaConsumer<>(srcProps);
    int numSrcPartitions = tmpConsumer.partitionsFor(avroTopic).size();
    tmpConsumer.close();

    for ( int i = 0; i < numSrcPartitions; i++){
      Transformer transformer = new Transformer(avroTopic, thriftTopic, srcProps, dstProps);
      Thread t = new Thread(transformer);
      t.start();
      threads.add(t);
    }
  }

  protected static void shutdown(){
    LOG.info("Terminating transformers");
    keepRunning = false;
    for(Transformer transformers : transformers){
      transformers.terminate();
    }
    for(Thread thread : threads){
      try{
        thread.join();
      }
      catch (Exception e){
        LOG.error("Failed to terminate thread: ", e);
      }
    }
  }

  private static class Transformer implements Runnable {
    private static final Logger LOG = LogManager.getLogger(Transformer.class);
    private String avroTopic;
    private String thriftTopic;
    private DecoderFactory avroDecoderFactory = new DecoderFactory();
    private SpecificDatumReader<com.pinterest.doctorkafka.avro.BrokerStats> avroReader =
        new SpecificDatumReader<>(com.pinterest.doctorkafka.avro.BrokerStats.getClassSchema());
    private TSerializer thriftSerializer = new TSerializer();
    private KafkaConsumer<byte[], byte[]> consumer;
    private KafkaProducer<byte[], byte[]> producer;

    Transformer(String avroTopic, String thriftTopic, Properties srcProps, Properties dstProps){
      this.avroTopic = avroTopic;
      this.thriftTopic = thriftTopic;
      consumer = new KafkaConsumer<>(srcProps);
      producer = new KafkaProducer<>(dstProps);
    }

    @Override
    public void run() {
      consumer.subscribe(Arrays.asList(avroTopic));
      while (AvroToThriftTransformer.keepRunning) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
        if(records.isEmpty()){
          continue;
        }
        int numPartitions = producer.partitionsFor(thriftTopic).size();
        for (ConsumerRecord<byte[], byte[]> record : records) {
          com.pinterest.doctorkafka.avro.BrokerStats result =
              new com.pinterest.doctorkafka.avro.BrokerStats();
          try {
            BinaryDecoder binaryDecoder = avroDecoderFactory.binaryDecoder(record.value(), null);
            avroReader.read(result, binaryDecoder);
          } catch (Exception e) {
            LOG.error("Failed to decode an message", e);
            continue;
          }
          BrokerStats thriftBrokerStats = transform(result);
          try {
            byte[] transformedMessage = thriftSerializer.serialize(thriftBrokerStats);
            producer.send(
                new ProducerRecord<>(
                    thriftTopic,
                    result.getId() % numPartitions,
                    record.timestamp(),
                    record.key(),
                    transformedMessage)
            );
          }
          catch (Exception e){
            LOG.error("Failed to encode an message: ", e);
          }
        }
        producer.flush();
        consumer.commitSync();
      }
    }

    public void terminate(){
      producer.close();
      consumer.close();
    }

    protected BrokerStats transform(com.pinterest.doctorkafka.avro.BrokerStats abs){
      if ( abs == null ){
        return null;
      }
      BrokerStats thriftBrokerStats = new BrokerStats();

      thriftBrokerStats
          .setTimestamp(abs.getTimestamp())
          .setId(abs.getId())
          .setName(abs.getName())
          .setZkUrl(abs.getZkUrl())
          .setKafkaVersion(abs.getKafkaVersion())
          .setStatsVersion(abs.getStatsVersion())
          .setHasFailure(abs.getHasFailure())
          .setFailureReason(transform(abs.getFailureReason()))
          .setAvailabilityZone(abs.getAvailabilityZone())
          .setInstanceType(abs.getInstanceType())
          .setAmiId(abs.getAmiId())
          .setRackId(abs.getRackId())
          .setLogFilesPath(abs.getLogFilesPath())
          .setCpuUsage(abs.getCpuUsage())
          .setFreeDiskSpaceInBytes(abs.getFreeDiskSpaceInBytes())
          .setTotalDiskSpaceInBytes(abs.getTotalDiskSpaceInBytes())
          .setLeadersBytesIn1MinRate(abs.getLeadersBytesIn1MinRate())
          .setLeadersBytesIn5MinRate(abs.getLeadersBytesIn5MinRate())
          .setLeadersBytesIn15MinRate(abs.getLeadersBytesIn15MinRate())
          .setLeadersBytesOut1MinRate(abs.getLeadersBytesOut1MinRate())
          .setLeadersBytesOut5MinRate(abs.getLeadersBytesOut5MinRate())
          .setLeadersBytesOut15MinRate(abs.getLeadersBytesOut15MinRate())
          .setSysBytesIn1MinRate(abs.getSysBytesIn1MinRate())
          .setSysBytesOut1MinRate(abs.getSysBytesOut1MinRate())
          .setNumReplicas(abs.getNumReplicas())
          .setNumLeaders(abs.getNumLeaders())
          .setTopicsBytesIn1MinRate(abs.getTopicsBytesIn1MinRate())
          .setTopicsBytesOut1MinRate(abs.getTopicsBytesOut1MinRate())
          .setTopicsBytesIn5MinRate(abs.getTopicsBytesIn5MinRate())
          .setTopicsBytesOut5MinRate(abs.getTopicsBytesOut5MinRate())
          .setTopicsBytesIn15MinRate(abs.getTopicsBytesIn15MinRate())
          .setTopicsBytesOut15MinRate(abs.getTopicsBytesOut15MinRate())
          .setLeaderReplicaStats(transformReplicaStatsList(abs.getLeaderReplicaStats()))
          .setLeaderReplicas(transformAvroTopicPartitionList(abs.getLeaderReplicas()))
          .setFollowerReplicas(transformAvroTopicPartitionList(abs.getFollowerReplicas()))
          .setInReassignmentReplicas(transformAvroTopicPartitionList(abs.getInReassignmentReplicas()));

      return thriftBrokerStats;
    }

    protected ReplicaStats transform(com.pinterest.doctorkafka.avro.ReplicaStat ars){
      ReplicaStats thriftReplicaStats = new ReplicaStats();
      thriftReplicaStats
          .setTimestamp(ars.getTimestamp())
          .setTopic(ars.getTopic())
          .setPartition(ars.getPartition())
          .setIsLeader(ars.getIsLeader())
          .setInReassignment(ars.getInReassignment())
          .setUnderReplicated(ars.getUnderReplicated())
          .setBytesIn1MinMeanRate(ars.getBytesIn1MinMeanRate())
          .setBytesIn5MinMeanRate(ars.getBytesIn5MinMeanRate())
          .setBytesIn15MinMeanRate(ars.getBytesIn15MinMeanRate())
          .setBytesOut1MinMeanRate(ars.getBytesOut1MinMeanRate())
          .setBytesOut5MinMeanRate(ars.getBytesOut5MinMeanRate())
          .setBytesOut15MinMeanRate(ars.getBytesOut15MinMeanRate())
          .setStartOffset(ars.getStartOffset())
          .setEndOffset(ars.getEndOffset())
          .setCpuUsage(ars.getCpuUsage())
          .setLogSizeInBytes(ars.getLogSizeInBytes())
          .setNumLogSegments(ars.getNumLogSegments());

      return thriftReplicaStats;
    }

    protected List<ReplicaStats> transformReplicaStatsList(List<com.pinterest.doctorkafka.avro.ReplicaStat> lars){
      if ( lars == null ){
        return null;
      }
      return lars.stream().map(ars -> transform(ars)).collect(Collectors.toList());
    }

    protected ThriftTopicPartition transform(
        com.pinterest.doctorkafka.avro.AvroTopicPartition atp){
      if ( atp == null ){
        return null;
      }
      ThriftTopicPartition thriftTopicPartition = new ThriftTopicPartition();
      thriftTopicPartition
          .setTopic(atp.getTopic())
          .setPartition(atp.getPartition());

      return thriftTopicPartition;
    }

    protected List<ThriftTopicPartition> transformAvroTopicPartitionList(
        List<com.pinterest.doctorkafka.avro.AvroTopicPartition> latp){
      if ( latp == null ){
        return null;
      }
      return latp.stream().map(atp -> transform(atp)).collect(Collectors.toList());
    }

    protected BrokerError transform(
        com.pinterest.doctorkafka.avro.BrokerError abe){
      if ( abe == null ){
        return null;
      }
      switch (abe){
        case DiskFailure:
          return BrokerError.DISK_FAILURE;
        case JmxConnectionFailure:
          return BrokerError.JMX_CONNECTION_FAILURE;
        case JmxQueryFailure:
          return BrokerError.JMX_QUERY_FAILURE;
        case KafkaServerProperitiesFailure:
          return BrokerError.KAFKA_SERVER_PROPERTIES_FAILURE;
        case MissingKafkaProcess:
          return BrokerError.MISSING_KAFKA_PROCESS;
        case MultipleKafkaProcesses:
          return BrokerError.MULTIPLE_KAFKA_PROCESS;
        default:
        case UnknownError:
          return BrokerError.UNKOWN_ERROR;
      }
    }
  }
}