/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;

import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.Kafka08Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionLeader;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.apache.flink.streaming.connectors.kafka.util.KafkaUtils.getIntFromConfig;
import static org.apache.flink.streaming.connectors.kafka.util.KafkaUtils.getLongFromConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
 * Apache Kafka 0.8.x. The consumer can run in multiple parallel instances, each of which will pull
 * data from one or more Kafka partitions. 
 * 
 * <p>The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
 * during a failure, and that the computation processes elements "exactly once". 
 * (Note: These guarantees naturally assume that Kafka itself does not loose any data.)</p>
 * 
 * <p>Flink's Kafka Consumer is designed to be compatible with Kafka's High-Level Consumer API (0.8.x).
 * Most of Kafka's configuration variables can be used with this consumer as well:
 *         <ul>
 *             <li>socket.timeout.ms</li>
 *             <li>socket.receive.buffer.bytes</li>
 *             <li>fetch.message.max.bytes</li>
 *             <li>auto.offset.reset with the values "largest", "smallest"</li>
 *             <li>fetch.wait.max.ms</li>
 *         </ul>
 *     </li>
 * </ul>
 * 
 * <h1>Offset handling</h1>
 * 
 * <p>Offsets whose records have been read and are checkpointed will be committed back to ZooKeeper
 * by the offset handler. In addition, the offset handler finds the point where the source initially
 * starts reading from the stream, when the streaming job is started.</p>
 *
 * <p>Please note that Flink snapshots the offsets internally as part of its distributed checkpoints. The offsets
 * committed to Kafka / ZooKeeper are only to bring the outside view of progress in sync with Flink's view
 * of the progress. That way, monitoring and other jobs can get a view of how far the Flink Kafka consumer
 * has consumed a topic.</p>
 *
 * <p>If checkpointing is disabled, the consumer will periodically commit the current offset
 * to Zookeeper.</p>
 *
 * <p>When using a Kafka topic to send data between Flink jobs, we recommend using the
 * {@see TypeInformationSerializationSchema} and {@see TypeInformationKeyValueSerializationSchema}.</p>
 * 
 * <p><b>NOTE:</b> The implementation currently accesses partition metadata when the consumer
 * is constructed. That means that the client that submits the program needs to be able to
 * reach the Kafka brokers or ZooKeeper.</p>
 */
public class FlinkKafkaConsumer08<T> extends FlinkKafkaConsumerBase<T> {

	private static final long serialVersionUID = -6272159445203409112L;

	/** Configuration key for the number of retries for getting the partition info */
	public static final String GET_PARTITIONS_RETRIES_KEY = "flink.get-partitions.retry";

	/** Default number of retries for getting the partition info. One retry means going through the full list of brokers */
	public static final int DEFAULT_GET_PARTITIONS_RETRIES = 3;

	// ------------------------------------------------------------------------

	/** The properties to parametrize the Kafka consumer and ZooKeeper client */ 
	private final Properties kafkaProperties;

	/** The behavior when encountering an invalid offset (see {@link OffsetRequest}) */
	private final long invalidOffsetBehavior;

	/** The interval in which to automatically commit (-1 if deactivated) */
	private final long autoCommitInterval;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer08(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(Collections.singletonList(topic), valueDeserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer08(String topic, KeyedDeserializationSchema<T> deserializer, Properties props) {
		this(Collections.singletonList(topic), deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * This constructor allows passing multiple topics to the consumer.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer08(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
		this(topics, new KeyedDeserializationSchemaWrapper<>(deserializer), props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * This constructor allows passing multiple topics and a key/value deserialization schema.
	 * 
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer08(List<String> topics, KeyedDeserializationSchema<T> deserializer, Properties props) {
		super(deserializer);

		checkNotNull(topics, "topics");
		this.kafkaProperties = checkNotNull(props, "props");

		// validate the zookeeper properties
		validateZooKeeperConfig(props);

		this.invalidOffsetBehavior = getInvalidOffsetBehavior(props);
		this.autoCommitInterval = getLongFromConfig(props, "auto.commit.interval.ms", 60000);

		// Connect to a broker to get the partitions for all topics
		List<KafkaTopicPartition> partitionInfos = 
				KafkaTopicPartition.dropLeaderData(getPartitionsForTopic(topics, props));

		if (partitionInfos.size() == 0) {
			throw new RuntimeException(
					"Unable to retrieve any partitions for the requested topics " + topics + 
							". Please check previous log entries");
		}

		if (LOG.isInfoEnabled()) {
			logPartitionInfo(LOG, partitionInfos);
		}

		setSubscribedPartitions(partitionInfos);
	}

	@Override
	protected AbstractFetcher<T, ?> createFetcher(
			SourceContext<T> sourceContext,
			List<KafkaTopicPartition> thisSubtaskPartitions,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext) throws Exception {

		return new Kafka08Fetcher<>(sourceContext, thisSubtaskPartitions,
				watermarksPeriodic, watermarksPunctuated,
				runtimeContext, deserializer, kafkaProperties,
				invalidOffsetBehavior, autoCommitInterval);
	}

	// ------------------------------------------------------------------------
	//  Kafka / ZooKeeper communication utilities
	// ------------------------------------------------------------------------

	/**
	 * Send request to Kafka to get partitions for topic.
	 * 
	 * @param topics The name of the topics.
	 * @param properties The properties for the Kafka Consumer that is used to query the partitions for the topic. 
	 */
	public static List<KafkaTopicPartitionLeader> getPartitionsForTopic(List<String> topics, Properties properties) {
		String seedBrokersConfString = properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
		final int numRetries = getIntFromConfig(properties, GET_PARTITIONS_RETRIES_KEY, DEFAULT_GET_PARTITIONS_RETRIES);

		checkNotNull(seedBrokersConfString, "Configuration property %s not set", ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
		String[] seedBrokers = seedBrokersConfString.split(",");
		List<KafkaTopicPartitionLeader> partitions = new ArrayList<>();

		final String clientId = "flink-kafka-consumer-partition-lookup";
		final int soTimeout = getIntFromConfig(properties, "socket.timeout.ms", 30000);
		final int bufferSize = getIntFromConfig(properties, "socket.receive.buffer.bytes", 65536);

		Random rnd = new Random();
		retryLoop: for (int retry = 0; retry < numRetries; retry++) {
			// we pick a seed broker randomly to avoid overloading the first broker with all the requests when the
			// parallel source instances start. Still, we try all available brokers.
			int index = rnd.nextInt(seedBrokers.length);
			brokersLoop: for (int arrIdx = 0; arrIdx < seedBrokers.length; arrIdx++) {
				String seedBroker = seedBrokers[index];
				LOG.info("Trying to get topic metadata from broker {} in try {}/{}", seedBroker, retry, numRetries);
				if (++index == seedBrokers.length) {
					index = 0;
				}

				URL brokerUrl = NetUtils.getCorrectHostnamePort(seedBroker);
				SimpleConsumer consumer = null;
				try {
					consumer = new SimpleConsumer(brokerUrl.getHost(), brokerUrl.getPort(), soTimeout, bufferSize, clientId);

					TopicMetadataRequest req = new TopicMetadataRequest(topics);
					kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

					List<TopicMetadata> metaData = resp.topicsMetadata();

					// clear in case we have an incomplete list from previous tries
					partitions.clear();
					for (TopicMetadata item : metaData) {
						if (item.errorCode() != ErrorMapping.NoError()) {
							// warn and try more brokers
							LOG.warn("Error while getting metadata from broker " + seedBroker + " to find partitions " +
									"for " + topics.toString() + ". Error: " + ErrorMapping.exceptionFor(item.errorCode()).getMessage());
							continue brokersLoop;
						}
						if (!topics.contains(item.topic())) {
							LOG.warn("Received metadata from topic " + item.topic() + " even though it was not requested. Skipping ...");
							continue brokersLoop;
						}
						for (PartitionMetadata part : item.partitionsMetadata()) {
							Node leader = brokerToNode(part.leader());
							KafkaTopicPartition ktp = new KafkaTopicPartition(item.topic(), part.partitionId());
							KafkaTopicPartitionLeader pInfo = new KafkaTopicPartitionLeader(ktp, leader);
							partitions.add(pInfo);
						}
					}
					break retryLoop; // leave the loop through the brokers
				} catch (Exception e) {
					LOG.warn("Error communicating with broker " + seedBroker + " to find partitions for " + topics.toString() + "." +
							"" + e.getClass() + ". Message: " + e.getMessage());
					LOG.debug("Detailed trace", e);
					// we sleep a bit. Retrying immediately doesn't make sense in cases where Kafka is reorganizing the leader metadata
					try {
						Thread.sleep(500);
					} catch (InterruptedException e1) {
						// sleep shorter.
					}
				} finally {
					if (consumer != null) {
						consumer.close();
					}
				}
			} // brokers loop
		} // retries loop
		return partitions;
	}

	/**
	 * Turn a broker instance into a node instance
	 * @param broker broker instance
	 * @return Node representing the given broker
	 */
	private static Node brokerToNode(Broker broker) {
		return new Node(broker.id(), broker.host(), broker.port());
	}

	/**
	 * Validate the ZK configuration, checking for required parameters
	 * @param props Properties to check
	 */
	protected static void validateZooKeeperConfig(Properties props) {
		if (props.getProperty("zookeeper.connect") == null) {
			throw new IllegalArgumentException("Required property 'zookeeper.connect' has not been set in the properties");
		}
		if (props.getProperty(ConsumerConfig.GROUP_ID_CONFIG) == null) {
			throw new IllegalArgumentException("Required property '" + ConsumerConfig.GROUP_ID_CONFIG
					+ "' has not been set in the properties");
		}
		
		try {
			//noinspection ResultOfMethodCallIgnored
			Integer.parseInt(props.getProperty("zookeeper.session.timeout.ms", "0"));
		}
		catch (NumberFormatException e) {
			throw new IllegalArgumentException("Property 'zookeeper.session.timeout.ms' is not a valid integer");
		}
		
		try {
			//noinspection ResultOfMethodCallIgnored
			Integer.parseInt(props.getProperty("zookeeper.connection.timeout.ms", "0"));
		}
		catch (NumberFormatException e) {
			throw new IllegalArgumentException("Property 'zookeeper.connection.timeout.ms' is not a valid integer");
		}
	}

	private static long getInvalidOffsetBehavior(Properties config) {
		final String val = config.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
		if (val.equals("none")) {
			throw new IllegalArgumentException("Cannot use '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
					+ "' value 'none'. Possible values: 'latest', 'largest', or 'earliest'.");
		}
		else if (val.equals("largest") || val.equals("latest")) { // largest is kafka 0.8, latest is kafka 0.9
			return OffsetRequest.LatestTime();
		} else {
			return OffsetRequest.EarliestTime();
		}
	}
}
