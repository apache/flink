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

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.internals.Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.LegacyFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.OffsetHandler;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionLeader;
import org.apache.flink.streaming.connectors.kafka.internals.ZookeeperOffsetHandler;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.util.NetUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.streaming.connectors.kafka.util.KafkaUtils.getIntFromConfig;
import static org.apache.flink.streaming.connectors.kafka.util.KafkaUtils.getLongFromConfig;


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
	
	// ------------------------------------------------------------------------
	
	private static final long serialVersionUID = -6272159445203409112L;
	
	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumer08.class);

	/** Magic number to define an unset offset. Negative offsets are not used by Kafka (invalid),
	 * and we pick a number that is probably (hopefully) not used by Kafka as a magic number for anything else. */
	public static final long OFFSET_NOT_SET = -915623761776L;


	/** Configuration key for the number of retries for getting the partition info */
	public static final String GET_PARTITIONS_RETRIES_KEY = "flink.get-partitions.retry";

	/** Default number of retries for getting the partition info. One retry means going through the full list of brokers */
	public static final int DEFAULT_GET_PARTITIONS_RETRIES = 3;

	
	// ------  Configuration of the Consumer -------

	/** Initial list of topics and partitions to consume  */
	private final List<KafkaTopicPartition> partitionInfos;
	
	/** The properties to parametrize the Kafka consumer and ZooKeeper client */ 
	private final Properties props;


	// ------  Runtime State  -------
	
	/** The fetcher used to pull data from the Kafka brokers */
	private transient Fetcher fetcher;
	
	/** The committer that persists the committed offsets */
	private transient OffsetHandler offsetHandler;
	
	/** The partitions actually handled by this consumer at runtime */
	private transient List<KafkaTopicPartition> subscribedPartitions;

	/** The latest offsets that have been committed to Kafka or ZooKeeper. These are never
	 * newer then the last offsets (Flink's internal view is fresher) */
	private transient HashMap<KafkaTopicPartition, Long> committedOffsets;

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
		super(deserializer, props);

		requireNonNull(topics, "topics");
		this.props = requireNonNull(props, "props");

		// validate the zookeeper properties
		validateZooKeeperConfig(props);

		// Connect to a broker to get the partitions for all topics
		this.partitionInfos = KafkaTopicPartition.dropLeaderData(getPartitionsForTopic(topics, props));

		if (partitionInfos.size() == 0) {
			throw new RuntimeException("Unable to retrieve any partitions for the requested topics " + topics.toString() + "." +
					"Please check previous log entries");
		}

		if (LOG.isInfoEnabled()) {
			logPartitionInfo(partitionInfos);
		}
	}

	// ------------------------------------------------------------------------
	//  Source life cycle
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		final int numConsumers = getRuntimeContext().getNumberOfParallelSubtasks();
		final int thisConsumerIndex = getRuntimeContext().getIndexOfThisSubtask();
		
		// pick which partitions we work on
		subscribedPartitions = assignPartitions(this.partitionInfos, numConsumers, thisConsumerIndex);
		
		if (LOG.isInfoEnabled()) {
			LOG.info("Kafka consumer {} will read partitions {} out of partitions {}",
					thisConsumerIndex, KafkaTopicPartition.toString(subscribedPartitions), this.partitionInfos.size());
		}

		// we leave the fetcher as null, if we have no partitions
		if (subscribedPartitions.isEmpty()) {
			LOG.info("Kafka consumer {} has no partitions (empty source)", thisConsumerIndex);
			this.fetcher = null; // fetcher remains null
			return;
		}
		

		// offset handling
		offsetHandler = new ZookeeperOffsetHandler(props);

		committedOffsets = new HashMap<>();

		Map<KafkaTopicPartition, Long> subscribedPartitionsWithOffsets = new HashMap<>(subscribedPartitions.size());
		// initially load the map with "offset not set"
		for(KafkaTopicPartition ktp: subscribedPartitions) {
			subscribedPartitionsWithOffsets.put(ktp, FlinkKafkaConsumer08.OFFSET_NOT_SET);
		}

		// seek to last known pos, from restore request
		if (restoreToOffset != null) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Consumer {} is restored from previous checkpoint: {}",
						thisConsumerIndex, KafkaTopicPartition.toString(restoreToOffset));
			}
			// initialize offsets with restored state
			this.offsetsState = restoreToOffset;
			subscribedPartitionsWithOffsets.putAll(restoreToOffset);
			restoreToOffset = null;
		}
		else {
			// start with empty offsets
			offsetsState = new HashMap<>();

			// no restore request: overwrite offsets.
			subscribedPartitionsWithOffsets.putAll(offsetHandler.getOffsets(subscribedPartitions));
		}
		if(subscribedPartitionsWithOffsets.size() != subscribedPartitions.size()) {
			throw new IllegalStateException("The subscribed partitions map has more entries than the subscribed partitions " +
					"list: " + subscribedPartitionsWithOffsets.size() + "," + subscribedPartitions.size());
		}

		// create fetcher
		fetcher = new LegacyFetcher(subscribedPartitionsWithOffsets, props,
				getRuntimeContext().getTaskName(), getRuntimeContext().getUserCodeClassLoader());
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		if (fetcher != null) {
			// For non-checkpointed sources, a thread which periodically commits the current offset into ZK.
			PeriodicOffsetCommitter<T> offsetCommitter = null;

			// check whether we need to start the periodic checkpoint committer
			StreamingRuntimeContext streamingRuntimeContext = (StreamingRuntimeContext) getRuntimeContext();
			if (!streamingRuntimeContext.isCheckpointingEnabled()) {
				// we use Kafka's own configuration parameter key for this.
				// Note that the default configuration value in Kafka is 60 * 1000, so we use the
				// same here.
				long commitInterval = getLongFromConfig(props, "auto.commit.interval.ms", 60000);
				offsetCommitter = new PeriodicOffsetCommitter<>(commitInterval, this);
				offsetCommitter.setDaemon(true);
				offsetCommitter.start();
				LOG.info("Starting periodic offset committer, with commit interval of {}ms", commitInterval);
			}

			try {
				fetcher.run(sourceContext, deserializer, offsetsState);
			} finally {
				if (offsetCommitter != null) {
					offsetCommitter.close();
					try {
						offsetCommitter.join();
					} catch(InterruptedException ie) {
						// ignore interrupt
					}
				}
			}
		}
		else {
			// this source never completes, so emit a Long.MAX_VALUE watermark
			// to not block watermark forwarding
			if (getRuntimeContext().getExecutionConfig().areTimestampsEnabled()) {
				sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));
			}

			final Object waitLock = new Object();
			while (running) {
				// wait until we are canceled
				try {
					//noinspection SynchronizationOnLocalVariableOrMethodParameter
					synchronized (waitLock) {
						waitLock.wait();
					}
				}
				catch (InterruptedException e) {
					// do nothing, check our "running" status
				}
			}
		}
		
		// close the context after the work was done. this can actually only
		// happen when the fetcher decides to stop fetching
		sourceContext.close();
	}

	@Override
	public void cancel() {
		// set ourselves as not running
		running = false;
		
		// close the fetcher to interrupt any work
		Fetcher fetcher = this.fetcher;
		this.fetcher = null;
		if (fetcher != null) {
			try {
				fetcher.close();
			}
			catch (IOException e) {
				LOG.warn("Error while closing Kafka connector data fetcher", e);
			}
		}
		
		OffsetHandler offsetHandler = this.offsetHandler;
		this.offsetHandler = null;
		if (offsetHandler != null) {
			try {
				offsetHandler.close();
			}
			catch (IOException e) {
				LOG.warn("Error while closing Kafka connector offset handler", e);
			}
		}
	}

	@Override
	public void close() throws Exception {
		cancel();
		super.close();
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and restore
	// ------------------------------------------------------------------------

	/**
	 * Utility method to commit offsets.
	 *
	 * @param toCommit the offsets to commit
	 * @throws Exception
	 */
	@Override
	protected void commitOffsets(HashMap<KafkaTopicPartition, Long> toCommit) throws Exception {
		Map<KafkaTopicPartition, Long> offsetsToCommit = new HashMap<>();
		for (KafkaTopicPartition tp : this.subscribedPartitions) {
			Long offset = toCommit.get(tp);
			if(offset == null) {
				// There was no data ever consumed from this topic, that's why there is no entry
				// for this topicPartition in the map.
				continue;
			}
			Long lastCommitted = this.committedOffsets.get(tp);
			if (lastCommitted == null) {
				lastCommitted = OFFSET_NOT_SET;
			}
			if (offset != OFFSET_NOT_SET) {
				if (offset > lastCommitted) {
					offsetsToCommit.put(tp, offset);
					this.committedOffsets.put(tp, offset);
					LOG.debug("Committing offset {} for partition {}", offset, tp);
				} else {
					LOG.debug("Ignoring offset {} for partition {} because it is already committed", offset, tp);
				}
			}
		}

		if (LOG.isDebugEnabled() && offsetsToCommit.size() > 0) {
			LOG.debug("Committing offsets {} to Zookeeper", KafkaTopicPartition.toString(offsetsToCommit));
		}

		this.offsetHandler.commit(offsetsToCommit);
	}

	// ------------------------------------------------------------------------
	//  Miscellaneous utilities 
	// ------------------------------------------------------------------------

	/**
	 * Thread to periodically commit the current read offset into Zookeeper.
	 */
	private static class PeriodicOffsetCommitter<T> extends Thread {
		private final long commitInterval;
		private final FlinkKafkaConsumer08<T> consumer;
		private volatile boolean running = true;

		public PeriodicOffsetCommitter(long commitInterval, FlinkKafkaConsumer08<T> consumer) {
			this.commitInterval = commitInterval;
			this.consumer = consumer;
		}

		@Override
		public void run() {
			try {

				while (running) {
					try {
						Thread.sleep(commitInterval);
						//  ------------  commit current offsets ----------------

						// create copy of current offsets
						@SuppressWarnings("unchecked")
						HashMap<KafkaTopicPartition, Long> currentOffsets = (HashMap<KafkaTopicPartition, Long>) consumer.offsetsState.clone();
						consumer.commitOffsets(currentOffsets);
					} catch (InterruptedException e) {
						if (running) {
							// throw unexpected interruption
							throw e;
						}
					}
				}
			} catch (Throwable t) {
				LOG.warn("Periodic checkpoint committer is stopping the fetcher because of an error", t);
				consumer.fetcher.stopWithError(t);
			}
		}

		public void close() {
			this.running = false;
			this.interrupt();
		}

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
	public static List<KafkaTopicPartitionLeader> getPartitionsForTopic(final List<String> topics, final Properties properties) {
		String seedBrokersConfString = properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
		final int numRetries = getIntFromConfig(properties, GET_PARTITIONS_RETRIES_KEY, DEFAULT_GET_PARTITIONS_RETRIES);

		requireNonNull(seedBrokersConfString, "Configuration property " + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " not set");
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
}
