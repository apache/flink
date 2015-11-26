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
import org.apache.commons.collections.map.LinkedMap;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.LegacyFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.OffsetHandler;
import org.apache.flink.streaming.connectors.kafka.internals.ZookeeperOffsetHandler;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.util.NetUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
 * Apache Kafka. The consumer can run in multiple parallel instances, each of which will pull
 * data from one or more Kafka partitions. 
 * 
 * <p>The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
 * during a failure, and that the computation processes elements "exactly once". 
 * (Note: These guarantees naturally assume that Kafka itself does not loose any data.)</p>
 * 
 * <p>To support a variety of Kafka brokers, protocol versions, and offset committing approaches,
 * the Flink Kafka Consumer can be parametrized with a <i>fetcher</i> and an <i>offset handler</i>.</p>
 *
 * <h1>Fetcher</h1>
 * 
 * <p>The fetcher is responsible to pull data from Kafka. Because Kafka has undergone a change in
 * protocols and APIs, there are currently two fetchers available:</p>
 * 
 * <ul>
 *     <li>{@link FetcherType#NEW_HIGH_LEVEL}: A fetcher based on the new Kafka consumer API.
 *         This fetcher is generally more robust, but works only with later versions of
 *         Kafka (&gt; 0.8.2).</li>
 *         
 *     <li>{@link FetcherType#LEGACY_LOW_LEVEL}: A fetcher based on the old low-level consumer API.
 *         This fetcher is works also with older versions of Kafka (0.8.1). The fetcher interprets
 *         the old Kafka consumer properties, like:
 *         <ul>
 *             <li>socket.timeout.ms</li>
 *             <li>socket.receive.buffer.bytes</li>
 *             <li>fetch.message.max.bytes</li>
 *             <li>auto.offset.reset with the values "latest", "earliest" (unlike 0.8.2 behavior)</li>
 *             <li>fetch.wait.max.ms</li>
 *         </ul>
 *     </li>
 * </ul>
 * 
 * <h1>Offset handler</h1>
 * 
 * <p>Offsets whose records have been read and are checkpointed will be committed back to Kafka / ZooKeeper
 * by the offset handler. In addition, the offset handler finds the point where the source initially
 * starts reading from the stream, when the streaming job is started.</p>
 * 
 * <p>Currently, the source offers two different offset handlers exist:</p>
 * <ul>
 *     <li>{@link OffsetStore#KAFKA}: Use this offset handler when the Kafka brokers are managing the offsets,
 *         and hence offsets need to be committed the Kafka brokers, rather than to ZooKeeper.
 *         Note that this offset handler works only on new versions of Kafka (0.8.2.x +) and
 *         with the {@link FetcherType#NEW_HIGH_LEVEL} fetcher.</li>
 *         
 *     <li>{@link OffsetStore#FLINK_ZOOKEEPER}: Use this offset handler when the offsets are managed
 *         by ZooKeeper, as in older versions of Kafka (0.8.1.x)</li>
 * </ul>
 * 
 * <p>Please note that Flink snapshots the offsets internally as part of its distributed checkpoints. The offsets
 * committed to Kafka / ZooKeeper are only to bring the outside view of progress in sync with Flink's view
 * of the progress. That way, monitoring and other jobs can get a view of how far the Flink Kafka consumer
 * has consumed a topic.</p>
 * 
 * <p><b>NOTE:</b> The implementation currently accesses partition metadata when the consumer
 * is constructed. That means that the client that submits the program needs to be able to
 * reach the Kafka brokers or ZooKeeper.</p>
 */
public class FlinkKafkaConsumer<T> extends RichParallelSourceFunction<T>
		implements CheckpointNotifier, CheckpointedAsynchronously<long[]>, ResultTypeQueryable<T> {

	/**
	 * The offset store defines how acknowledged offsets are committed back to Kafka. Different
	 * options include letting Flink periodically commit to ZooKeeper, or letting Kafka manage the
	 * offsets (new Kafka versions only).
	 */
	public enum OffsetStore {

		/**
		 * Let Flink manage the offsets. Flink will periodically commit them to Zookeeper (usually after
		 * successful checkpoints), in the same structure as Kafka 0.8.2.x
		 * 
		 * <p>Use this mode when using the source with Kafka 0.8.1.x brokers.</p>
		 */
		FLINK_ZOOKEEPER,

		/**
		 * Use the mechanisms in Kafka to commit offsets. Depending on the Kafka configuration, different
		 * mechanism will be used (broker coordinator, zookeeper)
		 */ 
		KAFKA
	}

	/**
	 * The fetcher type defines which code paths to use to pull data from teh Kafka broker.
	 */
	public enum FetcherType {

		/**
		 * The legacy fetcher uses Kafka's old low-level consumer API.
		 * 
		 * <p>Use this fetcher for Kafka 0.8.1 brokers.</p>
		 */
		LEGACY_LOW_LEVEL,

		/**
		 * This fetcher uses a backport of the new consumer API to pull data from the Kafka broker.
		 * It is the fetcher that will be maintained in the future, and it already 
		 * handles certain failure cases with less overhead than the legacy fetcher.
		 * 
		 * <p>This fetcher works only Kafka 0.8.2 and 0.8.3 (and future versions).</p>
		 */
		NEW_HIGH_LEVEL
	}
	
	// ------------------------------------------------------------------------
	
	private static final long serialVersionUID = -6272159445203409112L;
	
	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumer.class);

	/** Magic number to define an unset offset. Negative offsets are not used by Kafka (invalid),
	 * and we pick a number that is probably (hopefully) not used by Kafka as a magic number for anything else. */
	public static final long OFFSET_NOT_SET = -915623761776L;

	/** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks */
	public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

	/** Configuration key for the number of retries for getting the partition info */
	public static final String GET_PARTITIONS_RETRIES_KEY = "flink.get-partitions.retry";

	/** Default number of retries for getting the partition info. One retry means going through the full list of brokers */
	public static final int DEFAULT_GET_PARTITIONS_RETRIES = 3;

	
	
	// ------  Configuration of the Consumer -------
	
	/** The offset store where this consumer commits safe offsets */
	private final OffsetStore offsetStore;

	/** The type of fetcher to be used to pull data from Kafka */
	private final FetcherType fetcherType;
	
	/** name of the topic consumed by this source */
	private final String topic;
	
	/** The properties to parametrize the Kafka consumer and ZooKeeper client */ 
	private final Properties props;
	
	/** The ids of the partitions that are read by this consumer */
	private final int[] partitions;
	
	/** The schema to convert between Kafka#s byte messages, and Flink's objects */
	private final KeyedDeserializationSchema<T> deserializer;

	// ------  Runtime State  -------

	/** Data for pending but uncommitted checkpoints */
	private final LinkedMap pendingCheckpoints = new LinkedMap();
	
	/** The fetcher used to pull data from the Kafka brokers */
	private transient Fetcher fetcher;
	
	/** The committer that persists the committed offsets */
	private transient OffsetHandler offsetHandler;
	
	/** The partitions actually handled by this consumer */
	private transient List<TopicPartition> subscribedPartitions;

	/** The offsets of the last returned elements */
	private transient long[] lastOffsets;

	/** The latest offsets that have been committed to Kafka or ZooKeeper. These are never
	 * newer then the last offsets (Flink's internal view is fresher) */
	private transient long[] commitedOffsets;
	
	/** The offsets to restore to, if the consumer restores state from a checkpoint */
	private transient long[] restoreToOffset;
	
	private volatile boolean running = true;
	
	// ------------------------------------------------------------------------


	/**
	 * Creates a new Flink Kafka Consumer, using the given type of fetcher and offset handler.
	 *
	 * <p>To determine which kink of fetcher and offset handler to use, please refer to the docs
	 * at the beginnign of this class.</p>
	 *
	 * @param topic
	 *           The Kafka topic to read from.
	 * @param deserializer
	 *           The deserializer to turn raw byte messages (without key) into Java/Scala objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 * @param offsetStore
	 *           The type of offset store to use (Kafka / ZooKeeper)
	 * @param fetcherType
	 *           The type of fetcher to use (new high-level API, old low-level API).
	 */
	public FlinkKafkaConsumer(String topic, DeserializationSchema<T> deserializer, Properties props,
							OffsetStore offsetStore, FetcherType fetcherType) {
		this(topic, new KeyedDeserializationSchemaWrapper<>(deserializer),
				props, offsetStore, fetcherType);
	}

	/**
	 * Creates a new Flink Kafka Consumer, using the given type of fetcher and offset handler.
	 * 
	 * <p>To determine which kink of fetcher and offset handler to use, please refer to the docs
	 * at the beginnign of this class.</p>
	 * 
	 * @param topic 
	 *           The Kafka topic to read from.
	 * @param deserializer
	 *           The deserializer to turn raw byte messages into Java/Scala objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 * @param offsetStore
	 *           The type of offset store to use (Kafka / ZooKeeper)
	 * @param fetcherType
	 *           The type of fetcher to use (new high-level API, old low-level API).
	 */
	public FlinkKafkaConsumer(String topic, KeyedDeserializationSchema<T> deserializer, Properties props,
								OffsetStore offsetStore, FetcherType fetcherType) {
		this.offsetStore = checkNotNull(offsetStore);
		this.fetcherType = checkNotNull(fetcherType);

		if (fetcherType == FetcherType.NEW_HIGH_LEVEL) {
			throw new UnsupportedOperationException("The fetcher for Kafka 0.8.3 / 0.9.0 is not yet " +
					"supported in Flink");
		}
		if (offsetStore == OffsetStore.KAFKA && fetcherType == FetcherType.LEGACY_LOW_LEVEL) {
			throw new IllegalArgumentException(
					"The Kafka offset handler cannot be used together with the old low-level fetcher.");
		}
		
		this.topic = checkNotNull(topic, "topic");
		this.props = checkNotNull(props, "props");
		this.deserializer = checkNotNull(deserializer, "valueDeserializer");

		// validate the zookeeper properties
		if (offsetStore == OffsetStore.FLINK_ZOOKEEPER) {
			validateZooKeeperConfig(props);
		}
		
		// Connect to a broker to get the partitions
		List<PartitionInfo> partitionInfos = getPartitionsForTopic(topic, props);

		// get initial partitions list. The order of the partitions is important for consistent 
		// partition id assignment in restart cases.
		this.partitions = new int[partitionInfos.size()];
		for (int i = 0; i < partitionInfos.size(); i++) {
			partitions[i] = partitionInfos.get(i).partition();
			
			if (partitions[i] >= partitions.length) {
				throw new RuntimeException("Kafka partition numbers are sparse");
			}
		}
		LOG.info("Topic {} has {} partitions", topic, partitions.length);
	}

	// ------------------------------------------------------------------------
	//  Source life cycle
	// ------------------------------------------------------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		final int numConsumers = getRuntimeContext().getNumberOfParallelSubtasks();
		final int thisComsumerIndex = getRuntimeContext().getIndexOfThisSubtask();
		
		// pick which partitions we work on
		subscribedPartitions = assignPartitions(this.partitions, this.topic, numConsumers, thisComsumerIndex);
		
		if (LOG.isInfoEnabled()) {
			LOG.info("Kafka consumer {} will read partitions {} out of partitions {}",
					thisComsumerIndex, subscribedPartitions, Arrays.toString(partitions));
		}

		// we leave the fetcher as null, if we have no partitions
		if (subscribedPartitions.isEmpty()) {
			LOG.info("Kafka consumer {} has no partitions (empty source)", thisComsumerIndex);
			return;
		}
		
		// create fetcher
		switch (fetcherType){
			case NEW_HIGH_LEVEL:
				throw new UnsupportedOperationException("Currently unsupported");
			case LEGACY_LOW_LEVEL:
				fetcher = new LegacyFetcher(topic, props, getRuntimeContext().getTaskName());
				break;
			default:
				throw new RuntimeException("Requested unknown fetcher " + fetcher);
		}
		fetcher.setPartitionsToRead(subscribedPartitions);

		// offset handling
		switch (offsetStore){
			case FLINK_ZOOKEEPER:
				offsetHandler = new ZookeeperOffsetHandler(props);
				break;
			case KAFKA:
				throw new Exception("Kafka offset handler cannot work with legacy fetcher");
			default:
				throw new RuntimeException("Requested unknown offset store " + offsetStore);
		}
		
		// set up operator state
		lastOffsets = new long[partitions.length];
		commitedOffsets = new long[partitions.length];
		
		Arrays.fill(lastOffsets, OFFSET_NOT_SET);
		Arrays.fill(commitedOffsets, OFFSET_NOT_SET);
		
		// seek to last known pos, from restore request
		if (restoreToOffset != null) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Consumer {} found offsets from previous checkpoint: {}",
						thisComsumerIndex,  Arrays.toString(restoreToOffset));
			}
			
			for (int i = 0; i < restoreToOffset.length; i++) {
				long restoredOffset = restoreToOffset[i];
				if (restoredOffset != OFFSET_NOT_SET) {
					// if this fails because we are not subscribed to the topic, then the
					// partition assignment is not deterministic!
					
					// we set the offset +1 here, because seek() is accepting the next offset to read,
					// but the restore offset is the last read offset
					fetcher.seek(new TopicPartition(topic, i), restoredOffset + 1);
					lastOffsets[i] = restoredOffset;
				}
			}
		}
		else {
			// no restore request. Let the offset handler take care of the initial offset seeking
			offsetHandler.seekFetcherToInitialOffsets(subscribedPartitions, fetcher);
		}
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		if (fetcher != null) {
			// For non-checkpointed sources, a thread which periodically commits the current offset into ZK.
			PeriodicOffsetCommitter offsetCommitter = null;

			// check whether we need to start the periodic checkpoint committer
			StreamingRuntimeContext streamingRuntimeContext = (StreamingRuntimeContext) getRuntimeContext();
			if (!streamingRuntimeContext.isCheckpointingEnabled()) {
				// we use Kafka's own configuration parameter key for this.
				// Note that the default configuration value in Kafka is 60 * 1000, so we use the
				// same here.
				long commitInterval = Long.valueOf(props.getProperty("auto.commit.interval.ms", "60000"));
				offsetCommitter = new PeriodicOffsetCommitter(commitInterval, this);
				offsetCommitter.setDaemon(true);
				offsetCommitter.start();
				LOG.info("Starting periodic offset committer, with commit interval of {}ms", commitInterval);
			}

			try {
				fetcher.run(sourceContext, deserializer, lastOffsets);
			} finally {
				if (offsetCommitter != null) {
					offsetCommitter.close();
					offsetCommitter.join();
				}
			}
		}
		else {
			// this source never completes
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

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializer.getProducedType();
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and restore
	// ------------------------------------------------------------------------

	@Override
	public long[] snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (lastOffsets == null) {
			LOG.debug("snapshotState() requested on not yet opened source; returning null.");
			return null;
		}
		if (!running) {
			LOG.debug("snapshotState() called on closed source");
			return null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotting state. Offsets: {}, checkpoint id: {}, timestamp: {}",
					Arrays.toString(lastOffsets), checkpointId, checkpointTimestamp);
		}

		long[] currentOffsets = Arrays.copyOf(lastOffsets, lastOffsets.length);

		// the map cannot be asynchronously updated, because only one checkpoint call can happen
		// on this function at a time: either snapshotState() or notifyCheckpointComplete()
		pendingCheckpoints.put(checkpointId, currentOffsets);
			
		while (pendingCheckpoints.size() > MAX_NUM_PENDING_CHECKPOINTS) {
			pendingCheckpoints.remove(0);
		}

		return currentOffsets;
	}

	@Override
	public void restoreState(long[] restoredOffsets) {
		restoreToOffset = restoredOffsets;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (fetcher == null) {
			LOG.debug("notifyCheckpointComplete() called on uninitialized source");
			return;
		}
		if (!running) {
			LOG.debug("notifyCheckpointComplete() called on closed source");
			return;
		}
		
		// only one commit operation must be in progress
		if (LOG.isDebugEnabled()) {
			LOG.debug("Committing offsets externally for checkpoint {}", checkpointId);
		}

		try {
			long[] checkpointOffsets;
	
			// the map may be asynchronously updates when snapshotting state, so we synchronize
			synchronized (pendingCheckpoints) {
				final int posInMap = pendingCheckpoints.indexOf(checkpointId);
				if (posInMap == -1) {
					LOG.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
					return;
				}
	
				checkpointOffsets = (long[]) pendingCheckpoints.remove(posInMap);
				
				// remove older checkpoints in map
				for (int i = 0; i < posInMap; i++) {
					pendingCheckpoints.remove(0);
				}
			}
	
			if (LOG.isInfoEnabled()) {
				LOG.info("Committing offsets {} to offset store: {}", Arrays.toString(checkpointOffsets), offsetStore);
			}
	
			// build the map of (topic,partition) -> committed offset
			Map<TopicPartition, Long> offsetsToCommit = new HashMap<>();
			for (TopicPartition tp : subscribedPartitions) {
				
				int partition = tp.partition();
				long offset = checkpointOffsets[partition];
				long lastCommitted = commitedOffsets[partition];
				
				if (offset != OFFSET_NOT_SET) {
					if (offset > lastCommitted) {
						offsetsToCommit.put(tp, offset);
						LOG.debug("Committing offset {} for partition {}", offset, partition);
					}
					else {
						LOG.debug("Ignoring offset {} for partition {} because it is already committed", offset, partition);
					}
				}
			}
			
			offsetHandler.commit(offsetsToCommit);
		}
		catch (Exception e) {
			if (running) {
				throw e;
			}
			// else ignore exception if we are no longer running
		}
	}
	
	// ------------------------------------------------------------------------
	//  Miscellaneous utilities 
	// ------------------------------------------------------------------------

	protected static List<TopicPartition> assignPartitions(int[] partitions, String topicName,
															int numConsumers, int consumerIndex) {
		checkArgument(numConsumers > 0);
		checkArgument(consumerIndex < numConsumers);
		
		List<TopicPartition> partitionsToSub = new ArrayList<>();

		for (int i = 0; i < partitions.length; i++) {
			if (i % numConsumers == consumerIndex) {
				partitionsToSub.add(new TopicPartition(topicName, partitions[i]));
			}
		}
		return partitionsToSub;
	}

	/**
	 * Thread to periodically commit the current read offset into Zookeeper.
	 */
	private static class PeriodicOffsetCommitter extends Thread {
		private final long commitInterval;
		private final FlinkKafkaConsumer consumer;
		private volatile boolean running = true;

		public PeriodicOffsetCommitter(long commitInterval, FlinkKafkaConsumer consumer) {
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
						long[] currentOffsets = Arrays.copyOf(consumer.lastOffsets, consumer.lastOffsets.length);

						Map<TopicPartition, Long> offsetsToCommit = new HashMap<>();
						for (TopicPartition tp : (List<TopicPartition>)consumer.subscribedPartitions) {
							int partition = tp.partition();
							long offset = currentOffsets[partition];
							long lastCommitted = consumer.commitedOffsets[partition];

							if (offset != OFFSET_NOT_SET) {
								if (offset > lastCommitted) {
									offsetsToCommit.put(tp, offset);
									LOG.debug("Committing offset {} for partition {}", offset, partition);
								} else {
									LOG.debug("Ignoring offset {} for partition {} because it is already committed", offset, partition);
								}
							}
						}

						consumer.offsetHandler.commit(offsetsToCommit);
					} catch (InterruptedException e) {
						if (running) {
							// throw unexpected interruption
							throw e;
						}
						// looks like the thread is being closed. Leave loop
						break;
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
	 * @param topic The name of the topic.
	 * @param properties The properties for the Kafka Consumer that is used to query the partitions for the topic. 
	 */
	public static List<PartitionInfo> getPartitionsForTopic(final String topic, final Properties properties) {
		String seedBrokersConfString = properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
		final int numRetries = Integer.valueOf(properties.getProperty(GET_PARTITIONS_RETRIES_KEY, Integer.toString(DEFAULT_GET_PARTITIONS_RETRIES)));

		checkNotNull(seedBrokersConfString, "Configuration property " + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " not set");
		String[] seedBrokers = seedBrokersConfString.split(",");
		List<PartitionInfo> partitions = new ArrayList<>();

		Random rnd = new Random();
		retryLoop: for(int retry = 0; retry < numRetries; retry++) {
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
					final String clientId = "flink-kafka-consumer-partition-lookup";
					final int soTimeout = Integer.valueOf(properties.getProperty("socket.timeout.ms", "30000"));
					final int bufferSize = Integer.valueOf(properties.getProperty("socket.receive.buffer.bytes", "65536"));
					consumer = new SimpleConsumer(brokerUrl.getHost(), brokerUrl.getPort(), soTimeout, bufferSize, clientId);

					List<String> topics = Collections.singletonList(topic);
					TopicMetadataRequest req = new TopicMetadataRequest(topics);
					kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

					List<TopicMetadata> metaData = resp.topicsMetadata();

					// clear in case we have an incomplete list from previous tries
					partitions.clear();
					for (TopicMetadata item : metaData) {
						if (item.errorCode() != ErrorMapping.NoError()) {
							if (item.errorCode() == ErrorMapping.InvalidTopicCode() || item.errorCode() == ErrorMapping.UnknownTopicOrPartitionCode()) {
								// fail hard if topic is unknown
								throw new RuntimeException("Requested partitions for unknown topic", ErrorMapping.exceptionFor(item.errorCode()));
							}
							// warn and try more brokers
							LOG.warn("Error while getting metadata from broker " + seedBroker + " to find partitions for " + topic,
									ErrorMapping.exceptionFor(item.errorCode()));
							continue brokersLoop;
						}
						if (!item.topic().equals(topic)) {
							LOG.warn("Received metadata from topic " + item.topic() + " even though it was not requested. Skipping ...");
							continue brokersLoop;
						}
						for (PartitionMetadata part : item.partitionsMetadata()) {
							Node leader = brokerToNode(part.leader());
							Node[] replicas = new Node[part.replicas().size()];
							for (int i = 0; i < part.replicas().size(); i++) {
								replicas[i] = brokerToNode(part.replicas().get(i));
							}

							Node[] ISRs = new Node[part.isr().size()];
							for (int i = 0; i < part.isr().size(); i++) {
								ISRs[i] = brokerToNode(part.isr().get(i));
							}
							PartitionInfo pInfo = new PartitionInfo(topic, part.partitionId(), leader, replicas, ISRs);
							partitions.add(pInfo);
						}
					}
					break retryLoop; // leave the loop through the brokers
				} catch (Exception e) {
					LOG.warn("Error communicating with broker " + seedBroker + " to find partitions for " + topic, e);
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
