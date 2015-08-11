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

package org.apache.flink.streaming.connectors;

import org.apache.commons.collections.map.LinkedMap;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.internals.Fetcher;
import org.apache.flink.streaming.connectors.internals.LegacyFetcher;
import org.apache.flink.streaming.connectors.internals.NewConsumerApiFetcher;
import org.apache.flink.streaming.connectors.internals.OffsetHandler;
import org.apache.flink.streaming.connectors.internals.ZookeeperOffsetHandler;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.kafka_backport.common.KafkaException;
import org.apache.flink.kafka_backport.clients.consumer.ConsumerConfig;
import org.apache.flink.kafka_backport.clients.consumer.KafkaConsumer;
import org.apache.flink.kafka_backport.common.PartitionInfo;
import org.apache.flink.kafka_backport.common.TopicPartition;
import org.apache.flink.kafka_backport.common.serialization.ByteArrayDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
 *         Kafka (> 0.8.2).</li>
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
	private final DeserializationSchema<T> valueDeserializer;

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
	 * @param valueDeserializer
	 *           The deserializer to turn raw byte messages into Java/Scala objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 * @param offsetStore
	 *           The type of offset store to use (Kafka / ZooKeeper)
	 * @param fetcherType
	 *           The type of fetcher to use (new high-level API, old low-level API).
	 */
	public FlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props, 
								OffsetStore offsetStore, FetcherType fetcherType) {
		this.offsetStore = checkNotNull(offsetStore);
		this.fetcherType = checkNotNull(fetcherType);
		
		if (offsetStore == OffsetStore.KAFKA && fetcherType == FetcherType.LEGACY_LOW_LEVEL) {
			throw new IllegalArgumentException(
					"The Kafka offset handler cannot be used together with the old low-level fetcher.");
		}
		
		this.topic = checkNotNull(topic, "topic");
		this.props = checkNotNull(props, "props");
		this.valueDeserializer = checkNotNull(valueDeserializer, "valueDeserializer");

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

		// make sure that we take care of the committing
		props.setProperty("enable.auto.commit", "false");
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
				fetcher = new NewConsumerApiFetcher(props);
				break;
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
				if (fetcher instanceof NewConsumerApiFetcher) {
					offsetHandler = (NewConsumerApiFetcher) fetcher;
				} else {
					throw new Exception("Kafka offset handler cannot work with legacy fetcher");
				}
				break;
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
			fetcher.run(sourceContext, valueDeserializer, lastOffsets);
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
		return valueDeserializer.getProducedType();
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
		Map<TopicPartition, Long> offsetsToCommit = new HashMap<TopicPartition, Long>();
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
	
	// ------------------------------------------------------------------------
	//  Miscellaneous utilities 
	// ------------------------------------------------------------------------

	protected static List<TopicPartition> assignPartitions(int[] partitions, String topicName,
															int numConsumers, int consumerIndex) {
		checkArgument(numConsumers > 0);
		checkArgument(consumerIndex < numConsumers);
		
		List<TopicPartition> partitionsToSub = new ArrayList<TopicPartition>();

		for (int i = 0; i < partitions.length; i++) {
			if (i % numConsumers == consumerIndex) {
				partitionsToSub.add(new TopicPartition(topicName, partitions[i]));
			}
		}
		return partitionsToSub;
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
	public static List<PartitionInfo> getPartitionsForTopic(String topic, Properties properties) {
		// create a Kafka consumer to query the metadata
		// this is quite heavyweight
		KafkaConsumer<byte[], byte[]> consumer;
		try {
			consumer = new KafkaConsumer<byte[], byte[]>(properties, null,
				new ByteArrayDeserializer(), new ByteArrayDeserializer());
		}
		catch (KafkaException e) {
			throw new RuntimeException("Cannot access the Kafka partition metadata: " + e.getMessage(), e);
		}

		List<PartitionInfo> partitions;
		try {
			partitions = consumer.partitionsFor(topic);
		}
		finally {
			consumer.close();
		}

		if (partitions == null) {
			throw new RuntimeException("The topic " + topic + " does not seem to exist");
		}
		if (partitions.isEmpty()) {
			throw new RuntimeException("The topic "+topic+" does not seem to have any partitions");
		}
		return partitions;
	}
	
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
