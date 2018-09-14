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

package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.KinesisShardAssigner;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.metrics.KinesisConsumerMetricConstants;
import org.apache.flink.streaming.connectors.kinesis.metrics.ShardMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.util.InstantiationUtil;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A KinesisDataFetcher is responsible for fetching data from multiple Kinesis shards. Each parallel subtask instantiates
 * and runs a single fetcher throughout the subtask's lifetime. The fetcher accomplishes the following:
 * <ul>
 *     <li>1. continuously poll Kinesis to discover shards that the subtask should subscribe to. The subscribed subset
 *     		  of shards, including future new shards, is non-overlapping across subtasks (no two subtasks will be
 *     		  subscribed to the same shard) and determinate across subtask restores (the subtask will always subscribe
 *     		  to the same subset of shards even after restoring)</li>
 *     <li>2. decide where in each discovered shard should the fetcher start subscribing to</li>
 *     <li>3. subscribe to shards by creating a single thread for each shard</li>
 * </ul>
 *
 * <p>The fetcher manages two states: 1) last seen shard ids of each subscribed stream (used for continuous shard discovery),
 * and 2) last processed sequence numbers of each subscribed shard. Since operations on the second state will be performed
 * by multiple threads, these operations should only be done using the handler methods provided in this class.
 */
@Internal
public class KinesisDataFetcher<T> {

	public static final KinesisShardAssigner DEFAULT_SHARD_ASSIGNER = (shard, subtasks) -> shard.hashCode();

	private static final Logger LOG = LoggerFactory.getLogger(KinesisDataFetcher.class);

	// ------------------------------------------------------------------------
	//  Consumer-wide settings
	// ------------------------------------------------------------------------

	/** Configuration properties for the Flink Kinesis Consumer. */
	private final Properties configProps;

	/** The list of Kinesis streams that the consumer is subscribing to. */
	private final List<String> streams;

	/**
	 * The deserialization schema we will be using to convert Kinesis records to Flink objects.
	 * Note that since this might not be thread-safe, {@link ShardConsumer}s using this must
	 * clone a copy using {@link KinesisDataFetcher#getClonedDeserializationSchema()}.
	 */
	private final KinesisDeserializationSchema<T> deserializationSchema;

	/**
	 * The function that determines which subtask a shard should be assigned to.
	 */
	private final KinesisShardAssigner shardAssigner;

	// ------------------------------------------------------------------------
	//  Consumer metrics
	// ------------------------------------------------------------------------

	/** The metric group that all metrics should be registered to. */
	private final MetricGroup consumerMetricGroup;

	// ------------------------------------------------------------------------
	//  Subtask-specific settings
	// ------------------------------------------------------------------------

	/** Runtime context of the subtask that this fetcher was created in. */
	private final RuntimeContext runtimeContext;

	private final int totalNumberOfConsumerSubtasks;

	private final int indexOfThisConsumerSubtask;

	// ------------------------------------------------------------------------
	//  Executor services to run created threads
	// ------------------------------------------------------------------------

	/** Executor service to run {@link ShardConsumer}s to consume Kinesis shards. */
	private final ExecutorService shardConsumersExecutor;

	// ------------------------------------------------------------------------
	//  Managed state, accessed and updated across multiple threads
	// ------------------------------------------------------------------------

	/** The last discovered shard ids of each subscribed stream, updated as the fetcher discovers new shards in.
	 * Note: this state will be updated if new shards are found when {@link KinesisDataFetcher#discoverNewShardsToSubscribe()} is called.
	 */
	private final Map<String, String> subscribedStreamsToLastDiscoveredShardIds;

	/**
	 * The shards, along with their last processed sequence numbers, that this fetcher is subscribed to. The fetcher
	 * will add new subscribed shard states to this list as it discovers new shards. {@link ShardConsumer} threads update
	 * the last processed sequence number of subscribed shards as they fetch and process records.
	 *
	 * <p>Note that since multiple {@link ShardConsumer} threads will be performing operations on this list, all operations
	 * must be wrapped in synchronized blocks on the {@link KinesisDataFetcher#checkpointLock} lock. For this purpose,
	 * all threads must use the following thread-safe methods this class provides to operate on this list:
	 * <ul>
	 *     <li>{@link KinesisDataFetcher#registerNewSubscribedShardState(KinesisStreamShardState)}</li>
	 *     <li>{@link KinesisDataFetcher#updateState(int, SequenceNumber)}</li>
	 *     <li>{@link KinesisDataFetcher#emitRecordAndUpdateState(T, long, int, SequenceNumber)}</li>
	 * </ul>
	 */
	private final List<KinesisStreamShardState> subscribedShardsState;

	private final SourceFunction.SourceContext<T> sourceContext;

	/** Checkpoint lock, also used to synchronize operations on subscribedShardsState. */
	private final Object checkpointLock;

	/** Reference to the first error thrown by any of the {@link ShardConsumer} threads. */
	private final AtomicReference<Throwable> error;

	/** The Kinesis proxy factory that will be used to create instances for discovery and shard consumers. */
	private final FlinkKinesisProxyFactory kinesisProxyFactory;

	/** The Kinesis proxy that the fetcher will be using to discover new shards. */
	private final KinesisProxyInterface kinesis;

	/** Thread that executed runFetcher(). */
	private volatile Thread mainThread;

	/**
	 * The current number of shards that are actively read by this fetcher.
	 *
	 * <p>This value is updated in {@link KinesisDataFetcher#registerNewSubscribedShardState(KinesisStreamShardState)},
	 * and {@link KinesisDataFetcher#updateState(int, SequenceNumber)}.
	 */
	private final AtomicInteger numberOfActiveShards = new AtomicInteger(0);

	private volatile boolean running = true;

	/**
	 * Factory to create Kinesis proxy instances used by a fetcher.
	 */
	public interface FlinkKinesisProxyFactory {
		KinesisProxyInterface create(Properties configProps);
	}

	/**
	 * Creates a Kinesis Data Fetcher.
	 *
	 * @param streams the streams to subscribe to
	 * @param sourceContext context of the source function
	 * @param runtimeContext this subtask's runtime context
	 * @param configProps the consumer configuration properties
	 * @param deserializationSchema deserialization schema
	 */
	public KinesisDataFetcher(List<String> streams,
							SourceFunction.SourceContext<T> sourceContext,
							RuntimeContext runtimeContext,
							Properties configProps,
							KinesisDeserializationSchema<T> deserializationSchema,
							KinesisShardAssigner shardAssigner) {
		this(streams,
			sourceContext,
			sourceContext.getCheckpointLock(),
			runtimeContext,
			configProps,
			deserializationSchema,
			shardAssigner,
			new AtomicReference<>(),
			new ArrayList<>(),
			createInitialSubscribedStreamsToLastDiscoveredShardsState(streams),
			KinesisProxy::create);
	}

	@VisibleForTesting
	protected KinesisDataFetcher(List<String> streams,
								SourceFunction.SourceContext<T> sourceContext,
								Object checkpointLock,
								RuntimeContext runtimeContext,
								Properties configProps,
								KinesisDeserializationSchema<T> deserializationSchema,
								KinesisShardAssigner shardAssigner,
								AtomicReference<Throwable> error,
								List<KinesisStreamShardState> subscribedShardsState,
								HashMap<String, String> subscribedStreamsToLastDiscoveredShardIds,
								FlinkKinesisProxyFactory kinesisProxyFactory) {
		this.streams = checkNotNull(streams);
		this.configProps = checkNotNull(configProps);
		this.sourceContext = checkNotNull(sourceContext);
		this.checkpointLock = checkNotNull(checkpointLock);
		this.runtimeContext = checkNotNull(runtimeContext);
		this.totalNumberOfConsumerSubtasks = runtimeContext.getNumberOfParallelSubtasks();
		this.indexOfThisConsumerSubtask = runtimeContext.getIndexOfThisSubtask();
		this.deserializationSchema = checkNotNull(deserializationSchema);
		this.shardAssigner = checkNotNull(shardAssigner);
		this.kinesisProxyFactory = checkNotNull(kinesisProxyFactory);
		this.kinesis = kinesisProxyFactory.create(configProps);

		this.consumerMetricGroup = runtimeContext.getMetricGroup()
			.addGroup(KinesisConsumerMetricConstants.KINESIS_CONSUMER_METRICS_GROUP);

		this.error = checkNotNull(error);
		this.subscribedShardsState = checkNotNull(subscribedShardsState);
		this.subscribedStreamsToLastDiscoveredShardIds = checkNotNull(subscribedStreamsToLastDiscoveredShardIds);

		this.shardConsumersExecutor =
			createShardConsumersThreadPool(runtimeContext.getTaskNameWithSubtasks());
	}

	/**
	 * Create a new shard consumer.
	 * Override this method to customize shard consumer behavior in subclasses.
	 * @param subscribedShardStateIndex the state index of the shard this consumer is subscribed to
	 * @param subscribedShard the shard this consumer is subscribed to
	 * @param lastSequenceNum the sequence number in the shard to start consuming
	 * @param shardMetricsReporter the reporter to report metrics to
	 * @return shard consumer
	 */
	protected ShardConsumer createShardConsumer(
		Integer subscribedShardStateIndex,
		StreamShardHandle subscribedShard,
		SequenceNumber lastSequenceNum,
		ShardMetricsReporter shardMetricsReporter) {
		return new ShardConsumer<>(
			this,
			subscribedShardStateIndex,
			subscribedShard,
			lastSequenceNum,
			this.kinesisProxyFactory.create(configProps),
			shardMetricsReporter);
	}

	/**
	 * Starts the fetcher. After starting the fetcher, it can only
	 * be stopped by calling {@link KinesisDataFetcher#shutdownFetcher()}.
	 *
	 * @throws Exception the first error or exception thrown by the fetcher or any of the threads created by the fetcher.
	 */
	public void runFetcher() throws Exception {

		// check that we are running before proceeding
		if (!running) {
			return;
		}

		this.mainThread = Thread.currentThread();

		// ------------------------------------------------------------------------
		//  Procedures before starting the infinite while loop:
		// ------------------------------------------------------------------------

		//  1. check that there is at least one shard in the subscribed streams to consume from (can be done by
		//     checking if at least one value in subscribedStreamsToLastDiscoveredShardIds is not null)
		boolean hasShards = false;
		StringBuilder streamsWithNoShardsFound = new StringBuilder();
		for (Map.Entry<String, String> streamToLastDiscoveredShardEntry : subscribedStreamsToLastDiscoveredShardIds.entrySet()) {
			if (streamToLastDiscoveredShardEntry.getValue() != null) {
				hasShards = true;
			} else {
				streamsWithNoShardsFound.append(streamToLastDiscoveredShardEntry.getKey()).append(", ");
			}
		}

		if (streamsWithNoShardsFound.length() != 0 && LOG.isWarnEnabled()) {
			LOG.warn("Subtask {} has failed to find any shards for the following subscribed streams: {}",
				indexOfThisConsumerSubtask, streamsWithNoShardsFound.toString());
		}

		if (!hasShards) {
			throw new RuntimeException("No shards can be found for all subscribed streams: " + streams);
		}

		//  2. start consuming any shard state we already have in the subscribedShardState up to this point; the
		//     subscribedShardState may already be seeded with values due to step 1., or explicitly added by the
		//     consumer using a restored state checkpoint
		for (int seededStateIndex = 0; seededStateIndex < subscribedShardsState.size(); seededStateIndex++) {
			KinesisStreamShardState seededShardState = subscribedShardsState.get(seededStateIndex);

			// only start a consuming thread if the seeded subscribed shard has not been completely read already
			if (!seededShardState.getLastProcessedSequenceNum().equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get())) {

				if (LOG.isInfoEnabled()) {
					LOG.info("Subtask {} will start consuming seeded shard {} from sequence number {} with ShardConsumer {}",
						indexOfThisConsumerSubtask, seededShardState.getStreamShardHandle().toString(),
						seededShardState.getLastProcessedSequenceNum(), seededStateIndex);
					}

				shardConsumersExecutor.submit(
					createShardConsumer(
						seededStateIndex,
						subscribedShardsState.get(seededStateIndex).getStreamShardHandle(),
						subscribedShardsState.get(seededStateIndex).getLastProcessedSequenceNum(),
						registerShardMetrics(consumerMetricGroup, subscribedShardsState.get(seededStateIndex))));
			}
		}

		// ------------------------------------------------------------------------

		// finally, start the infinite shard discovery and consumer launching loop;
		// we will escape from this loop only when shutdownFetcher() or stopWithError() is called

		final long discoveryIntervalMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_DISCOVERY_INTERVAL_MILLIS)));

		if (this.numberOfActiveShards.get() == 0) {
			LOG.info("Subtask {} has no active shards to read on startup; marking the subtask as temporarily idle ...",
				indexOfThisConsumerSubtask);
			sourceContext.markAsTemporarilyIdle();
		}

		while (running) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} is trying to discover new shards that were created due to resharding ...",
					indexOfThisConsumerSubtask);
			}
			List<StreamShardHandle> newShardsDueToResharding = discoverNewShardsToSubscribe();

			for (StreamShardHandle shard : newShardsDueToResharding) {
				// since there may be delay in discovering a new shard, all new shards due to
				// resharding should be read starting from the earliest record possible
				KinesisStreamShardState newShardState =
					new KinesisStreamShardState(convertToStreamShardMetadata(shard), shard, SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get());
				int newStateIndex = registerNewSubscribedShardState(newShardState);

				if (LOG.isInfoEnabled()) {
					LOG.info("Subtask {} has discovered a new shard {} due to resharding, and will start consuming " +
							"the shard from sequence number {} with ShardConsumer {}",
						indexOfThisConsumerSubtask, newShardState.getStreamShardHandle().toString(),
						newShardState.getLastProcessedSequenceNum(), newStateIndex);
				}

				shardConsumersExecutor.submit(
					createShardConsumer(
						newStateIndex,
						newShardState.getStreamShardHandle(),
						newShardState.getLastProcessedSequenceNum(),
						registerShardMetrics(consumerMetricGroup, newShardState)));
			}

			// we also check if we are running here so that we won't start the discovery sleep
			// interval if the running flag was set to false during the middle of the while loop
			if (running && discoveryIntervalMillis != 0) {
				try {
					Thread.sleep(discoveryIntervalMillis);
				} catch (InterruptedException iex) {
					// the sleep may be interrupted by shutdownFetcher()
				}
			}
		}

		// make sure all resources have been terminated before leaving
		awaitTermination();

		// any error thrown in the shard consumer threads will be thrown to the main thread
		Throwable throwable = this.error.get();
		if (throwable != null) {
			if (throwable instanceof Exception) {
				throw (Exception) throwable;
			} else if (throwable instanceof Error) {
				throw (Error) throwable;
			} else {
				throw new Exception(throwable);
			}
		}
	}

	/**
	 * Creates a snapshot of the current last processed sequence numbers of each subscribed shard.
	 *
	 * @return state snapshot
	 */
	public HashMap<StreamShardMetadata, SequenceNumber> snapshotState() {
		// this method assumes that the checkpoint lock is held
		assert Thread.holdsLock(checkpointLock);

		HashMap<StreamShardMetadata, SequenceNumber> stateSnapshot = new HashMap<>();
		for (KinesisStreamShardState shardWithState : subscribedShardsState) {
			stateSnapshot.put(shardWithState.getStreamShardMetadata(), shardWithState.getLastProcessedSequenceNum());
		}
		return stateSnapshot;
	}

	/**
	 * Starts shutting down the fetcher. Must be called to allow {@link KinesisDataFetcher#runFetcher()} to complete.
	 * Once called, the shutdown procedure will be executed and all shard consuming threads will be interrupted.
	 */
	public void shutdownFetcher() {
		running = false;

		if (mainThread != null) {
			mainThread.interrupt(); // the main thread may be sleeping for the discovery interval
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Shutting down the shard consumer threads of subtask {} ...", indexOfThisConsumerSubtask);
		}
		shardConsumersExecutor.shutdownNow();
	}

	/** After calling {@link KinesisDataFetcher#shutdownFetcher()}, this can be called to await the fetcher shutdown. */
	public void awaitTermination() throws InterruptedException {
		while (!shardConsumersExecutor.isTerminated()) {
			Thread.sleep(50);
		}
	}

	/** Called by created threads to pass on errors. Only the first thrown error is set.
	 * Once set, the shutdown process will be executed and all shard consuming threads will be interrupted. */
	protected void stopWithError(Throwable throwable) {
		if (this.error.compareAndSet(null, throwable)) {
			shutdownFetcher();
		}
	}

	// ------------------------------------------------------------------------
	//  Functions that update the subscribedStreamToLastDiscoveredShardIds state
	// ------------------------------------------------------------------------

	/** Updates the last discovered shard of a subscribed stream; only updates if the update is valid. */
	public void advanceLastDiscoveredShardOfStream(String stream, String shardId) {
		String lastSeenShardIdOfStream = this.subscribedStreamsToLastDiscoveredShardIds.get(stream);

		// the update is valid only if the given shard id is greater
		// than the previous last seen shard id of the stream
		if (lastSeenShardIdOfStream == null) {
			// if not previously set, simply put as the last seen shard id
			this.subscribedStreamsToLastDiscoveredShardIds.put(stream, shardId);
		} else if (StreamShardHandle.compareShardIds(shardId, lastSeenShardIdOfStream) > 0) {
			this.subscribedStreamsToLastDiscoveredShardIds.put(stream, shardId);
		}
	}

	/**
	 * A utility function that does the following:
	 *
	 * <p>1. Find new shards for each stream that we haven't seen before
	 * 2. For each new shard, determine whether this consumer subtask should subscribe to them;
	 * 	  if yes, it is added to the returned list of shards
	 * 3. Update the subscribedStreamsToLastDiscoveredShardIds state so that we won't get shards
	 *    that we have already seen before the next time this function is called
	 */
	public List<StreamShardHandle> discoverNewShardsToSubscribe() throws InterruptedException {

		List<StreamShardHandle> newShardsToSubscribe = new LinkedList<>();

		GetShardListResult shardListResult = kinesis.getShardList(subscribedStreamsToLastDiscoveredShardIds);
		if (shardListResult.hasRetrievedShards()) {
			Set<String> streamsWithNewShards = shardListResult.getStreamsWithRetrievedShards();

			for (String stream : streamsWithNewShards) {
				List<StreamShardHandle> newShardsOfStream = shardListResult.getRetrievedShardListOfStream(stream);
				for (StreamShardHandle newShard : newShardsOfStream) {
					int hashCode = shardAssigner.assign(newShard, totalNumberOfConsumerSubtasks);
					if (isThisSubtaskShouldSubscribeTo(hashCode, totalNumberOfConsumerSubtasks, indexOfThisConsumerSubtask)) {
						newShardsToSubscribe.add(newShard);
					}
				}

				advanceLastDiscoveredShardOfStream(
					stream, shardListResult.getLastSeenShardOfStream(stream).getShard().getShardId());
			}
		}

		return newShardsToSubscribe;
	}

	// ------------------------------------------------------------------------
	//  Functions to get / set information about the consumer
	// ------------------------------------------------------------------------

	protected Properties getConsumerConfiguration() {
		return configProps;
	}

	protected KinesisDeserializationSchema<T> getClonedDeserializationSchema() {
		try {
			return InstantiationUtil.clone(deserializationSchema, runtimeContext.getUserCodeClassLoader());
		} catch (IOException | ClassNotFoundException ex) {
			// this really shouldn't happen; simply wrap it around a runtime exception
			throw new RuntimeException(ex);
		}
	}

	// ------------------------------------------------------------------------
	//  Thread-safe operations for record emitting and shard state updating
	//  that assure atomicity with respect to the checkpoint lock
	// ------------------------------------------------------------------------

	/**
	 * Atomic operation to collect a record and update state to the sequence number of the record.
	 * This method is called by {@link ShardConsumer}s.
	 *
	 * @param record the record to collect
	 * @param recordTimestamp timestamp to attach to the collected record
	 * @param shardStateIndex index of the shard to update in subscribedShardsState;
	 *                        this index should be the returned value from
	 *                        {@link KinesisDataFetcher#registerNewSubscribedShardState(KinesisStreamShardState)}, called
	 *                        when the shard state was registered.
	 * @param lastSequenceNumber the last sequence number value to update
	 */
	protected void emitRecordAndUpdateState(T record, long recordTimestamp, int shardStateIndex, SequenceNumber lastSequenceNumber) {
		synchronized (checkpointLock) {
			if (record != null) {
				sourceContext.collectWithTimestamp(record, recordTimestamp);
			} else {
				LOG.warn("Skipping non-deserializable record at sequence number {} of shard {}.",
					lastSequenceNumber,
					subscribedShardsState.get(shardStateIndex).getStreamShardHandle());
			}

			updateState(shardStateIndex, lastSequenceNumber);
		}
	}

	/**
	 * Update the shard to last processed sequence number state.
	 * This method is called by {@link ShardConsumer}s.
	 *
	 * @param shardStateIndex index of the shard to update in subscribedShardsState;
	 *                        this index should be the returned value from
	 *                        {@link KinesisDataFetcher#registerNewSubscribedShardState(KinesisStreamShardState)}, called
	 *                        when the shard state was registered.
	 * @param lastSequenceNumber the last sequence number value to update
	 */
	protected final void updateState(int shardStateIndex, SequenceNumber lastSequenceNumber) {
		synchronized (checkpointLock) {
			subscribedShardsState.get(shardStateIndex).setLastProcessedSequenceNum(lastSequenceNumber);

			// if a shard's state is updated to be SENTINEL_SHARD_ENDING_SEQUENCE_NUM by its consumer thread,
			// we've finished reading the shard and should determine it to be non-active
			if (lastSequenceNumber.equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get())) {
				LOG.info("Subtask {} has reached the end of subscribed shard: {}",
					indexOfThisConsumerSubtask, subscribedShardsState.get(shardStateIndex).getStreamShardHandle());

				// check if we need to mark the source as idle;
				// note that on resharding, if registerNewSubscribedShardState was invoked for newly discovered shards
				// AFTER the old shards had reached the end, the subtask's status will be automatically toggled back to
				// be active immediately afterwards as soon as we collect records from the new shards
				if (this.numberOfActiveShards.decrementAndGet() == 0) {
					LOG.info("Subtask {} has reached the end of all currently subscribed shards; marking the subtask as temporarily idle ...",
						indexOfThisConsumerSubtask);

					sourceContext.markAsTemporarilyIdle();
				}
			}
		}
	}

	/**
	 * Register a new subscribed shard state.
	 *
	 * @param newSubscribedShardState the new shard state that this fetcher is to be subscribed to
	 */
	public int registerNewSubscribedShardState(KinesisStreamShardState newSubscribedShardState) {
		synchronized (checkpointLock) {
			subscribedShardsState.add(newSubscribedShardState);

			// If a registered shard has initial state that is not SENTINEL_SHARD_ENDING_SEQUENCE_NUM (will be the case
			// if the consumer had already finished reading a shard before we failed and restored), we determine that
			// this subtask has a new active shard
			if (!newSubscribedShardState.getLastProcessedSequenceNum().equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get())) {
				this.numberOfActiveShards.incrementAndGet();
			}

			return subscribedShardsState.size() - 1;
		}
	}

	/**
	 * Registers a metric group associated with the shard id of the provided {@link KinesisStreamShardState shardState}.
	 *
	 * @return a {@link ShardMetricsReporter} that can be used to update metric values
	 */
	private static ShardMetricsReporter registerShardMetrics(MetricGroup metricGroup, KinesisStreamShardState shardState) {
		ShardMetricsReporter shardMetrics = new ShardMetricsReporter();

		MetricGroup streamShardMetricGroup = metricGroup
			.addGroup(
				KinesisConsumerMetricConstants.STREAM_METRICS_GROUP,
				shardState.getStreamShardHandle().getStreamName())
			.addGroup(
				KinesisConsumerMetricConstants.SHARD_METRICS_GROUP,
				shardState.getStreamShardHandle().getShard().getShardId());

		streamShardMetricGroup.gauge(KinesisConsumerMetricConstants.MILLIS_BEHIND_LATEST_GAUGE, shardMetrics::getMillisBehindLatest);
		streamShardMetricGroup.gauge(KinesisConsumerMetricConstants.MAX_RECORDS_PER_FETCH, shardMetrics::getMaxNumberOfRecordsPerFetch);
		streamShardMetricGroup.gauge(KinesisConsumerMetricConstants.NUM_AGGREGATED_RECORDS_PER_FETCH, shardMetrics::getNumberOfAggregatedRecords);
		streamShardMetricGroup.gauge(KinesisConsumerMetricConstants.NUM_DEAGGREGATED_RECORDS_PER_FETCH, shardMetrics::getNumberOfDeaggregatedRecords);
		streamShardMetricGroup.gauge(KinesisConsumerMetricConstants.AVG_RECORD_SIZE_BYTES, shardMetrics::getAverageRecordSizeBytes);
		streamShardMetricGroup.gauge(KinesisConsumerMetricConstants.BYTES_PER_READ, shardMetrics::getBytesPerRead);
		streamShardMetricGroup.gauge(KinesisConsumerMetricConstants.RUNTIME_LOOP_NANOS, shardMetrics::getRunLoopTimeNanos);
		streamShardMetricGroup.gauge(KinesisConsumerMetricConstants.LOOP_FREQUENCY_HZ, shardMetrics::getLoopFrequencyHz);
		streamShardMetricGroup.gauge(KinesisConsumerMetricConstants.SLEEP_TIME_MILLIS, shardMetrics::getSleepTimeMillis);
		return shardMetrics;
	}

	// ------------------------------------------------------------------------
	//  Miscellaneous utility functions
	// ------------------------------------------------------------------------

	/**
	 * Utility function to determine whether a shard should be subscribed by this consumer subtask.
	 *
	 * @param shardHash hash code for the shard
	 * @param totalNumberOfConsumerSubtasks total number of consumer subtasks
	 * @param indexOfThisConsumerSubtask index of this consumer subtask
	 */
	public static boolean isThisSubtaskShouldSubscribeTo(int shardHash,
														int totalNumberOfConsumerSubtasks,
														int indexOfThisConsumerSubtask) {
		return (Math.abs(shardHash % totalNumberOfConsumerSubtasks)) == indexOfThisConsumerSubtask;
	}

	@VisibleForTesting
	protected ExecutorService createShardConsumersThreadPool(final String subtaskName) {
		return Executors.newCachedThreadPool(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable runnable) {
				final AtomicLong threadCount = new AtomicLong(0);
				Thread thread = new Thread(runnable);
				thread.setName("shardConsumers-" + subtaskName + "-thread-" + threadCount.getAndIncrement());
				thread.setDaemon(true);
				return thread;
			}
		});
	}

	@VisibleForTesting
	public List<KinesisStreamShardState> getSubscribedShardsState() {
		return subscribedShardsState;
	}

	/**
	 * Utility function to create an initial map of the last discovered shard id of each subscribed stream, set to null;
	 * This is called in the constructor; correct values will be set later on by calling advanceLastDiscoveredShardOfStream().
	 *
	 * @param streams the list of subscribed streams
	 * @return the initial map for subscribedStreamsToLastDiscoveredShardIds
	 */
	protected static HashMap<String, String> createInitialSubscribedStreamsToLastDiscoveredShardsState(List<String> streams) {
		HashMap<String, String> initial = new HashMap<>();
		for (String stream : streams) {
			initial.put(stream, null);
		}
		return initial;
	}

	/**
	 * Utility function to convert {@link StreamShardHandle} into {@link StreamShardMetadata}.
	 *
	 * @param streamShardHandle the {@link StreamShardHandle} to be converted
	 * @return a {@link StreamShardMetadata} object
	 */
	public static StreamShardMetadata convertToStreamShardMetadata(StreamShardHandle streamShardHandle) {
		StreamShardMetadata streamShardMetadata = new StreamShardMetadata();

		streamShardMetadata.setStreamName(streamShardHandle.getStreamName());
		streamShardMetadata.setShardId(streamShardHandle.getShard().getShardId());
		streamShardMetadata.setParentShardId(streamShardHandle.getShard().getParentShardId());
		streamShardMetadata.setAdjacentParentShardId(streamShardHandle.getShard().getAdjacentParentShardId());

		if (streamShardHandle.getShard().getHashKeyRange() != null) {
			streamShardMetadata.setStartingHashKey(streamShardHandle.getShard().getHashKeyRange().getStartingHashKey());
			streamShardMetadata.setEndingHashKey(streamShardHandle.getShard().getHashKeyRange().getEndingHashKey());
		}

		if (streamShardHandle.getShard().getSequenceNumberRange() != null) {
			streamShardMetadata.setStartingSequenceNumber(streamShardHandle.getShard().getSequenceNumberRange().getStartingSequenceNumber());
			streamShardMetadata.setEndingSequenceNumber(streamShardHandle.getShard().getSequenceNumberRange().getEndingSequenceNumber());
		}

		return streamShardMetadata;
	}

	/**
	 * Utility function to convert {@link StreamShardMetadata} into {@link StreamShardHandle}.
	 *
	 * @param streamShardMetadata the {@link StreamShardMetadata} to be converted
	 * @return a {@link StreamShardHandle} object
	 */
	public static StreamShardHandle convertToStreamShardHandle(StreamShardMetadata streamShardMetadata) {
		Shard shard = new Shard();
		shard.withShardId(streamShardMetadata.getShardId());
		shard.withParentShardId(streamShardMetadata.getParentShardId());
		shard.withAdjacentParentShardId(streamShardMetadata.getAdjacentParentShardId());

		HashKeyRange hashKeyRange = new HashKeyRange();
		hashKeyRange.withStartingHashKey(streamShardMetadata.getStartingHashKey());
		hashKeyRange.withEndingHashKey(streamShardMetadata.getEndingHashKey());
		shard.withHashKeyRange(hashKeyRange);

		SequenceNumberRange sequenceNumberRange = new SequenceNumberRange();
		sequenceNumberRange.withStartingSequenceNumber(streamShardMetadata.getStartingSequenceNumber());
		sequenceNumberRange.withEndingSequenceNumber(streamShardMetadata.getEndingSequenceNumber());
		shard.withSequenceNumberRange(sequenceNumberRange);

		return new StreamShardHandle(streamShardMetadata.getStreamName(), shard);
	}
}
