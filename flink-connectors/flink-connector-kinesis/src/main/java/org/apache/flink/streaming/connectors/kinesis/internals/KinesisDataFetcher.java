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
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
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
import org.apache.flink.streaming.connectors.kinesis.util.RecordEmitter;
import org.apache.flink.streaming.connectors.kinesis.util.WatermarkTracker;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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

	private final AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner;
	private final WatermarkTracker watermarkTracker;
	private final transient RecordEmitter recordEmitter;
	private transient boolean isIdle;

	/**
	 * The watermark related state for each shard consumer. Entries in this map will be created when shards
	 * are discovered. After recovery, this shard map will be recreated, possibly with different shard index keys,
	 * since those are transient and not part of checkpointed state.
	 */
	private ConcurrentHashMap<Integer, ShardWatermarkState> shardWatermarks = new ConcurrentHashMap<>();

	/**
	 * The most recent watermark, calculated from the per shard watermarks. The initial value will never be emitted and
	 * also apply after recovery. The fist watermark that will be emitted is derived from actually consumed records.
	 * In case of recovery and replay, the watermark will rewind, consistent wth the shard consumer sequence.
	 */
	private long lastWatermark = Long.MIN_VALUE;

	/**
	 * The next watermark used for synchronization.
	 * For purposes of global watermark calculation, we need to consider the next watermark based
	 * on the buffered records vs. the last emitted watermark to allow for progress.
	 */
	private long nextWatermark = Long.MIN_VALUE;


	/**
	 * The time span since last consumed record, after which a shard will be considered idle for purpose of watermark
	 * calculation. A positive value will allow the watermark to progress even when some shards don't receive new records.
	 */
	private long shardIdleIntervalMillis = ConsumerConfigConstants.DEFAULT_SHARD_IDLE_INTERVAL_MILLIS;

	/**
	 * Factory to create Kinesis proxy instances used by a fetcher.
	 */
	public interface FlinkKinesisProxyFactory {
		KinesisProxyInterface create(Properties configProps);
	}

	/**
	 * The wrapper that holds the watermark handling related parameters
	 * of a record produced by the shard consumer thread.
	 *
	 * @param <T>
	 */
	private static class RecordWrapper<T> extends TimestampedValue<T> {
		int shardStateIndex;
		SequenceNumber lastSequenceNumber;
		long timestamp;
		Watermark watermark;

		private RecordWrapper(T record, long timestamp) {
			super(record, timestamp);
			this.timestamp = timestamp;
		}

		@Override
		public long getTimestamp() {
			return timestamp;
		}
	}

	/** Kinesis data fetcher specific, asynchronous record emitter. */
	private class AsyncKinesisRecordEmitter extends RecordEmitter<RecordWrapper<T>> {

		private AsyncKinesisRecordEmitter() {
			this(DEFAULT_QUEUE_CAPACITY);
		}

		private AsyncKinesisRecordEmitter(int queueCapacity) {
			super(queueCapacity);
		}

		@Override
		public void emit(RecordWrapper<T> record, RecordQueue<RecordWrapper<T>> queue) {
			emitRecordAndUpdateState(record);
		}
	}

	/** Synchronous emitter for use w/o watermark synchronization. */
	private class SyncKinesisRecordEmitter extends AsyncKinesisRecordEmitter {
		private final ConcurrentHashMap<Integer, RecordQueue<RecordWrapper<T>>> queues =
			new ConcurrentHashMap<>();

		@Override
		public RecordQueue<RecordWrapper<T>> getQueue(int producerIndex) {
			return queues.computeIfAbsent(producerIndex, (key) -> {
				return new RecordQueue<RecordWrapper<T>>() {
					@Override
					public void put(RecordWrapper<T> record) {
						emit(record, this);
					}

					@Override
					public int getSize() {
						return 0;
					}

					@Override
					public RecordWrapper<T> peek() {
						return null;
					}

				};
			});
		}
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
							KinesisShardAssigner shardAssigner,
							AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner,
							WatermarkTracker watermarkTracker) {
		this(streams,
			sourceContext,
			sourceContext.getCheckpointLock(),
			runtimeContext,
			configProps,
			deserializationSchema,
			shardAssigner,
			periodicWatermarkAssigner,
			watermarkTracker,
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
								AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner,
								WatermarkTracker watermarkTracker,
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
		this.periodicWatermarkAssigner = periodicWatermarkAssigner;
		this.watermarkTracker = watermarkTracker;
		this.kinesisProxyFactory = checkNotNull(kinesisProxyFactory);
		this.kinesis = kinesisProxyFactory.create(configProps);

		this.consumerMetricGroup = runtimeContext.getMetricGroup()
			.addGroup(KinesisConsumerMetricConstants.KINESIS_CONSUMER_METRICS_GROUP);

		this.error = checkNotNull(error);
		this.subscribedShardsState = checkNotNull(subscribedShardsState);
		this.subscribedStreamsToLastDiscoveredShardIds = checkNotNull(subscribedStreamsToLastDiscoveredShardIds);

		this.shardConsumersExecutor =
			createShardConsumersThreadPool(runtimeContext.getTaskNameWithSubtasks());
		this.recordEmitter = createRecordEmitter(configProps);
	}

	private RecordEmitter createRecordEmitter(Properties configProps) {
		if (periodicWatermarkAssigner != null && watermarkTracker != null) {
			int queueCapacity = Integer.parseInt(configProps.getProperty(
				ConsumerConfigConstants.WATERMARK_SYNC_QUEUE_CAPACITY,
				Integer.toString(AsyncKinesisRecordEmitter.DEFAULT_QUEUE_CAPACITY)));
			return new AsyncKinesisRecordEmitter(queueCapacity);
		}
		return new SyncKinesisRecordEmitter();
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

        // start periodic watermark emitter, if a watermark assigner was configured
		if (periodicWatermarkAssigner != null) {
			long periodicWatermarkIntervalMillis = runtimeContext.getExecutionConfig().getAutoWatermarkInterval();
			if (periodicWatermarkIntervalMillis > 0) {
				ProcessingTimeService timerService = ((StreamingRuntimeContext) runtimeContext).getProcessingTimeService();
				LOG.info("Starting periodic watermark emitter with interval {}", periodicWatermarkIntervalMillis);
				new PeriodicWatermarkEmitter(timerService, periodicWatermarkIntervalMillis).start();
				if (watermarkTracker != null) {
					// setup global watermark tracking
					long watermarkSyncMillis = Long.parseLong(
						getConsumerConfiguration().getProperty(ConsumerConfigConstants.WATERMARK_SYNC_MILLIS,
							Long.toString(ConsumerConfigConstants.DEFAULT_WATERMARK_SYNC_MILLIS)));
					watermarkTracker.setUpdateTimeoutMillis(watermarkSyncMillis * 3); // synchronization latency
					watermarkTracker.open(runtimeContext);
					new WatermarkSyncCallback(timerService, watermarkSyncMillis).start();
					// emit records ahead of watermark to offset synchronization latency
					long lookaheadMillis = Long.parseLong(
						getConsumerConfiguration().getProperty(ConsumerConfigConstants.WATERMARK_LOOKAHEAD_MILLIS,
							Long.toString(0)));
					recordEmitter.setMaxLookaheadMillis(Math.max(lookaheadMillis, watermarkSyncMillis * 3));
				}
			}
			this.shardIdleIntervalMillis = Long.parseLong(
				getConsumerConfiguration().getProperty(ConsumerConfigConstants.SHARD_IDLE_INTERVAL_MILLIS,
					Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_IDLE_INTERVAL_MILLIS)));

			// run record emitter in separate thread since main thread is used for discovery
			Thread thread = new Thread(this.recordEmitter);
			thread.setName("recordEmitter-" + runtimeContext.getTaskNameWithSubtasks());
			thread.setDaemon(true);
			thread.start();
		}

		// ------------------------------------------------------------------------

		// finally, start the infinite shard discovery and consumer launching loop;
		// we will escape from this loop only when shutdownFetcher() or stopWithError() is called
		// TODO: have this thread emit the records for tracking backpressure

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
		try {
			awaitTermination();
		} catch (InterruptedException ie) {
			// If there is an original exception, preserve it, since that's more important/useful.
			this.error.compareAndSet(null, ie);
		}

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

		if (watermarkTracker != null) {
			watermarkTracker.close();
		}
		this.recordEmitter.stop();

		if (LOG.isInfoEnabled()) {
			LOG.info("Shutting down the shard consumer threads of subtask {} ...", indexOfThisConsumerSubtask);
		}
		shardConsumersExecutor.shutdownNow();
	}

	/** After calling {@link KinesisDataFetcher#shutdownFetcher()}, this can be called to await the fetcher shutdown. */
	public void awaitTermination() throws InterruptedException {
		while (!shardConsumersExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
			// Keep waiting.
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
		} else if (shouldAdvanceLastDiscoveredShardId(shardId, lastSeenShardIdOfStream)) {
			this.subscribedStreamsToLastDiscoveredShardIds.put(stream, shardId);
		}
	}

	/** Given lastSeenShardId, check if last discovered shardId should be advanced. */
	protected boolean shouldAdvanceLastDiscoveredShardId(String shardId, String lastSeenShardIdOfStream) {
		return (StreamShardHandle.compareShardIds(shardId, lastSeenShardIdOfStream) > 0);
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
	 * Prepare a record and hand it over to the {@link RecordEmitter}, which may collect it asynchronously.
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
		ShardWatermarkState sws = shardWatermarks.get(shardStateIndex);
		Preconditions.checkNotNull(
			sws, "shard watermark state initialized in registerNewSubscribedShardState");
		Watermark watermark = null;
		if (sws.periodicWatermarkAssigner != null) {
			recordTimestamp =
				sws.periodicWatermarkAssigner.extractTimestamp(record, sws.lastRecordTimestamp);
			// track watermark per record since extractTimestamp has side effect
			watermark = sws.periodicWatermarkAssigner.getCurrentWatermark();
		}
		sws.lastRecordTimestamp = recordTimestamp;
		sws.lastUpdated = getCurrentTimeMillis();

		RecordWrapper<T> recordWrapper = new RecordWrapper<>(record, recordTimestamp);
		recordWrapper.shardStateIndex = shardStateIndex;
		recordWrapper.lastSequenceNumber = lastSequenceNumber;
		recordWrapper.watermark = watermark;
		try {
			sws.emitQueue.put(recordWrapper);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Atomic operation to collect a record and update state to the sequence number of the record.
	 * This method is called from the record emitter.
	 *
	 * <p>Responsible for tracking per shard watermarks and emit timestamps extracted from
	 * the record, when a watermark assigner was configured.
	 *
	 * @param rw
	 */
	private void emitRecordAndUpdateState(RecordWrapper<T> rw) {
		synchronized (checkpointLock) {
			if (rw.getValue() != null) {
				sourceContext.collectWithTimestamp(rw.getValue(), rw.timestamp);
				ShardWatermarkState<T> sws = shardWatermarks.get(rw.shardStateIndex);
				sws.lastEmittedRecordWatermark = rw.watermark;
			} else {
				LOG.warn("Skipping non-deserializable record at sequence number {} of shard {}.",
					rw.lastSequenceNumber,
					subscribedShardsState.get(rw.shardStateIndex).getStreamShardHandle());
			}
			updateState(rw.shardStateIndex, rw.lastSequenceNumber);
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

			int shardStateIndex = subscribedShardsState.size() - 1;

			// track all discovered shards for watermark determination
			ShardWatermarkState sws = shardWatermarks.get(shardStateIndex);
			if (sws == null) {
				sws = new ShardWatermarkState();
				try {
					sws.periodicWatermarkAssigner = InstantiationUtil.clone(periodicWatermarkAssigner);
				} catch (Exception e) {
					throw new RuntimeException("Failed to instantiate new WatermarkAssigner", e);
				}
				sws.emitQueue = recordEmitter.getQueue(shardStateIndex);
				sws.lastUpdated = getCurrentTimeMillis();
				sws.lastRecordTimestamp = Long.MIN_VALUE;
				shardWatermarks.put(shardStateIndex, sws);
			}

			return shardStateIndex;
		}
	}

	/**
	 * Return the current system time. Allow tests to override this to simulate progress for watermark
	 * logic.
	 *
	 * @return current processing time
	 */
	@VisibleForTesting
	protected long getCurrentTimeMillis() {
		return System.currentTimeMillis();
	}

	/**
	 * Called periodically to emit a watermark. Checks all shards for the current event time
	 * watermark, and possibly emits the next watermark.
	 *
	 * <p>Shards that have not received an update for a certain interval are considered inactive so as
	 * to not hold back the watermark indefinitely. When all shards are inactive, the subtask will be
	 * marked as temporarily idle to not block downstream operators.
	 */
	@VisibleForTesting
	protected void emitWatermark() {
		LOG.debug("Evaluating watermark for subtask {} time {}", indexOfThisConsumerSubtask, getCurrentTimeMillis());
		long potentialWatermark = Long.MAX_VALUE;
		long potentialNextWatermark = Long.MAX_VALUE;
		long idleTime =
			(shardIdleIntervalMillis > 0)
				? getCurrentTimeMillis() - shardIdleIntervalMillis
				: Long.MAX_VALUE;

		for (Map.Entry<Integer, ShardWatermarkState> e : shardWatermarks.entrySet()) {
			Watermark w = e.getValue().lastEmittedRecordWatermark;
			// consider only active shards, or those that would advance the watermark
			if (w != null && (e.getValue().lastUpdated >= idleTime
				|| e.getValue().emitQueue.getSize() > 0
				|| w.getTimestamp() > lastWatermark)) {
				potentialWatermark = Math.min(potentialWatermark, w.getTimestamp());
				// for sync, use the watermark of the next record, when available
				// otherwise watermark may stall when record is blocked by synchronization
				RecordEmitter.RecordQueue<RecordWrapper<T>> q = e.getValue().emitQueue;
				RecordWrapper<T> nextRecord = q.peek();
				Watermark nextWatermark = (nextRecord != null) ? nextRecord.watermark : w;
				potentialNextWatermark = Math.min(potentialNextWatermark, nextWatermark.getTimestamp());
			}
		}

		// advance watermark if possible (watermarks can only be ascending)
		if (potentialWatermark == Long.MAX_VALUE) {
			if (shardWatermarks.isEmpty() || shardIdleIntervalMillis > 0) {
				LOG.info("No active shard for subtask {}, marking the source idle.",
					indexOfThisConsumerSubtask);
				// no active shard, signal downstream operators to not wait for a watermark
				sourceContext.markAsTemporarilyIdle();
				isIdle = true;
			}
		} else {
			if (potentialWatermark > lastWatermark) {
				LOG.debug("Emitting watermark {} from subtask {}",
					potentialWatermark,
					indexOfThisConsumerSubtask);
				sourceContext.emitWatermark(new Watermark(potentialWatermark));
				lastWatermark = potentialWatermark;
				isIdle = false;
			}
			nextWatermark = potentialNextWatermark;
		}
	}

	/** Per shard tracking of watermark and last activity. */
	private static class ShardWatermarkState<T> {
		private AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner;
		private RecordEmitter.RecordQueue<RecordWrapper<T>> emitQueue;
		private volatile long lastRecordTimestamp;
		private volatile long lastUpdated;
		private volatile Watermark lastEmittedRecordWatermark;
	}

	/**
	 * The periodic watermark emitter. In its given interval, it checks all shards for the current
	 * event time watermark, and possibly emits the next watermark.
	 */
	private class PeriodicWatermarkEmitter implements ProcessingTimeCallback {

		private final ProcessingTimeService timerService;
		private final long interval;

		PeriodicWatermarkEmitter(ProcessingTimeService timerService, long autoWatermarkInterval) {
			this.timerService = checkNotNull(timerService);
			this.interval = autoWatermarkInterval;
		}

		public void start() {
			LOG.debug("registering periodic watermark timer with interval {}", interval);
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}

		@Override
		public void onProcessingTime(long timestamp) {
			emitWatermark();
			// schedule the next watermark
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}
	}

	/** Timer task to update shared watermark state. */
	private class WatermarkSyncCallback implements ProcessingTimeCallback {

		private final ProcessingTimeService timerService;
		private final long interval;
		private final MetricGroup shardMetricsGroup;
		private long lastGlobalWatermark = Long.MIN_VALUE;
		private long propagatedLocalWatermark = Long.MIN_VALUE;
		private long logIntervalMillis = 60_000;
		private int stalledWatermarkIntervalCount = 0;
		private long lastLogged;

		WatermarkSyncCallback(ProcessingTimeService timerService, long interval) {
			this.timerService = checkNotNull(timerService);
			this.interval = interval;
			this.shardMetricsGroup = consumerMetricGroup.addGroup("subtaskId",
				String.valueOf(indexOfThisConsumerSubtask));
			this.shardMetricsGroup.gauge("localWatermark", () -> nextWatermark);
			this.shardMetricsGroup.gauge("globalWatermark", () -> lastGlobalWatermark);
		}

		public void start() {
			LOG.info("Registering watermark tracker with interval {}", interval);
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}

		@Override
		public void onProcessingTime(long timestamp) {
			if (nextWatermark != Long.MIN_VALUE) {
				long globalWatermark = lastGlobalWatermark;
				// TODO: refresh watermark while idle
				if (!(isIdle && nextWatermark == propagatedLocalWatermark)) {
					globalWatermark = watermarkTracker.updateWatermark(nextWatermark);
					propagatedLocalWatermark = nextWatermark;
				} else {
					LOG.info("WatermarkSyncCallback subtask: {} is idle", indexOfThisConsumerSubtask);
				}

				if (timestamp - lastLogged > logIntervalMillis) {
					lastLogged = System.currentTimeMillis();
					LOG.info("WatermarkSyncCallback subtask: {} local watermark: {}"
							+ ", global watermark: {}, delta: {} timeouts: {}, emitter: {}",
						indexOfThisConsumerSubtask,
						nextWatermark,
						globalWatermark,
						nextWatermark - globalWatermark,
						watermarkTracker.getUpdateTimeoutCount(),
						recordEmitter.printInfo());

					// Following is for debugging non-reproducible issue with stalled watermark
					if (globalWatermark == nextWatermark && globalWatermark == lastGlobalWatermark
						&& stalledWatermarkIntervalCount++ > 5) {
						// subtask blocks watermark, log to aid troubleshooting
						stalledWatermarkIntervalCount = 0;
						for (Map.Entry<Integer, ShardWatermarkState> e : shardWatermarks.entrySet()) {
							RecordEmitter.RecordQueue<RecordWrapper<T>> q = e.getValue().emitQueue;
							RecordWrapper<T> nextRecord = q.peek();
							if (nextRecord != null) {
								LOG.info("stalled watermark {} key {} next watermark {} next timestamp {}",
									nextWatermark,
									e.getKey(),
									nextRecord.watermark,
									nextRecord.timestamp);
							}
						}
					}
				}

				lastGlobalWatermark = globalWatermark;
				recordEmitter.setCurrentWatermark(globalWatermark);
			}
			// schedule next callback
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
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
