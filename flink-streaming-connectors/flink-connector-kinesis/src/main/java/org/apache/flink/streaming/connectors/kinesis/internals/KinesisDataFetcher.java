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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
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
public class KinesisDataFetcher<T> {

	private static final Logger LOG = LoggerFactory.getLogger(KinesisDataFetcher.class);

	// ------------------------------------------------------------------------
	//  Consumer-wide settings
	// ------------------------------------------------------------------------

	/** Configuration properties for the Flink Kinesis Consumer */
	private final Properties configProps;

	/** The list of Kinesis streams that the consumer is subscribing to */
	private final List<String> streams;

	/**
	 * The deserialization schema we will be using to convert Kinesis records to Flink objects.
	 * Note that since this might not be thread-safe, {@link ShardConsumer}s using this must
	 * clone a copy using {@link KinesisDataFetcher#getClonedDeserializationSchema()}.
	 */
	private final KinesisDeserializationSchema<T> deserializationSchema;

	// ------------------------------------------------------------------------
	//  Subtask-specific settings
	// ------------------------------------------------------------------------

	/** Runtime context of the subtask that this fetcher was created in */
	private final RuntimeContext runtimeContext;

	private final int totalNumberOfConsumerSubtasks;

	private final int indexOfThisConsumerSubtask;

	/**
	 * This flag should be set by {@link FlinkKinesisConsumer} using
	 * {@link KinesisDataFetcher#setIsRestoringFromFailure(boolean)}
	 */
	private boolean isRestoredFromFailure;

	// ------------------------------------------------------------------------
	//  Executor services to run created threads
	// ------------------------------------------------------------------------

	/** Executor service to run {@link ShardConsumer}s to consume Kinesis shards */
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

	/** Checkpoint lock, also used to synchronize operations on subscribedShardsState */
	private final Object checkpointLock;

	/** Reference to the first error thrown by any of the {@link ShardConsumer} threads */
	private final AtomicReference<Throwable> error;

	/** The Kinesis proxy that the fetcher will be using to discover new shards */
	private final KinesisProxyInterface kinesis;

	/** Thread that executed runFetcher() */
	private Thread mainThread;

	/**
	 * The current number of shards that are actively read by this fetcher.
	 *
	 * This value is updated in {@link KinesisDataFetcher#registerNewSubscribedShardState(KinesisStreamShardState)},
	 * and {@link KinesisDataFetcher#updateState(int, SequenceNumber)}.
	 */
	private final AtomicInteger numberOfActiveShards = new AtomicInteger(0);

	private volatile boolean running = true;

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
							KinesisDeserializationSchema<T> deserializationSchema) {
		this(streams,
			sourceContext,
			sourceContext.getCheckpointLock(),
			runtimeContext,
			configProps,
			deserializationSchema,
			new AtomicReference<Throwable>(),
			new LinkedList<KinesisStreamShardState>(),
			createInitialSubscribedStreamsToLastDiscoveredShardsState(streams),
			KinesisProxy.create(configProps));
	}

	/** This constructor is exposed for testing purposes */
	protected KinesisDataFetcher(List<String> streams,
								SourceFunction.SourceContext<T> sourceContext,
								Object checkpointLock,
								RuntimeContext runtimeContext,
								Properties configProps,
								KinesisDeserializationSchema<T> deserializationSchema,
								AtomicReference<Throwable> error,
								LinkedList<KinesisStreamShardState> subscribedShardsState,
								HashMap<String, String> subscribedStreamsToLastDiscoveredShardIds,
								KinesisProxyInterface kinesis) {
		this.streams = checkNotNull(streams);
		this.configProps = checkNotNull(configProps);
		this.sourceContext = checkNotNull(sourceContext);
		this.checkpointLock = checkNotNull(checkpointLock);
		this.runtimeContext = checkNotNull(runtimeContext);
		this.totalNumberOfConsumerSubtasks = runtimeContext.getNumberOfParallelSubtasks();
		this.indexOfThisConsumerSubtask = runtimeContext.getIndexOfThisSubtask();
		this.deserializationSchema = checkNotNull(deserializationSchema);
		this.kinesis = checkNotNull(kinesis);

		this.error = checkNotNull(error);
		this.subscribedShardsState = checkNotNull(subscribedShardsState);
		this.subscribedStreamsToLastDiscoveredShardIds = checkNotNull(subscribedStreamsToLastDiscoveredShardIds);

		this.shardConsumersExecutor =
			createShardConsumersThreadPool(runtimeContext.getTaskNameWithSubtasks());
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

		//  1. query for any new shards that may have been created while the Kinesis consumer was not running,
		//     and register them to the subscribedShardState list.
		if (LOG.isDebugEnabled()) {
			String logFormat = (!isRestoredFromFailure)
				? "Subtask {} is trying to discover initial shards ..."
				: "Subtask {} is trying to discover any new shards that were created while the consumer wasn't " +
				"running due to failure ...";

			LOG.debug(logFormat, indexOfThisConsumerSubtask);
		}
		List<KinesisStreamShard> newShardsCreatedWhileNotRunning = discoverNewShardsToSubscribe();
		for (KinesisStreamShard shard : newShardsCreatedWhileNotRunning) {
			// the starting state for new shards created while the consumer wasn't running depends on whether or not
			// we are starting fresh (not restoring from a checkpoint); when we are starting fresh, this simply means
			// all existing shards of streams we are subscribing to are new shards; when we are restoring from checkpoint,
			// any new shards due to Kinesis resharding from the time of the checkpoint will be considered new shards.
			InitialPosition initialPosition = InitialPosition.valueOf(configProps.getProperty(
				ConsumerConfigConstants.STREAM_INITIAL_POSITION, ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION));

			SentinelSequenceNumber startingStateForNewShard = (isRestoredFromFailure)
				? SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM
				: initialPosition.toSentinelSequenceNumber();

			if (LOG.isInfoEnabled()) {
				String logFormat = (!isRestoredFromFailure)
					? "Subtask {} will be seeded with initial shard {}, starting state set as sequence number {}"
					: "Subtask {} will be seeded with new shard {} that was created while the consumer wasn't " +
					"running due to failure, starting state set as sequence number {}";

				LOG.info(logFormat, indexOfThisConsumerSubtask, shard.toString(), startingStateForNewShard.get());
			}
			registerNewSubscribedShardState(new KinesisStreamShardState(shard, startingStateForNewShard.get()));
		}

		//  2. check that there is at least one shard in the subscribed streams to consume from (can be done by
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

		//  3. start consuming any shard state we already have in the subscribedShardState up to this point; the
		//     subscribedShardState may already be seeded with values due to step 1., or explicitly added by the
		//     consumer using a restored state checkpoint
		for (int seededStateIndex = 0; seededStateIndex < subscribedShardsState.size(); seededStateIndex++) {
			KinesisStreamShardState seededShardState = subscribedShardsState.get(seededStateIndex);

			// only start a consuming thread if the seeded subscribed shard has not been completely read already
			if (!seededShardState.getLastProcessedSequenceNum().equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get())) {

				if (LOG.isInfoEnabled()) {
					LOG.info("Subtask {} will start consuming seeded shard {} from sequence number {} with ShardConsumer {}",
						indexOfThisConsumerSubtask, seededShardState.getKinesisStreamShard().toString(),
						seededShardState.getLastProcessedSequenceNum(), seededStateIndex);
					}

				shardConsumersExecutor.submit(
					new ShardConsumer<>(
						this,
						seededStateIndex,
						subscribedShardsState.get(seededStateIndex).getKinesisStreamShard(),
						subscribedShardsState.get(seededStateIndex).getLastProcessedSequenceNum()));
			}
		}

		// ------------------------------------------------------------------------

		// finally, start the infinite shard discovery and consumer launching loop;
		// we will escape from this loop only when shutdownFetcher() or stopWithError() is called

		final long discoveryIntervalMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_DISCOVERY_INTERVAL_MILLIS)));

		// FLINK-4341:
		// For downstream operators that work on time (ex. window operators), we are required to emit a max value watermark
		// for subtasks that won't continue to have shards to read from unless resharding happens in the future, otherwise
		// the downstream watermarks would not advance, leading to unbounded accumulating state.
		//
		// The side-effect of this limitation is that on resharding, we must fail hard if the newly discovered shard
		// is to be subscribed by a subtask that has previously emitted a max value watermark, otherwise the watermarks
		// will be messed up.
		//
		// There are 2 cases were we need to either emit a max value watermark, or deliberately fail hard:
		//  (a) if this subtask has no more shards to read from unless resharding happens in the future, we emit a max
		//      value watermark. This case is encountered when 1) all previously read shards by this subtask were closed
		//      due to resharding, 2) when this subtask was initially only subscribed to closed shards while the consumer
		//      was told to start from TRIM_HORIZON, or 3) there was initially no shards for this subtask to read on startup.
		//  (b) this subtask has discovered new shards to read from due to a reshard; if this subtask has already emitted
		//      a max value watermark, we must deliberately fail hard to avoid messing up the watermarks. The new shards
		//      will be subscribed by this subtask after restore as initial shards on startup.
		//
		// TODO: This is a temporary workaround until a min-watermark information service is available in the JobManager
		// Please see FLINK-4341 for more detail

		boolean emittedMaxValueWatermark = false;

		if (this.numberOfActiveShards.get() == 0) {
			// FLINK-4341 workaround case (a) - please see the above for details on this case
			LOG.info("Subtask {} has no initial shards to read on startup; emitting max value watermark ...",
				indexOfThisConsumerSubtask);
			sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));
			emittedMaxValueWatermark = true;
		}

		while (running) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Subtask {} is trying to discover new shards that were created due to resharding ...",
					indexOfThisConsumerSubtask);
			}
			List<KinesisStreamShard> newShardsDueToResharding = discoverNewShardsToSubscribe();

			// -- NOTE: Potential race condition between newShardsDueToResharding and numberOfActiveShards --
			// Since numberOfActiveShards is updated by parallel shard consuming threads in updateState(), there exists
			// a race condition with the currently queried newShardsDueToResharding. Therefore, numberOfActiveShards
			// may not correctly reflect the discover result in the below case determination. This may lead to incorrect
			// case determination on the current discovery attempt, but can still be correctly handled on future attempts.
			//
			// Although this can be resolved by wrapping the current shard discovery attempt with the below
			// case determination within a synchronized block on the checkpoint lock for atomicity, there will be
			// considerable throughput performance regression as shard discovery is a remote call to AWS. Therefore,
			// since the case determination is a temporary workaround for FLINK-4341, the race condition is tolerable as
			// we can still eventually handle max value watermark emitting / deliberately failing on successive
			// discovery attempts.

			if (newShardsDueToResharding.size() == 0 && this.numberOfActiveShards.get() == 0 && !emittedMaxValueWatermark) {
				// FLINK-4341 workaround case (a) - please see the above for details on this case
				LOG.info("Subtask {} has completed reading all shards; emitting max value watermark ...",
					indexOfThisConsumerSubtask);
				sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));
				emittedMaxValueWatermark = true;
			} else if (newShardsDueToResharding.size() > 0 && emittedMaxValueWatermark) {
				// FLINK-4341 workaround case (b) - please see the above for details on this case
				//
				// Note that in the case where on resharding this subtask ceased to read all of it's previous shards
				// but new shards is also to be subscribed by this subtask immediately after, emittedMaxValueWatermark
				// will be false; this allows the fetcher to continue reading the new shards without failing on such cases.
				// However, due to the race condition mentioned above, we might still fall into case (a) first, and
				// then (b) on the next discovery attempt. Although the failure is ideally unnecessary, max value
				// watermark emitting still remains to be correct.

				LOG.warn("Subtask {} has discovered {} new shards to subscribe, but is failing hard to avoid messing" +
						" up watermarks; the new shards will be subscribed by this subtask after restore ...",
					indexOfThisConsumerSubtask, newShardsDueToResharding.size());
				throw new RuntimeException("Deliberate failure to avoid messing up watermarks");
			}

			for (KinesisStreamShard shard : newShardsDueToResharding) {
				// since there may be delay in discovering a new shard, all new shards due to
				// resharding should be read starting from the earliest record possible
				KinesisStreamShardState newShardState =
					new KinesisStreamShardState(shard, SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get());
				int newStateIndex = registerNewSubscribedShardState(newShardState);

				if (LOG.isInfoEnabled()) {
					LOG.info("Subtask {} has discovered a new shard {} due to resharding, and will start consuming " +
							"the shard from sequence number {} with ShardConsumer {}",
						indexOfThisConsumerSubtask, newShardState.getKinesisStreamShard().toString(),
						newShardState.getLastProcessedSequenceNum(), newStateIndex);
				}

				shardConsumersExecutor.submit(
					new ShardConsumer<>(
						this,
						newStateIndex,
						newShardState.getKinesisStreamShard(),
						newShardState.getLastProcessedSequenceNum()));
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
	public HashMap<KinesisStreamShard, SequenceNumber> snapshotState() {
		// this method assumes that the checkpoint lock is held
		assert Thread.holdsLock(checkpointLock);

		HashMap<KinesisStreamShard, SequenceNumber> stateSnapshot = new HashMap<>();
		for (KinesisStreamShardState shardWithState : subscribedShardsState) {
			stateSnapshot.put(shardWithState.getKinesisStreamShard(), shardWithState.getLastProcessedSequenceNum());
		}
		return stateSnapshot;
	}

	/**
	 * Starts shutting down the fetcher. Must be called to allow {@link KinesisDataFetcher#runFetcher()} to complete.
	 * Once called, the shutdown procedure will be executed and all shard consuming threads will be interrupted.
	 */
	public void shutdownFetcher() {
		running = false;
		mainThread.interrupt(); // the main thread may be sleeping for the discovery interval

		if (LOG.isInfoEnabled()) {
			LOG.info("Shutting down the shard consumer threads of subtask {} ...", indexOfThisConsumerSubtask);
		}
		shardConsumersExecutor.shutdownNow();
	}

	/** After calling {@link KinesisDataFetcher#shutdownFetcher()}, this can be called to await the fetcher shutdown */
	public void awaitTermination() throws InterruptedException {
		while(!shardConsumersExecutor.isTerminated()) {
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

	/** Updates the last discovered shard of a subscribed stream; only updates if the update is valid */
	public void advanceLastDiscoveredShardOfStream(String stream, String shardId) {
		String lastSeenShardIdOfStream = this.subscribedStreamsToLastDiscoveredShardIds.get(stream);

		// the update is valid only if the given shard id is greater
		// than the previous last seen shard id of the stream
		if (lastSeenShardIdOfStream == null) {
			// if not previously set, simply put as the last seen shard id
			this.subscribedStreamsToLastDiscoveredShardIds.put(stream, shardId);
		} else if (KinesisStreamShard.compareShardIds(shardId, lastSeenShardIdOfStream) > 0) {
			this.subscribedStreamsToLastDiscoveredShardIds.put(stream, shardId);
		}
	}

	/**
	 * A utility function that does the following:
	 *
	 * 1. Find new shards for each stream that we haven't seen before
	 * 2. For each new shard, determine whether this consumer subtask should subscribe to them;
	 * 	  if yes, it is added to the returned list of shards
	 * 3. Update the subscribedStreamsToLastDiscoveredShardIds state so that we won't get shards
	 *    that we have already seen before the next time this function is called
	 */
	private List<KinesisStreamShard> discoverNewShardsToSubscribe() throws InterruptedException {

		List<KinesisStreamShard> newShardsToSubscribe = new LinkedList<>();

		GetShardListResult shardListResult = kinesis.getShardList(subscribedStreamsToLastDiscoveredShardIds);
		if (shardListResult.hasRetrievedShards()) {
			Set<String> streamsWithNewShards = shardListResult.getStreamsWithRetrievedShards();

			for (String stream : streamsWithNewShards) {
				List<KinesisStreamShard> newShardsOfStream = shardListResult.getRetrievedShardListOfStream(stream);
				for (KinesisStreamShard newShard : newShardsOfStream) {
					if (isThisSubtaskShouldSubscribeTo(newShard, totalNumberOfConsumerSubtasks, indexOfThisConsumerSubtask)) {
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

	public void setIsRestoringFromFailure(boolean bool) {
		this.isRestoredFromFailure = bool;
	}

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
			sourceContext.collectWithTimestamp(record, recordTimestamp);
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
	protected void updateState(int shardStateIndex, SequenceNumber lastSequenceNumber) {
		synchronized (checkpointLock) {
			subscribedShardsState.get(shardStateIndex).setLastProcessedSequenceNum(lastSequenceNumber);

			// if a shard's state is updated to be SENTINEL_SHARD_ENDING_SEQUENCE_NUM by its consumer thread,
			// we've finished reading the shard and should determine it to be non-active
			if (lastSequenceNumber.equals(SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get())) {
				this.numberOfActiveShards.decrementAndGet();
				LOG.info("Subtask {} has reached the end of subscribed shard: {}",
					indexOfThisConsumerSubtask, subscribedShardsState.get(shardStateIndex).getKinesisStreamShard());
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

			return subscribedShardsState.size()-1;
		}
	}

	// ------------------------------------------------------------------------
	//  Miscellaneous utility functions
	// ------------------------------------------------------------------------

	/**
	 * Utility function to determine whether a shard should be subscribed by this consumer subtask.
	 *
	 * @param shard the shard to determine
	 * @param totalNumberOfConsumerSubtasks total number of consumer subtasks
	 * @param indexOfThisConsumerSubtask index of this consumer subtask
	 */
	private static boolean isThisSubtaskShouldSubscribeTo(KinesisStreamShard shard,
														int totalNumberOfConsumerSubtasks,
														int indexOfThisConsumerSubtask) {
		return (Math.abs(shard.hashCode() % totalNumberOfConsumerSubtasks)) == indexOfThisConsumerSubtask;
	}

	private static ExecutorService createShardConsumersThreadPool(final String subtaskName) {
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

	/**
	 * Utility function to create an initial map of the last discovered shard id of each subscribed stream, set to null;
	 * This is called in the constructor; correct values will be set later on by calling advanceLastDiscoveredShardOfStream()
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
}
