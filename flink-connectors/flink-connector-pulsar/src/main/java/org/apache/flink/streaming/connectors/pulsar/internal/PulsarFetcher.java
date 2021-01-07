/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.util.SerializedValue;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.internal.metrics.PulsarSourceMetrics.COMMITTED_OFFSETS_METRICS_GAUGE;
import static org.apache.flink.streaming.connectors.pulsar.internal.metrics.PulsarSourceMetrics.CURRENT_OFFSETS_METRICS_GAUGE;
import static org.apache.flink.streaming.connectors.pulsar.internal.metrics.PulsarSourceMetrics.OFFSETS_BY_TOPIC_METRICS_GROUP;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implements the logic around emitting records and tracking offsets,
 * as well as around the optional timestamp assignment and watermark generation.
 *
 * @param <T> The type of elements deserialized from Pulsar messages, and emitted into
 * 	the Flink data stream.
 */
@Internal
public class PulsarFetcher<T> {
	private static final Logger log = LoggerFactory.getLogger(PulsarFetcher.class);
	private static final int NO_TIMESTAMPS_WATERMARKS = 0;
	private static final int WITH_WATERMARK_GENERATOR = 1;

	// ------------------------------------------------------------------------

	/** The source context to emit records and watermarks to. */
	protected final SourceContext<T> sourceContext;

	protected final Map<TopicRange, MessageId> seedTopicsWithInitialOffsets;

	/**
	 * The lock that guarantees that record emission and state updates are atomic,
	 * from the view of taking a checkpoint.
	 */
	private final Object checkpointLock;

	/** All partitions (and their state) that this fetcher is subscribed to. */
	protected final List<PulsarTopicState<T>> subscribedPartitionStates;

	/**
	 * Queue of partitions that are not yet assigned to any reader thread for consuming.
	 *
	 * <p>All partitions added to this queue are guaranteed to have been added
	 * to {@link #subscribedPartitionStates} already.
	 */
	protected final ClosableBlockingQueue<PulsarTopicState<T>> unassignedPartitionsQueue;

	/** The mode describing whether the fetcher also generates timestamps and watermarks. */
	private final int timestampWatermarkMode;

	/**
	 * Optional watermark strategy that will be run per pulsar partition, to exploit per-partition
	 * timestamp characteristics. The watermark strategy is kept in serialized form, to deserialize
	 * it into multiple copies.
	 */
	private final SerializedValue<WatermarkStrategy<T>> watermarkStrategy;

	/** User class loader used to deserialize watermark assigners. */
	private final ClassLoader userCodeClassLoader;

	private final StreamingRuntimeContext runtimeContext;

	protected final ClientConfigurationData clientConf;

	protected final Map<String, Object> readerConf;

	protected final PulsarDeserializationSchema<T> deserializer;

	protected final int pollTimeoutMs;

	private final int commitMaxRetries;

	protected final PulsarMetadataReader metadataReader;

	/**
	 * Wrapper around our SourceContext for allowing the {@link org.apache.flink.api.common.eventtime.WatermarkGenerator}
	 * to emit watermarks and mark idleness.
	 */
	protected final WatermarkOutput watermarkOutput;

	/**
	 * {@link WatermarkOutputMultiplexer} for supporting per-partition watermark generation.
	 */
	private final WatermarkOutputMultiplexer watermarkOutputMultiplexer;

	/** Flag to mark the main work loop as alive. */
	private volatile boolean running = true;

	/** The threads that runs the actual reading and hand the records to this fetcher. */
	private Map<TopicRange, ReaderThread<T>> topicToThread;

	/** Failed or not when data loss. **/
	private boolean failOnDataLoss = true;

	// ------------------------------------------------------------------------
	//  Metrics
	// ------------------------------------------------------------------------

	/**
	 * Flag indicating whether or not metrics should be exposed.
	 * If {@code true}, offset metrics (e.g. current offset, committed offset) and
	 * pulsar-shipped metrics will be registered.
	 */
	private final boolean useMetrics;

	/**
	 * The metric group which all metrics for the source should be registered to.
	 */
	private final MetricGroup consumerMetricGroup;

	public PulsarFetcher(
		SourceContext<T> sourceContext,
		Map<TopicRange, MessageId> seedTopicsWithInitialOffsets,
		SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
		ProcessingTimeService processingTimeProvider,
		long autoWatermarkInterval,
		ClassLoader userCodeClassLoader,
		StreamingRuntimeContext runtimeContext,
		ClientConfigurationData clientConf,
		Map<String, Object> readerConf,
		int pollTimeoutMs,
		PulsarDeserializationSchema<T> deserializer,
		PulsarMetadataReader metadataReader,
		MetricGroup consumerMetricGroup,
		boolean useMetrics) throws Exception {
		this(
			sourceContext,
			seedTopicsWithInitialOffsets,
			watermarkStrategy,
			processingTimeProvider,
			autoWatermarkInterval,
			userCodeClassLoader,
			runtimeContext,
			clientConf,
			readerConf,
			pollTimeoutMs,
			3, // commit retries before fail
			deserializer,
			metadataReader,
			consumerMetricGroup,
			useMetrics);
	}

	public PulsarFetcher(
		SourceContext<T> sourceContext,
		Map<TopicRange, MessageId> seedTopicsWithInitialOffsets,
		SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
		ProcessingTimeService processingTimeProvider,
		long autoWatermarkInterval,
		ClassLoader userCodeClassLoader,
		StreamingRuntimeContext runtimeContext,
		ClientConfigurationData clientConf,
		Map<String, Object> readerConf,
		int pollTimeoutMs,
		int commitMaxRetries,
		PulsarDeserializationSchema<T> deserializer,
		PulsarMetadataReader metadataReader,
		MetricGroup consumerMetricGroup,
		boolean useMetrics) throws Exception {

		this.sourceContext = sourceContext;
		this.watermarkOutput = new SourceContextWatermarkOutputAdapter<>(sourceContext);
		this.watermarkOutputMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
		this.useMetrics = useMetrics;
		this.consumerMetricGroup = checkNotNull(consumerMetricGroup);
		this.seedTopicsWithInitialOffsets = seedTopicsWithInitialOffsets;
		this.checkpointLock = sourceContext.getCheckpointLock();
		this.userCodeClassLoader = userCodeClassLoader;
		this.runtimeContext = runtimeContext;
		this.clientConf = clientConf;
		this.readerConf = readerConf == null ? new HashMap<>() : readerConf;

		String failOnDataLossVal = this.readerConf
			.getOrDefault(PulsarOptions.FAIL_ON_DATA_LOSS_OPTION_KEY, "true")
			.toString();
		this.failOnDataLoss = Boolean.parseBoolean(failOnDataLossVal);
		this.readerConf.remove(PulsarOptions.FAIL_ON_DATA_LOSS_OPTION_KEY);

		this.pollTimeoutMs = pollTimeoutMs;
		this.commitMaxRetries = commitMaxRetries;
		this.deserializer = deserializer;
		this.metadataReader = metadataReader;

		// figure out what we watermark mode we will be using
		this.watermarkStrategy = watermarkStrategy;

		if (watermarkStrategy == null) {
			timestampWatermarkMode = NO_TIMESTAMPS_WATERMARKS;
		} else {
			timestampWatermarkMode = WITH_WATERMARK_GENERATOR;
		}

		this.unassignedPartitionsQueue = new ClosableBlockingQueue<>();

		// initialize subscribed partition states with seed partitions
		this.subscribedPartitionStates = createPartitionStateHolders(
			seedTopicsWithInitialOffsets,
			timestampWatermarkMode,
			watermarkStrategy,
			userCodeClassLoader);

		// check that all seed partition states have a defined offset
		for (PulsarTopicState<T> state : subscribedPartitionStates) {
			if (!state.isOffsetDefined()) {
				throw new IllegalArgumentException(
					"The fetcher was assigned seed partitions with undefined initial offsets.");
			}
		}

		// all seed partitions are not assigned yet, so should be added to the unassigned partitions queue
		for (PulsarTopicState<T> state : subscribedPartitionStates) {
			unassignedPartitionsQueue.add(state);
		}

		// register metrics for the initial seed partitions
		if (useMetrics) {
			registerOffsetMetrics(consumerMetricGroup, subscribedPartitionStates);
		}

		// if we have periodic watermarks, kick off the interval scheduler
		if (timestampWatermarkMode == WITH_WATERMARK_GENERATOR && autoWatermarkInterval > 0) {
			PeriodicWatermarkEmitter<T> periodicEmitter = new PeriodicWatermarkEmitter<>(
				checkpointLock,
				subscribedPartitionStates,
				watermarkOutputMultiplexer,
				processingTimeProvider,
				autoWatermarkInterval);

			periodicEmitter.start();
		}
	}

	public void runFetchLoop() throws Exception {
		topicToThread = new HashMap<>();
		ExceptionProxy exceptionProxy = new ExceptionProxy(Thread.currentThread());

		try {

			while (running) {
				// re-throw any exception from the concurrent fetcher threads
				exceptionProxy.checkAndThrowException();

				// wait for max 5 seconds trying to get partitions to assign
				// if threads shut down, this poll returns earlier, because the threads inject the
				// special marker into the queue
				List<PulsarTopicState<T>> topicsToAssign = unassignedPartitionsQueue.getBatchBlocking(
					5000);
				// if there are more markers, remove them all
				topicsToAssign.removeIf(s -> s.equals(PoisonState.INSTANCE));

				if (!topicsToAssign.isEmpty()) {

					if (!running) {
						throw BreakingException.INSTANCE;
					}

					topicToThread.putAll(
						createAndStartReaderThread(topicsToAssign, exceptionProxy));

				} else {
					// there were no partitions to assign. Check if any consumer threads shut down.
					// we get into this section of the code, if either the poll timed out, or the
					// blocking poll was woken up by the marker element

					topicToThread.values().removeIf(t -> !t.isRunning());

				}

				if (topicToThread.size() == 0 && unassignedPartitionsQueue.isEmpty()) {
					PulsarTopicState topicForBlocking = unassignedPartitionsQueue.getElementBlocking();
					if (topicForBlocking.equals(PoisonState.INSTANCE)) {
						throw BreakingException.INSTANCE;
					}
					topicToThread.putAll(
						createAndStartReaderThread(
							ImmutableList.of(topicForBlocking),
							exceptionProxy));
				}
			}

		} catch (BreakingException b) {
			// do nothing
		} catch (InterruptedException e) {
			// this may be thrown because an exception on one of the concurrent fetcher threads
			// woke this thread up. make sure we throw the root exception instead in that case
			exceptionProxy.checkAndThrowException();

			// no other root exception, throw the interrupted exception
			throw e;
		} finally {
			running = false;

			// clear the interruption flag
			// this allows the joining on reader threads (on best effort) to happen in
			// case the initial interrupt already
			Thread.interrupted();

			// make sure that in any case (completion, abort, error), all spawned threads are stopped
			try {
				int runningThreads = 0;
				do { // check whether threads are alive and cancel them
					runningThreads = 0;

					topicToThread.values().removeIf(s -> !s.isAlive());

					for (ReaderThread t : topicToThread.values()) {
						t.cancel();
						runningThreads++;
					}

					if (runningThreads > 0) {
						for (ReaderThread t : topicToThread.values()) {
							t.join(500 / runningThreads + 1);
						}

					}

				} while (runningThreads > 0);

			} catch (InterruptedException ignored) {
				// waiting for the thread shutdown apparently got interrupted
				// restore interrupted state and continue
				Thread.currentThread().interrupt();
			} catch (Throwable t) {
				// we catch all here to preserve the original exception
				log.error("Exception while shutting down reader threads", t);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  emitting records
	// ------------------------------------------------------------------------

	/**
	 * Emits a record attaching a timestamp to it.
	 *
	 * @param record The record to emit
	 * @param partitionState The state of the pulsar partition from which the record was fetched
	 * @param offset The offset of the corresponding pulsar record
	 * @param pulsarEventTimestamp The timestamp of the pulsar record
	 */
	protected void emitRecordsWithTimestamps(
		T record,
		PulsarTopicState<T> partitionState,
		MessageId offset,
		long pulsarEventTimestamp) {
		// emit the records, using the checkpoint lock to guarantee
		// atomicity of record emission and offset state update
		synchronized (checkpointLock) {
			if (record != null) {
				long timestamp = partitionState.extractTimestamp(record, pulsarEventTimestamp);
				sourceContext.collectWithTimestamp(record, timestamp);

				// this might emit a watermark, so do it after emitting the record
				partitionState.onEvent(record, timestamp);
			}
			partitionState.setOffset(offset);
		}
	}

	public void cancel() throws Exception {
		// single the main thread to exit
		running = false;

		Set<TopicRange> topics = subscribedPartitionStates.stream()
			.map(PulsarTopicState::getTopicRange).collect(Collectors.toSet());

		metadataReader.removeCursor(topics);
		// make sure the main thread wakes up soon
		unassignedPartitionsQueue.addIfOpen(PoisonState.INSTANCE);
	}

	public void commitOffsetToPulsar(
		Map<TopicRange, MessageId> offset,
		PulsarCommitCallback offsetCommitCallback) throws InterruptedException {

		doCommitOffsetToPulsar(removeEarliestAndLatest(offset), offsetCommitCallback);
	}

	protected void doCommitOffsetToPulsar(
		Map<TopicRange, MessageId> offset,
		PulsarCommitCallback offsetCommitCallback) throws InterruptedException {

		try {
			int retries = 0;
			boolean success = false;
			while (running) {
				try {
					metadataReader.commitCursorToOffset(offset);
					success = true;
					break;
				} catch (Exception e) {
					log.warn("Failed to commit cursor to Pulsar.", e);
					if (retries >= commitMaxRetries) {
						log.error("Failed to commit cursor to Pulsar after {} attempts", retries);
						throw e;
					}
					retries += 1;
					Thread.sleep(1000);
				}
			}
			if (success) {
				offsetCommitCallback.onSuccess();
			} else {
				return;
			}
		} catch (Exception e) {
			if (running) {
				offsetCommitCallback.onException(e);
			} else {
				return;
			}
		}

		for (PulsarTopicState state : subscribedPartitionStates) {
			MessageId off = offset.get(state.getTopicRange());
			if (off != null) {
				state.setCommittedOffset(off);
			}
		}
	}

	public Map<TopicRange, MessageId> removeEarliestAndLatest(Map<TopicRange, MessageId> offset) {
		Map<TopicRange, MessageId> result = new HashMap<>();
		for (Map.Entry<TopicRange, MessageId> entry : offset.entrySet()) {
			MessageId mid = entry.getValue();
			if (!mid.equals(MessageId.earliest) && !mid.equals(MessageId.latest)) {
				result.put(entry.getKey(), mid);
			}
		}
		return result;
	}

	public void addDiscoveredTopics(Set<TopicRange> newTopics) throws IOException, ClassNotFoundException {
		List<PulsarTopicState<T>> newStates = createPartitionStateHolders(
			newTopics.stream().collect(Collectors.toMap(t -> t, t -> MessageId.earliest)),
			timestampWatermarkMode,
			watermarkStrategy,
			userCodeClassLoader);

		for (PulsarTopicState state : newStates) {
			// the ordering is crucial here; first register the state holder, then
			// push it to the partitions queue to be read
			subscribedPartitionStates.add(state);
			unassignedPartitionsQueue.add(state);
		}
	}

	// ------------------------------------------------------------------------
	//  snapshot and restore the state
	// ------------------------------------------------------------------------

	/**
	 * Takes a snapshot of the partition offsets.
	 *
	 * <p>Important: This method must be called under the checkpoint lock.
	 *
	 * @return A map from partition to current offset.
	 */
	public Map<TopicRange, MessageId> snapshotCurrentState() {
		// this method assumes that the checkpoint lock is held
		assert Thread.holdsLock(checkpointLock);

		Map<TopicRange, MessageId> state = new HashMap<>(subscribedPartitionStates.size());

		for (PulsarTopicState pa : subscribedPartitionStates) {
			state.put(pa.getTopicRange(), pa.getOffset());
		}
		return state;
	}

	public Map<TopicRange, ReaderThread<T>> createAndStartReaderThread(
		List<PulsarTopicState<T>> states,
		ExceptionProxy exceptionProxy) {

		Map<TopicRange, MessageId> startingOffsets = states
			.stream()
			.collect(Collectors.toMap(
				PulsarTopicState::getTopicRange,
				PulsarTopicState::getOffset));
		metadataReader.setupCursor(startingOffsets, failOnDataLoss);
		Map<TopicRange, ReaderThread<T>> topic2Threads = new HashMap<>();

		for (PulsarTopicState state : states) {
			ReaderThread<T> readerT = createReaderThread(exceptionProxy, state);
			readerT.setName(String.format(
				"Pulsar Reader for %s in task %s",
				state.getTopicRange(),
				runtimeContext.getTaskName()));
			readerT.setDaemon(true);
			readerT.start();
			log.info("Starting Thread {}", readerT.getName());
			topic2Threads.put(state.getTopicRange(), readerT);
		}
		return topic2Threads;
	}

	protected List<PulsarTopicState<T>> getSubscribedTopicStates() {
		return subscribedPartitionStates;
	}

	protected ReaderThread<T> createReaderThread(
		ExceptionProxy exceptionProxy,
		PulsarTopicState state) {
		return new ReaderThread<>(
			this,
			state,
			clientConf,
			readerConf,
			deserializer,
			pollTimeoutMs,
			exceptionProxy,
			failOnDataLoss);
	}

	/**
	 * Utility method that takes the topic partitions and creates the topic partition state
	 * holders, depending on the timestamp / watermark mode.
	 */
	private List<PulsarTopicState<T>> createPartitionStateHolders(
		Map<TopicRange, MessageId> partitionsToInitialOffsets,
		int timestampWatermarkMode,
		SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
		ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

		// CopyOnWrite as adding discovered partitions could happen in parallel
		// while different threads iterate the partitions list
		List<PulsarTopicState<T>> partitionStates = new CopyOnWriteArrayList<>();

		switch (timestampWatermarkMode) {
			case NO_TIMESTAMPS_WATERMARKS: {
				for (Map.Entry<TopicRange, MessageId> partitionEntry : partitionsToInitialOffsets.entrySet()) {
					PulsarTopicState<T> state = new PulsarTopicState(partitionEntry.getKey());
					state.setOffset(partitionEntry.getValue());
					partitionStates.add(state);
				}
				return partitionStates;
			}

			case WITH_WATERMARK_GENERATOR: {
				for (Map.Entry<TopicRange, MessageId> partitionEntry : partitionsToInitialOffsets.entrySet()) {
					final TopicRange topicRange = partitionEntry.getKey();

					PulsarTopicState<T> state = new PulsarTopicState(partitionEntry.getKey());
					WatermarkStrategy<T> deserializedWatermarkStrategy = watermarkStrategy.deserializeValue(
						userCodeClassLoader);

					// the format of the ID does not matter, as long as it is unique
					final String partitionId = state.getTopicRange().toString();
					watermarkOutputMultiplexer.registerNewOutput(partitionId);
					WatermarkOutput immediateOutput =
						watermarkOutputMultiplexer.getImmediateOutput(partitionId);
					WatermarkOutput deferredOutput =
						watermarkOutputMultiplexer.getDeferredOutput(partitionId);

					PulsarTopicPartitionStateWithWatermarkGenerator<T> partitionState =
						new PulsarTopicPartitionStateWithWatermarkGenerator<>(
							topicRange,
							state,
							deserializedWatermarkStrategy.createTimestampAssigner(() -> consumerMetricGroup),
							deserializedWatermarkStrategy.createWatermarkGenerator(() -> consumerMetricGroup),
							immediateOutput,
							deferredOutput);

					partitionState.setOffset(partitionEntry.getValue());

					partitionStates.add(partitionState);

				}

				return partitionStates;
			}

			default:
				// cannot happen, add this as a guard for the future
				throw new RuntimeException();
		}
	}

	// ------------------------- Metrics ----------------------------------

	/**
	 * For each partition, register a new metric group to expose current offsets and committed offsets.
	 * Per-partition metric groups can be scoped by user variables.
	 *
	 * <p>Note: this method also registers gauges for deprecated offset metrics, to maintain backwards compatibility.
	 *
	 * @param consumerMetricGroup The consumer metric group
	 * @param partitionOffsetStates The partition offset state holders, whose values will be used to update metrics
	 */
	private void registerOffsetMetrics(
		MetricGroup consumerMetricGroup,
		List<PulsarTopicState<T>> partitionOffsetStates) {

		for (PulsarTopicState<T> pts : partitionOffsetStates) {
			MetricGroup topicPartitionGroup = consumerMetricGroup
				.addGroup(OFFSETS_BY_TOPIC_METRICS_GROUP, pts.getTopicRange().getTopic());

			topicPartitionGroup.gauge(
				CURRENT_OFFSETS_METRICS_GAUGE,
				new OffsetGauge(pts, OffsetGaugeType.CURRENT_OFFSET));
			topicPartitionGroup.gauge(
				COMMITTED_OFFSETS_METRICS_GAUGE,
				new OffsetGauge(pts, OffsetGaugeType.COMMITTED_OFFSET));
		}
	}

	/**
	 * Gauge types.
	 */
	private enum OffsetGaugeType {
		CURRENT_OFFSET,
		COMMITTED_OFFSET
	}

	/**
	 * Gauge for getting the offset of a PulsarTopicState.
	 */
	private static class OffsetGauge implements Gauge<MessageId> {

		private final PulsarTopicState<?> pts;
		private final OffsetGaugeType gaugeType;

		OffsetGauge(PulsarTopicState<?> pts, OffsetGaugeType gaugeType) {
			this.pts = pts;
			this.gaugeType = gaugeType;
		}

		@Override
		public MessageId getValue() {
			switch (gaugeType) {
				case COMMITTED_OFFSET:
					return pts.getCommittedOffset();
				case CURRENT_OFFSET:
					return pts.getOffset();
				default:
					throw new RuntimeException("Unknown gauge type: " + gaugeType);
			}
		}
	}
	// ------------------------------------------------------------------------

	/**
	 * The periodic watermark emitter. In its given interval, it checks all partitions for
	 * the current event time watermark, and possibly emits the next watermark.
	 */
	private static class PeriodicWatermarkEmitter<T> implements ProcessingTimeCallback {

		private final Object checkpointLock;

		private final List<PulsarTopicState<T>> allPartitions;

		private final WatermarkOutputMultiplexer watermarkOutputMultiplexer;

		private final ProcessingTimeService timerService;

		private final long interval;

		//-------------------------------------------------

		PeriodicWatermarkEmitter(
			Object checkpointLock,
			List<PulsarTopicState<T>> allPartitions,
			WatermarkOutputMultiplexer watermarkOutputMultiplexer,
			ProcessingTimeService timerService,
			long autoWatermarkInterval
		) {
			this.checkpointLock = checkpointLock;
			this.allPartitions = checkNotNull(allPartitions);
			this.watermarkOutputMultiplexer = watermarkOutputMultiplexer;
			this.timerService = checkNotNull(timerService);
			this.interval = autoWatermarkInterval;
		}

		//-------------------------------------------------

		public void start() {
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}

		@Override
		public void onProcessingTime(long timestamp) throws Exception {
			synchronized (checkpointLock) {
				for (PulsarTopicState<?> state : allPartitions) {
					state.onPeriodicEmit();
				}

				watermarkOutputMultiplexer.onPeriodicEmit();
			}

			// schedule the next watermark
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}
	}

	private static class BreakingException extends Exception {
		static final BreakingException INSTANCE = new BreakingException();

		private BreakingException() {
		}
	}

	public PulsarMetadataReader getMetaDataReader() {
		return this.metadataReader;
	}
}
