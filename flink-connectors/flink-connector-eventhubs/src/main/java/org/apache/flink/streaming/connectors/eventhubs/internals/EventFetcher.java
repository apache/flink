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

package org.apache.flink.streaming.connectors.eventhubs.internals;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;

import com.microsoft.azure.eventhubs.EventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Created by jozh on 6/14/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector.
 * A fetcher that fetches data from Eventhub via the EventhubUtil.
 * Eventhub offset is stored at flink checkpoint backend
 * @param <T> The type of elements produced by the fetcher.
 */
public class EventFetcher<T> {
	protected static final int NO_TIMESTAMPS_WATERMARKS = 0;
	protected static final int PERIODIC_WATERMARKS = 1;
	protected static final int PUNCTUATED_WATERMARKS = 2;
	private static final Logger logger = LoggerFactory.getLogger(EventFetcher.class);
	private volatile boolean running = true;

	private final KeyedDeserializationSchema<T> deserializer;
	private final Handover handover;
	private final Properties eventhubProps;
	private final EventhubConsumerThread consumerThread;
	private final String taskNameWithSubtasks;


	protected final SourceFunction.SourceContext<T> sourceContext;
	protected final Object checkpointLock;
	private final Map<EventhubPartition, EventhubPartitionState> subscribedPartitionStates;
	protected final int timestampWatermarkMode;
	protected final boolean useMetrics;
	private volatile long maxWatermarkSoFar = Long.MIN_VALUE;
	private Counter receivedCount;

	public EventFetcher(
		SourceFunction.SourceContext sourceContext,
		Map<EventhubPartition, String> assignedPartitionsWithInitialOffsets,
		KeyedDeserializationSchema deserializer,
		SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
		SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
		ProcessingTimeService processTimerProvider,
		long autoWatermarkInterval,
		ClassLoader userCodeClassLoader,
		String taskNameWithSubtasks,
		Properties eventhubProps,
		boolean useMetrics,
		Counter receivedCount) throws Exception {

		this.sourceContext = checkNotNull(sourceContext);
		this.deserializer = checkNotNull(deserializer);
		this.eventhubProps = eventhubProps;
		this.checkpointLock = sourceContext.getCheckpointLock();
		this.useMetrics = useMetrics;
		this.receivedCount = receivedCount;
		this.taskNameWithSubtasks = taskNameWithSubtasks;
		this.timestampWatermarkMode = getTimestampWatermarkMode(watermarksPeriodic, watermarksPunctuated);

		this.subscribedPartitionStates = initializeSubscribedPartitionStates(
			assignedPartitionsWithInitialOffsets,
			timestampWatermarkMode,
			watermarksPeriodic, watermarksPunctuated,
			userCodeClassLoader);

		this.handover = new Handover();
		this.consumerThread = new EventhubConsumerThread(logger,
			handover,
			this.eventhubProps,
			getFetcherName() + " for " + taskNameWithSubtasks,
			this.subscribedPartitionStates.values().toArray(new EventhubPartitionState[this.subscribedPartitionStates.size()]));

		if (this.timestampWatermarkMode == PERIODIC_WATERMARKS) {
			PeriodicWatermarkEmitter periodicEmitter =
				new PeriodicWatermarkEmitter(this.subscribedPartitionStates, sourceContext, processTimerProvider, autoWatermarkInterval);
			periodicEmitter.start();
		}

	}

	public HashMap<EventhubPartition, String> snapshotCurrentState() {
		// this method assumes that the checkpoint lock is held
		logger.debug("snapshot current offset state for subtask {}", taskNameWithSubtasks);
		assert Thread.holdsLock(checkpointLock);

		HashMap<EventhubPartition, String> state = new HashMap<>(subscribedPartitionStates.size());
		for (Map.Entry<EventhubPartition, EventhubPartitionState> partition : subscribedPartitionStates.entrySet()){
			state.put(partition.getKey(), partition.getValue().getOffset());
		}

		return state;
	}

	public void runFetchLoop() throws Exception{
		try {
			final Handover handover = this.handover;
			consumerThread.start();
			logger.info("Eventhub consumer thread started for substask {}", taskNameWithSubtasks);

			logger.info("Start fetcher loop to get data from eventhub and emit to flink for subtask {}", taskNameWithSubtasks);
			while (running){
				final Tuple2<EventhubPartition, Iterable<EventData>> eventsTuple = handover.pollNext();
				for (EventData event : eventsTuple.f1){
					final T value = deserializer.deserialize(null,
						event.getBytes(),
						event.getSystemProperties().getPartitionKey(),
						eventsTuple.f0.getParitionId(),
						event.getSystemProperties().getSequenceNumber());

					if (deserializer.isEndOfStream(value)){
						running = false;
						break;
					}
					emitRecord(value, subscribedPartitionStates.get(eventsTuple.f0), event.getSystemProperties().getOffset());
					receivedCount.inc();
				}
			}
		}
		finally {
			logger.warn("Stopping eventhub consumer thread of subtask {}, because something wrong when deserializing received event "
				, taskNameWithSubtasks);
			consumerThread.shutdown();
		}

		try {
			consumerThread.join();
			logger.warn("Waiting eventhub consumer thread of subtask {} stopped", taskNameWithSubtasks);
		}
		catch (InterruptedException ex){
			Thread.currentThread().interrupt();
		}

		logger.info("EventFetcher of subtask {} stopped", taskNameWithSubtasks);
	}

	public void cancel(){
		logger.info("EventFetcher of subtask {} canceled on demand", taskNameWithSubtasks);
		running = false;
		handover.close();
		consumerThread.shutdown();
	}

	protected void emitRecord(T record, EventhubPartitionState partitionState, String offset) throws Exception{
		if (record == null){
			synchronized (this.checkpointLock){
				partitionState.setOffset(offset);
			}
			return;
		}

		if (timestampWatermarkMode == NO_TIMESTAMPS_WATERMARKS){
			synchronized (this.checkpointLock){
				sourceContext.collect(record);
				partitionState.setOffset(offset);
			}
		}
		else if (timestampWatermarkMode == PERIODIC_WATERMARKS){
			emitRecordWithTimestampAndPeriodicWatermark(record, partitionState, offset, Long.MIN_VALUE);
		}
		else {
			emitRecordWithTimestampAndPunctuatedWatermark(record, partitionState, offset, Long.MIN_VALUE);
		}
	}

	protected void emitRecordWithTimestampAndPunctuatedWatermark(
		T record,
		EventhubPartitionState partitionState,
		String offset,
		long eventTimestamp) {

		final EventhubPartitionStateWithPeriodicWatermarks<T> withWatermarksState =
			(EventhubPartitionStateWithPeriodicWatermarks<T>) partitionState;

		final long timestamp;
		synchronized (withWatermarksState) {
			timestamp = withWatermarksState.getTimestampForRecord(record, eventTimestamp);
		}

		synchronized (checkpointLock) {
			sourceContext.collectWithTimestamp(record, timestamp);
			partitionState.setOffset(offset);
		}
	}

	protected void emitRecordWithTimestampAndPeriodicWatermark(
		T record,
		EventhubPartitionState partitionState,
		String offset,
		long eventTimestamp) {

		final EventhubPartitionStateWithPunctuatedWatermarks<T> withWatermarksState =
			(EventhubPartitionStateWithPunctuatedWatermarks<T>) partitionState;

		final long timestamp = withWatermarksState.getTimestampForRecord(record, eventTimestamp);
		final Watermark newWatermark = withWatermarksState.checkAndGetNewWatermark(record, timestamp);

		synchronized (checkpointLock) {
			sourceContext.collectWithTimestamp(record, timestamp);
			partitionState.setOffset(offset);
		}

		if (newWatermark != null) {
			updateMinPunctuatedWatermark(newWatermark);
		}
	}

	protected String getFetcherName() {
		return "Eventhubs Fetcher";
	}

	private int getTimestampWatermarkMode(SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
	SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated)
		throws IllegalArgumentException {
		if (watermarksPeriodic == null){
			if (watermarksPunctuated == null){
				return NO_TIMESTAMPS_WATERMARKS;
			}
			else {
				return PUNCTUATED_WATERMARKS;
			}
		}
		else {
			if (watermarksPunctuated == null){
				return PERIODIC_WATERMARKS;
			}
			else {
				throw new IllegalArgumentException("Cannot have both periodic and punctuated watermarks");
			}
		}
	}

	private Map<EventhubPartition, EventhubPartitionState> initializeSubscribedPartitionStates(
		Map<EventhubPartition, String> assignedPartitionsWithInitialOffsets,
		int timestampWatermarkMode,
		SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
		SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
		ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

		if (timestampWatermarkMode != NO_TIMESTAMPS_WATERMARKS
			&& timestampWatermarkMode != PERIODIC_WATERMARKS
			&& timestampWatermarkMode != PUNCTUATED_WATERMARKS) {
			throw new RuntimeException();
		}

		Map<EventhubPartition, EventhubPartitionState> partitionsState = new HashMap<>(assignedPartitionsWithInitialOffsets.size());
		for (Map.Entry<EventhubPartition, String> partition : assignedPartitionsWithInitialOffsets.entrySet()){
			switch (timestampWatermarkMode){
				case NO_TIMESTAMPS_WATERMARKS:{
					partitionsState.put(partition.getKey(), new EventhubPartitionState(partition.getKey(), partition.getValue()));
					logger.info("NO_TIMESTAMPS_WATERMARKS: Assigned partition {}, offset is {}", partition.getKey(), partition.getValue());
					break;
				}

				case PERIODIC_WATERMARKS:{
					AssignerWithPeriodicWatermarks<T> assignerInstance =
						watermarksPeriodic.deserializeValue(userCodeClassLoader);
					partitionsState.put(partition.getKey(),
						new EventhubPartitionStateWithPeriodicWatermarks(partition.getKey(), partition.getValue(), assignerInstance));
					logger.info("PERIODIC_WATERMARKS: Assigned partition {}, offset is {}", partition.getKey(), partition.getValue());
					break;
				}

				case PUNCTUATED_WATERMARKS: {
					AssignerWithPunctuatedWatermarks<T> assignerInstance =
						watermarksPunctuated.deserializeValue(userCodeClassLoader);
					partitionsState.put(partition.getKey(),
						new EventhubPartitionStateWithPunctuatedWatermarks(partition.getKey(), partition.getValue(), assignerInstance));
					logger.info("PUNCTUATED_WATERMARKS: Assigned partition {}, offset is {}", partition.getKey(), partition.getValue());
					break;
				}
			}
		}
		return partitionsState;
	}

	private void updateMinPunctuatedWatermark(Watermark nextWatermark) {
		if (nextWatermark.getTimestamp() > maxWatermarkSoFar) {
			long newMin = Long.MAX_VALUE;

			for (Map.Entry<EventhubPartition, EventhubPartitionState> partition : subscribedPartitionStates.entrySet()){
				final EventhubPartitionStateWithPunctuatedWatermarks<T> withWatermarksState =
					(EventhubPartitionStateWithPunctuatedWatermarks<T>) partition.getValue();

				newMin = Math.min(newMin, withWatermarksState.getCurrentPartitionWatermark());
			}

			// double-check locking pattern
			if (newMin > maxWatermarkSoFar) {
				synchronized (checkpointLock) {
					if (newMin > maxWatermarkSoFar) {
						maxWatermarkSoFar = newMin;
						sourceContext.emitWatermark(new Watermark(newMin));
					}
				}
			}
		}
	}

	private static class PeriodicWatermarkEmitter implements ProcessingTimeCallback {

		private final Map<EventhubPartition, EventhubPartitionState> allPartitions;

		private final SourceFunction.SourceContext<?> emitter;

		private final ProcessingTimeService timerService;

		private final long interval;

		private long lastWatermarkTimestamp;

		//-------------------------------------------------

		PeriodicWatermarkEmitter(
			Map<EventhubPartition, EventhubPartitionState> allPartitions,
			SourceFunction.SourceContext<?> emitter,
			ProcessingTimeService timerService,
			long autoWatermarkInterval) {
			this.allPartitions = checkNotNull(allPartitions);
			this.emitter = checkNotNull(emitter);
			this.timerService = checkNotNull(timerService);
			this.interval = autoWatermarkInterval;
			this.lastWatermarkTimestamp = Long.MIN_VALUE;
		}

		public void start() {
			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}

		@Override
		public void onProcessingTime(long timestamp) throws Exception {

			long minAcrossAll = Long.MAX_VALUE;
			for (Map.Entry<EventhubPartition, EventhubPartitionState> partition : allPartitions.entrySet()){
				final long curr;
				EventhubPartitionStateWithPeriodicWatermarks state =
					(EventhubPartitionStateWithPeriodicWatermarks) partition.getValue();

				synchronized (state) {
					curr = state.getCurrentWatermarkTimestamp();
				}

				minAcrossAll = Math.min(minAcrossAll, curr);
			}

			if (minAcrossAll > lastWatermarkTimestamp) {
				lastWatermarkTimestamp = minAcrossAll;
				emitter.emitWatermark(new Watermark(minAcrossAll));
			}

			timerService.registerTimer(timerService.getCurrentProcessingTime() + interval, this);
		}
	}
}
