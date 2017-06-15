package org.apache.flink.streaming.connectors.eventhubs;

/**
 * Created by jozh on 6/14/2017.
 */
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import com.microsoft.azure.eventhubs.EventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;




/**
 * A fetcher that fetches data from Eventhub via the EventhubUtil.
 * Eventhub offset is stored at flink checkpoint backend
 *
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
		boolean useMetrics) throws Exception {

		this.sourceContext = Preconditions.checkNotNull(sourceContext);
		this.deserializer = Preconditions.checkNotNull(deserializer);
		this.eventhubProps = eventhubProps;
		this.checkpointLock = sourceContext.getCheckpointLock();
		this.useMetrics = useMetrics;
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
			throw new Exception("Not implemented yet for AssignerWithPeriodicWatermarks");
		}
		else {
			throw new Exception("Not implemented yet for AssignerWithPunctuatedWatermarks");
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
		int timestampWatermarkMode, SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
		SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
		ClassLoader userCodeClassLoader) {

		Map<EventhubPartition, EventhubPartitionState> partitionsState = new HashMap<>(assignedPartitionsWithInitialOffsets.size());
		for (Map.Entry<EventhubPartition, String> partition : assignedPartitionsWithInitialOffsets.entrySet()){
			partitionsState.put(partition.getKey(), new EventhubPartitionState(partition.getKey(), partition.getValue()));
			logger.info("Assigned partition {}, offset is {}", partition.getKey(), partition.getValue());
		}

		return partitionsState;
	}
}
