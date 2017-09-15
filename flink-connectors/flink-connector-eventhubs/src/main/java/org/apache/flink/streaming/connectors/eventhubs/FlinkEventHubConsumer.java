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

package org.apache.flink.streaming.connectors.eventhubs;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.eventhubs.internals.EventFetcher;
import org.apache.flink.streaming.connectors.eventhubs.internals.EventhubPartition;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import com.microsoft.azure.eventhubs.PartitionReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
/**
 * Created by jozh on 5/22/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 * This class is used to create datastream from event hub
 */

public class FlinkEventHubConsumer<T> extends RichParallelSourceFunction<T>  implements
	CheckpointedFunction,
	ResultTypeQueryable<T> {
	private static final long serialVersionUID = -3247976470793561346L;
	protected static final Logger LOGGER = LoggerFactory.getLogger(FlinkEventHubConsumer.class);
	protected static final String DEFAULTOFFSETSTATENAME = "flink.eventhub.offset";

	protected final KeyedDeserializationSchema<T> deserializer;
	protected final Properties eventhubsProps;
	protected final int partitionCount;
	protected List<Tuple2<EventhubPartition, String>> subscribedPartitions;
	protected final String defaultEventhubInitOffset;

	private Map<EventhubPartition, String> subscribedPartitionsToStartOffsets;
	private SerializedValue<AssignerWithPeriodicWatermarks<T>> periodicWatermarkAssigner;
	private SerializedValue<AssignerWithPunctuatedWatermarks<T>> punctuatedWatermarkAssigner;
	private transient ListState<Tuple2<EventhubPartition, String>> offsetsStateForCheckpoint;
	private transient volatile EventFetcher<T> eventhubFetcher;
	private transient volatile HashMap<EventhubPartition, String> restoreToOffset;
	private volatile boolean running = true;

	private Counter receivedCount;

	public FlinkEventHubConsumer(Properties eventhubsProps, DeserializationSchema<T> deserializer){
		this(eventhubsProps, new KeyedDeserializationSchemaWrapper<T>(deserializer));
	}

	public FlinkEventHubConsumer(Properties eventhubsProps, KeyedDeserializationSchema<T> deserializer){
		Preconditions.checkNotNull(eventhubsProps);
		Preconditions.checkNotNull(deserializer);
		Preconditions.checkNotNull(eventhubsProps.getProperty("eventhubs.policyname"));
		Preconditions.checkNotNull(eventhubsProps.getProperty("eventhubs.policykey"));
		Preconditions.checkNotNull(eventhubsProps.getProperty("eventhubs.namespace"));
		Preconditions.checkNotNull(eventhubsProps.getProperty("eventhubs.name"));
		Preconditions.checkNotNull(eventhubsProps.getProperty("eventhubs.partition.count"));

		this.eventhubsProps = eventhubsProps;
		this.partitionCount = Integer.parseInt(eventhubsProps.getProperty("eventhubs.partition.count"));
		this.deserializer = deserializer;

		String userDefinedOffset = eventhubsProps.getProperty("eventhubs.auto.offset");
		if (userDefinedOffset != null && userDefinedOffset.toLowerCase().compareTo("lastest") == 0){
			this.defaultEventhubInitOffset = PartitionReceiver.END_OF_STREAM;
		}
		else {
			this.defaultEventhubInitOffset = PartitionReceiver.START_OF_STREAM;
		}

		if (this.partitionCount <= 0){
			throw new IllegalArgumentException("eventhubs.partition.count must greater than 0");
		}
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		receivedCount = getRuntimeContext().getMetricGroup().addGroup(this.getClass().getName()).counter("received_event_count");

		List<EventhubPartition> eventhubPartitions = this.getAllEventhubPartitions();
		this.subscribedPartitionsToStartOffsets = new HashMap<>(eventhubPartitions.size());

		if (this.restoreToOffset != null){
			for (EventhubPartition partition : eventhubPartitions){
				if (this.restoreToOffset.containsKey(partition)){
					this.subscribedPartitionsToStartOffsets.put(partition, restoreToOffset.get(partition));
				}
			}

			LOGGER.info("Consumer subtask {} will start reading {} partitions with offsets in restored state: {}",
				getRuntimeContext().getIndexOfThisSubtask(),
				this.subscribedPartitionsToStartOffsets.size(),
				this.subscribedPartitionsToStartOffsets);
		}
		else {
			//If there is no restored state. Then all partitions to read from start, the offset is "-1". In the
			//future eventhub supports specify offset, we modify here
			//We assign partition to each subTask in round robin mode
			int numParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
			int indexofThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
			for (int i = 0; i < eventhubPartitions.size(); i++) {
				if (i % numParallelSubtasks == indexofThisSubtask) {
					this.subscribedPartitionsToStartOffsets.put(eventhubPartitions.get(i), defaultEventhubInitOffset);
				}
			}

			LOGGER.info("Consumer subtask {} will start reading {} partitions with offsets: {}",
				getRuntimeContext().getIndexOfThisSubtask(),
				this.subscribedPartitionsToStartOffsets.size(),
				this.subscribedPartitionsToStartOffsets);
		}
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		if (this.subscribedPartitionsToStartOffsets == null || this.subscribedPartitionsToStartOffsets.size() == 0){
			throw new Exception("The partitions were not set for the consumer");
		}

		StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();

		if (!this.subscribedPartitionsToStartOffsets.isEmpty()){
			final EventFetcher<T> fetcher = new EventFetcher<T>(sourceContext,
				subscribedPartitionsToStartOffsets,
				deserializer,
				periodicWatermarkAssigner,
				punctuatedWatermarkAssigner,
				runtimeContext.getProcessingTimeService(),
				runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
				runtimeContext.getUserCodeClassLoader(),
				runtimeContext.getTaskNameWithSubtasks(),
				eventhubsProps,
				false,
				receivedCount);

			this.eventhubFetcher = fetcher;
			if (!this.running){
				return;
			}

			this.eventhubFetcher.runFetchLoop();
		}
		else {
			sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));

			final Object waitObj = new Object();
			while (this.running){
				try {
					synchronized (waitObj){
						waitObj.wait();
					}
				}
				catch (InterruptedException ex){
					if (this.running){
						Thread.currentThread().interrupt();
					}
				}
			}
		}
	}

	@Override
	public void close() throws Exception {
		try {
			this.cancel();
		}
		finally {
			super.close();
		}
	}

	@Override
	public void cancel() {
		this.running = false;

		if (this.eventhubFetcher != null){
			this.eventhubFetcher.cancel();
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (!this.running){
			LOGGER.info("Consumer subtask {}: snapshotState() is called on the closed source", getRuntimeContext().getIndexOfThisSubtask());
			return;
		}

		this.offsetsStateForCheckpoint.clear();
		final EventFetcher<?> fetcher = this.eventhubFetcher;
		if (fetcher == null){
			for (Map.Entry<EventhubPartition, String> subscribedPartition : this.subscribedPartitionsToStartOffsets.entrySet()){
				this.offsetsStateForCheckpoint.add(Tuple2.of(subscribedPartition.getKey(), subscribedPartition.getValue()));
			}
		}
		else {
			HashMap<EventhubPartition, String> currentOffsets = fetcher.snapshotCurrentState();
			for (Map.Entry<EventhubPartition, String> subscribedPartition : currentOffsets.entrySet()){
				this.offsetsStateForCheckpoint.add(Tuple2.of(subscribedPartition.getKey(), subscribedPartition.getValue()));
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		LOGGER.info("Consumer subtask {}:Start init eventhub offset state", getRuntimeContext().getIndexOfThisSubtask());
		OperatorStateStore stateStore = context.getOperatorStateStore();
       /* this.offsetsStateForCheckpoint = stateStore
                .getListState(new ListStateDescriptor<Tuple2<EventhubPartition, String>>(DEFAULT_OFFSET_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<EventhubPartition, String>>(){})));
*/
		this.offsetsStateForCheckpoint =  stateStore.getSerializableListState(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);
		if (context.isRestored()){
			if (this.restoreToOffset == null){
				this.restoreToOffset = new HashMap<>();
				for (Tuple2<EventhubPartition, String> offsetState : this.offsetsStateForCheckpoint.get()){
					this.restoreToOffset.put(offsetState.f0, offsetState.f1);
				}

				LOGGER.info("Consumer subtask {}:Eventhub offset state is restored from checkpoint", getRuntimeContext().getIndexOfThisSubtask());
			}
			else if (this.restoreToOffset.isEmpty()){
				this.restoreToOffset = null;
			}
		}
		else {
			LOGGER.info("Consumer subtask {}:No restore state for flink-eventhub-consumer", getRuntimeContext().getIndexOfThisSubtask());
		}
	}

	//deprecated for CheckpointedRestoring
	public void restoreState(HashMap<EventhubPartition, String> eventhubPartitionOffsets) throws Exception {
		LOGGER.info("{} (taskIdx={}) restoring offsets from an older version.",
			getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask());

		this.restoreToOffset = eventhubPartitionOffsets;

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("{} (taskIdx={}) restored offsets from an older Flink version: {}",
				getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask(), eventhubPartitionOffsets);
		}
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return this.deserializer.getProducedType();
	}

	public FlinkEventHubConsumer<T> assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks<T> assigner) {
		Preconditions.checkNotNull(assigner);

		if (this.periodicWatermarkAssigner != null) {
			throw new IllegalStateException("A periodic watermark emitter has already been set.");
		}
		try {
			ClosureCleaner.clean(assigner, true);
			this.punctuatedWatermarkAssigner = new SerializedValue<>(assigner);
			return this;
		} catch (Exception e) {
			throw new IllegalArgumentException("The given assigner is not serializable", e);
		}
	}

	public FlinkEventHubConsumer<T> assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks<T> assigner) {
		Preconditions.checkNotNull(assigner);

		if (this.punctuatedWatermarkAssigner != null) {
			throw new IllegalStateException("A punctuated watermark emitter has already been set.");
		}
		try {
			ClosureCleaner.clean(assigner, true);
			this.periodicWatermarkAssigner = new SerializedValue<>(assigner);
			return this;
		} catch (Exception e) {
			throw new IllegalArgumentException("The given assigner is not serializable", e);
		}
	}

	private List<EventhubPartition> getAllEventhubPartitions() {
		List<EventhubPartition> partitions = new ArrayList<>();
		for (int i = 0; i < this.partitionCount; i++){
			partitions.add(new EventhubPartition(this.eventhubsProps, i));
		}

		LOGGER.info("Consumer subtask {}:Create {} eventhub partitions info", getRuntimeContext().getIndexOfThisSubtask(), this.partitionCount);
		return partitions;
	}
}
