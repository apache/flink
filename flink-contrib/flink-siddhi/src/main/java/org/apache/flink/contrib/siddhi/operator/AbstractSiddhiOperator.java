/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.contrib.siddhi.exception.UndefinedStreamException;
import org.apache.flink.contrib.siddhi.schema.StreamSchema;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * <h1>Siddhi Runtime Operator</h1>
 *
 * A flink Stream Operator to integrate with native siddhi execution runtime, extension and type schema mechanism/
 *
 * <ul>
 * <li>
 * Create Siddhi {@link ExecutionPlanRuntime} according predefined execution plan and integrate with Flink Stream Operator lifecycle.
 * </li>
 * <li>
 * Connect Flink DataStreams with predefined Siddhi Stream according to unique streamId
 * </li>
 * <li>
 * Convert native {@link StreamRecord} to Siddhi {@link org.wso2.siddhi.core.event.Event} according to {@link StreamSchema}, and send to Siddhi Runtime.
 * </li>
 * <li>
 * Listen output callback event and convert as expected output type according to output {@link org.apache.flink.api.common.typeinfo.TypeInformation}, then output as typed DataStream.
 * </li>
 * </li>
 * <li>
 * Integrate siddhi runtime state management with Flink state (See `AbstractSiddhiOperator`)
 * </li>
 * <li>
 * Support siddhi plugin management to extend CEP functions. (See `SiddhiCEP#registerExtension`)
 * </li>
 * </ul>
 *
 * @param <IN>  Input Element Type
 * @param <OUT> Output Element Type
 */
public abstract class AbstractSiddhiOperator<IN, OUT> extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<IN, OUT> {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSiddhiOperator.class);
	private static final int INITIAL_PRIORITY_QUEUE_CAPACITY = 11;

	private final SiddhiOperatorContext siddhiPlan;
	private final String executionExpression;
	private final boolean isProcessingTime;
	private final Map<String, MultiplexingStreamRecordSerializer<IN>> streamRecordSerializers;

	private transient SiddhiManager siddhiManager;
	private transient ExecutionPlanRuntime siddhiRuntime;
	private transient Map<String, InputHandler> inputStreamHandlers;

	// queue to buffer out of order stream records
	private transient PriorityQueue<StreamRecord<IN>> priorityQueue;

	/**
	 * @param siddhiPlan Siddhi CEP  Execution Plan
	 */
	public AbstractSiddhiOperator(SiddhiOperatorContext siddhiPlan) {
		validate(siddhiPlan);
		this.executionExpression = siddhiPlan.getFinalExecutionPlan();
		this.siddhiPlan = siddhiPlan;
		this.isProcessingTime = this.siddhiPlan.getTimeCharacteristic() == TimeCharacteristic.ProcessingTime;
		this.streamRecordSerializers = new HashMap<>();

		registerStreamRecordSerializers();
	}

	/**
	 * Register StreamRecordSerializer based on {@link StreamSchema}
	 */
	private void registerStreamRecordSerializers() {
		for (String streamId : this.siddhiPlan.getInputStreams()) {
			streamRecordSerializers.put(streamId, createStreamRecordSerializer(this.siddhiPlan.getInputStreamSchema(streamId), this.siddhiPlan.getExecutionConfig()));
		}
	}

	protected abstract MultiplexingStreamRecordSerializer<IN> createStreamRecordSerializer(StreamSchema streamSchema, ExecutionConfig executionConfig);

	protected MultiplexingStreamRecordSerializer<IN> getStreamRecordSerializer(String streamId) {
		if (streamRecordSerializers.containsKey(streamId)) {
			return streamRecordSerializers.get(streamId);
		} else {
			throw new UndefinedStreamException("Stream " + streamId + " not defined");
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		String streamId = getStreamId(element.getValue());
		StreamSchema<IN> schema = siddhiPlan.getInputStreamSchema(streamId);

		if (isProcessingTime) {
			processEvent(streamId, schema, element.getValue(), System.currentTimeMillis());
		} else {
			PriorityQueue<StreamRecord<IN>> priorityQueue = getPriorityQueue();
			// event time processing
			// we have to buffer the elements until we receive the proper watermark
			if (getExecutionConfig().isObjectReuseEnabled()) {
				// copy the StreamRecord so that it cannot be changed
				priorityQueue.offer(new StreamRecord<>(schema.getTypeSerializer().copy(element.getValue()), element.getTimestamp()));
			} else {
				priorityQueue.offer(element);
			}
		}
	}

	protected abstract void processEvent(String streamId, StreamSchema<IN> schema, IN value, long timestamp) throws Exception;

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		while (!priorityQueue.isEmpty() && priorityQueue.peek().getTimestamp() <= mark.getTimestamp()) {
			StreamRecord<IN> streamRecord = priorityQueue.poll();
			String streamId = getStreamId(streamRecord.getValue());
			long timestamp = streamRecord.getTimestamp();
			StreamSchema<IN> schema = siddhiPlan.getInputStreamSchema(streamId);
			processEvent(streamId, schema, streamRecord.getValue(), timestamp);
		}
		output.emitWatermark(mark);
	}

	public abstract String getStreamId(IN record);

	public PriorityQueue<StreamRecord<IN>> getPriorityQueue() {
		return priorityQueue;
	}

	protected ExecutionPlanRuntime getSiddhiRuntime() {
		return this.siddhiRuntime;
	}

	public InputHandler getSiddhiInputHandler(String streamId) {
		return inputStreamHandlers.get(streamId);
	}

	protected SiddhiOperatorContext getSiddhiPlan() {
		return this.siddhiPlan;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		startSiddhiRuntime();
	}

	@Override
	public void open() throws Exception {
		if (priorityQueue == null) {
			priorityQueue = new PriorityQueue<>(INITIAL_PRIORITY_QUEUE_CAPACITY, new StreamRecordComparator<IN>());
		}
		super.open();
	}

	/**
	 * Send input data to siddhi runtime
	 */
	protected void send(String streamId, Object[] data, long timestamp) throws InterruptedException {
		this.getSiddhiInputHandler(streamId).send(timestamp, data);
	}

	/**
	 * Validate execution plan during building DAG before submitting to execution environment and fail-fast.
	 */
	private static void validate(final SiddhiOperatorContext siddhiPlan) {
		SiddhiManager siddhiManager = siddhiPlan.createSiddhiManager();
		try {
			siddhiManager.validateExecutionPlan(siddhiPlan.getFinalExecutionPlan());
		} finally {
			siddhiManager.shutdown();
		}
	}

	/**
	 * Create and start execution runtime
	 */
	private void startSiddhiRuntime() {
		if (this.siddhiRuntime == null) {
			this.siddhiManager = this.siddhiPlan.createSiddhiManager();
			for (Map.Entry<String, Class<?>> entry : this.siddhiPlan.getExtensions().entrySet()) {
				this.siddhiManager.setExtension(entry.getKey(), entry.getValue());
			}
			this.siddhiRuntime = siddhiManager.createExecutionPlanRuntime(executionExpression);
			this.siddhiRuntime.start();
			registerInputAndOutput(this.siddhiRuntime);
			LOGGER.info("Siddhi runtime {} started", siddhiRuntime.getName());
		} else {
			throw new IllegalStateException("Siddhi runtime has already been initialized");
		}
	}

	private void shutdownSiddhiRuntime() {
		if (this.siddhiRuntime != null) {
			this.siddhiRuntime.shutdown();
			LOGGER.info("Siddhi runtime {} shutdown", this.siddhiRuntime.getName());
			this.siddhiRuntime = null;
			this.siddhiManager.shutdown();
			this.siddhiManager = null;
			this.inputStreamHandlers = null;
		} else {
			throw new IllegalStateException("Siddhi runtime has already shutdown");
		}
	}

	@SuppressWarnings("unchecked")
	private void registerInputAndOutput(ExecutionPlanRuntime runtime) {
		AbstractDefinition definition = this.siddhiRuntime.getStreamDefinitionMap().get(this.siddhiPlan.getOutputStreamId());
		runtime.addCallback(this.siddhiPlan.getOutputStreamId(), new StreamOutputHandler<>(this.siddhiPlan.getOutputStreamType(), definition, this.output));
		inputStreamHandlers = new HashMap<>();
		for (String inputStreamId : this.siddhiPlan.getInputStreams()) {
			inputStreamHandlers.put(inputStreamId, runtime.getInputHandler(inputStreamId));
		}
	}

	@Override
	public void dispose() throws Exception {
		LOGGER.info("Disposing");
		super.dispose();
		shutdownSiddhiRuntime();
		output.close();
	}

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		super.snapshotState(out, checkpointId, timestamp);
		final ObjectOutputStream oos = new ObjectOutputStream(out);

		// Write siddhi snapshot
		byte[] siddhiRuntimeSnapshot = this.siddhiRuntime.snapshot();
		int siddhiRuntimeSnapshotLength = siddhiRuntimeSnapshot.length;
		oos.writeInt(siddhiRuntimeSnapshotLength);
		out.write(siddhiRuntimeSnapshot, 0, siddhiRuntimeSnapshotLength);

		// Write queue buffer snapshot
		this.snapshotQueuerState(this.priorityQueue, new DataOutputViewStreamWrapper(oos));

		oos.flush();
	}

	@Override
	public void restoreState(FSDataInputStream state) throws Exception {
		super.restoreState(state);
		final ObjectInputStream ois = new ObjectInputStream(state);

		// Restore siddhi snapshot
		startSiddhiRuntime();
		int siddhiRuntimeSnapshotLength = ois.readInt();
		byte[] siddhiRuntimeSnapshot = new byte[siddhiRuntimeSnapshotLength];
		int readLength = ois.read(siddhiRuntimeSnapshot, 0, siddhiRuntimeSnapshotLength);
		assert readLength == siddhiRuntimeSnapshotLength;
		this.siddhiRuntime.restore(siddhiRuntimeSnapshot);

		// Restore queue buffer snapshot
		this.priorityQueue = restoreQueuerState(new DataInputViewStreamWrapper(ois));
	}

	protected abstract void snapshotQueuerState(PriorityQueue<StreamRecord<IN>> queue,
												DataOutputView dataOutputView) throws IOException;

	protected abstract PriorityQueue<StreamRecord<IN>> restoreQueuerState(DataInputView dataInputView) throws IOException;
}
