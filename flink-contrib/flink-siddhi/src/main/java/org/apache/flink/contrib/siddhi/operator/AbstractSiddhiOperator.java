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

import org.apache.flink.contrib.siddhi.schema.SiddhiStreamSchema;
import org.apache.flink.contrib.siddhi.schema.StreamSchema;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public abstract class AbstractSiddhiOperator<IN,OUT> extends AbstractStreamOperator<OUT> implements OneInputStreamOperator<IN,OUT> {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSiddhiOperator.class);
	protected static final int INITIAL_PRIORITY_QUEUE_CAPACITY = 11;

	private final SiddhiExecutionPlan<OUT> siddhiPlan;
	private final String executionExpression;
	private final boolean isProcessingTime;

	private transient SiddhiManager siddhiManager;
	private transient ExecutionPlanRuntime siddhiRuntime;
	private transient Map<String,InputHandler> inputStreamHandlers;

	// queue to buffer out of order stream records
	private transient PriorityQueue<StreamRecord<IN>> priorityQueue;

	/**
	 * @param siddhiPlan Siddhi CEP  Execution Plan
     */
	public AbstractSiddhiOperator(SiddhiExecutionPlan<OUT> siddhiPlan){
		this.siddhiPlan = siddhiPlan;
		this.executionExpression = siddhiPlan.getFinalExecutionExpression();
		this.isProcessingTime = this.siddhiPlan.getTimeCharacteristic() == TimeCharacteristic.ProcessingTime;
		validate(executionExpression);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		String streamId = getStreamId(element.getValue());
		StreamSchema<IN> schema = siddhiPlan.getInputStreamSchema(streamId);

		if (isProcessingTime) {
			processEvent(streamId,schema,element.getValue(),System.currentTimeMillis());
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

	protected abstract void processEvent(String streamId, StreamSchema<IN> schema,IN value,long timestamp) throws Exception;

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		while(!priorityQueue.isEmpty() && priorityQueue.peek().getTimestamp() <= mark.getTimestamp()) {
			StreamRecord<IN> streamRecord = priorityQueue.poll();
			String streamId = getStreamId(streamRecord.getValue());
			StreamSchema<IN> schema = siddhiPlan.getInputStreamSchema(streamId);
			processEvent(streamId,schema,streamRecord.getValue(),streamRecord.getTimestamp());
		}
		output.emitWatermark(mark);
	}

	public abstract String getStreamId(IN record);

	public PriorityQueue<StreamRecord<IN>> getPriorityQueue(){
		return priorityQueue;
	}

	protected ExecutionPlanRuntime getSiddhiRuntime(){
		return this.siddhiRuntime;
	}

	public InputHandler getSiddhiInputHandler(String streamId){
		return inputStreamHandlers.get(streamId);
	}

	protected SiddhiExecutionPlan<OUT> getSiddhiPlan(){
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
	protected void send(String streamId,Object[] data,long timestamp) throws InterruptedException {
		this.getSiddhiInputHandler(streamId).send(timestamp,data);
	}

	/**
	 * Validate execution plan during building DAG before submitting to execution environment and fail-fast.
	 */
	private static void validate(final String executionPlan){
		SiddhiManager siddhiManager = new SiddhiManager();
		try {
			siddhiManager.validateExecutionPlan(executionPlan);
		} finally {
			siddhiManager.shutdown();
		}
	}

	/**
	 * Create and start execution runtime
     */
	protected void startSiddhiRuntime(){
		if(this.siddhiRuntime == null) {
			this.siddhiManager = new SiddhiManager();
			this.siddhiRuntime = siddhiManager.createExecutionPlanRuntime(executionExpression);
			this.siddhiRuntime.start();
			registerInputAndOutput(this.siddhiRuntime);
			LOGGER.info("Siddhi runtime {} started",siddhiRuntime.getName());
		} else {
			throw new IllegalStateException("Siddhi runtime has already been initialized");
		}
	}

	protected void shutdownSiddhiRuntime(){
		if(this.siddhiRuntime!=null){
			this.siddhiRuntime.shutdown();
//			try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//				//
//			}
			LOGGER.info("Siddhi runtime {} shutdown",this.siddhiRuntime.getName());
			this.siddhiRuntime = null;
			this.siddhiManager.shutdown();
			this.siddhiManager = null;
			this.inputStreamHandlers = null;
		} else {
			throw new IllegalStateException("Siddhi runtime has already shutdown");
		}
	}

	private void registerInputAndOutput(ExecutionPlanRuntime runtime) {
		AbstractDefinition definition = this.siddhiRuntime.getStreamDefinitionMap().get(this.siddhiPlan.getOutputStreamId());
		runtime.addCallback(this.siddhiPlan.getOutputStreamId(), new StreamOutputHandler<>(this.siddhiPlan.getOutputStreamType(),definition,this.output));
		inputStreamHandlers = new HashMap<>();
		for (String inputStreamId:this.siddhiPlan.getInputStreams()){
			inputStreamHandlers.put(inputStreamId,runtime.getInputHandler(inputStreamId));
		}
	}

	@Override
	public void dispose() {
		LOGGER.info("Disposing");
		super.dispose();
		shutdownSiddhiRuntime();
		output.close();
	}

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState =  super.snapshotOperatorState(checkpointId, timestamp);
		final AbstractStateBackend.CheckpointStateOutputStream os = this.getStateBackend().createCheckpointStateOutputStream(
			checkpointId,
			timestamp);
		final AbstractStateBackend.CheckpointStateOutputView ov = new AbstractStateBackend.CheckpointStateOutputView(os);
		byte[] siddhiRuntimeSnapshot = this.siddhiRuntime.snapshot();
		int siddhiRuntimeSnapshotLength = siddhiRuntimeSnapshot.length;
		ov.writeInt(siddhiRuntimeSnapshotLength);
		ov.write(siddhiRuntimeSnapshot,0,siddhiRuntimeSnapshotLength);
		taskState.setOperatorState(os.closeAndGetHandle());
		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState state) throws Exception {
		super.restoreState(state);
		StreamStateHandle stream = (StreamStateHandle)state.getOperatorState();
		final InputStream is = stream.getState(getUserCodeClassloader());
		final DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(is);
		startSiddhiRuntime();
		int siddhiRuntimeSnapshotLength = div.readInt();
		byte[] siddhiRuntimeSnapshot = new byte[siddhiRuntimeSnapshotLength];
		int readLength = div.read(siddhiRuntimeSnapshot,0,siddhiRuntimeSnapshotLength);
		assert readLength == siddhiRuntimeSnapshotLength;
		this.siddhiRuntime.restore(siddhiRuntimeSnapshot);
		div.close();
	}

}
