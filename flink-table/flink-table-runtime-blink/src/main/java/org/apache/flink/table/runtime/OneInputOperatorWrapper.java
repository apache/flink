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

package org.apache.flink.table.runtime;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.generated.GeneratedClass;

/**
 * Wrapper for code gen operator.
 * TODO Remove it after FLINK-11974.
 */
public class OneInputOperatorWrapper<IN, OUT>
		implements OneInputStreamOperator<IN, OUT> {

	private final GeneratedClass<OneInputStreamOperator<IN, OUT>> generatedClass;

	private transient OneInputStreamOperator<IN, OUT> operator;

	public OneInputOperatorWrapper(GeneratedClass<OneInputStreamOperator<IN, OUT>> generatedClass) {
		this.generatedClass = generatedClass;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config,
			Output<StreamRecord<OUT>> output) {
		operator = generatedClass.newInstance(containingTask.getUserCodeClassLoader());
		operator.setup(containingTask, config, output);
	}

	@VisibleForTesting
	public OneInputStreamOperator<IN, OUT> getOperator() {
		return operator;
	}

	@Override
	public void open() throws Exception {
		operator.open();
	}

	@Override
	public void close() throws Exception {
		operator.close();
	}

	@Override
	public void dispose() throws Exception {
		operator.dispose();
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		operator.prepareSnapshotPreBarrier(checkpointId);
	}

	@Override
	public OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp,
			CheckpointOptions checkpointOptions,
			CheckpointStreamFactory storageLocation) throws Exception {
		return operator.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
	}

	@Override
	public void initializeState() throws Exception {
		operator.initializeState();
	}

	@Override
	public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
		operator.setKeyContextElement1(record);
	}

	@Override
	public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
		operator.setKeyContextElement2(record);
	}

	@Override
	public ChainingStrategy getChainingStrategy() {
		return ChainingStrategy.ALWAYS;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {
		operator.setChainingStrategy(strategy);
	}

	@Override
	public MetricGroup getMetricGroup() {
		return operator.getMetricGroup();
	}

	@Override
	public OperatorID getOperatorID() {
		return operator.getOperatorID();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		operator.processElement(element);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		operator.processWatermark(mark);
	}

	@Override
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		operator.processLatencyMarker(latencyMarker);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		operator.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public void setCurrentKey(Object key) {
		operator.setCurrentKey(key);
	}

	@Override
	public Object getCurrentKey() {
		return operator.getCurrentKey();
	}
}
