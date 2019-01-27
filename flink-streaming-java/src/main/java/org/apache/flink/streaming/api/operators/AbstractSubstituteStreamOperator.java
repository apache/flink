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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * Basic class for all substitute stream operators.
 *
 * <p>For concrete implementations, the following methods must also be implemented:<ul>
 *  <li> {@link StreamOperatorSubstitutor#getActualStreamOperator(ClassLoader)} </li>
 *  <li> {@link StreamOperator#setChainingStrategy(ChainingStrategy)} </li>
 *  <li> {@link StreamOperator#getChainingStrategy()} </li>
 * </ul></p>
 *
 * @param <OUT> output type of the actual stream operator
 */
@PublicEvolving
public interface AbstractSubstituteStreamOperator <OUT> extends StreamOperator<OUT>, StreamOperatorSubstitutor<OUT> {
	@Override
	default void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		throw new UnsupportedOperationException("This method is unsupported in AbstractSubstituteStreamOperator.");
	}

	@Override
	default void open() throws Exception {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void close() throws Exception {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void dispose() throws Exception {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, CheckpointStreamFactory storageLocation) throws Exception {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void initializeState() throws Exception {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void setKeyContextElement1(StreamRecord<?> record) throws Exception {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void setKeyContextElement2(StreamRecord<?> record) throws Exception {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default MetricGroup getMetricGroup() {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default OperatorID getOperatorID() {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void notifyCheckpointComplete(long checkpointId) throws Exception {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default void setCurrentKey(Object key) {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}

	@Override
	default Object getCurrentKey() {
		throw new UnsupportedOperationException("For an AbstractSubstituteStreamOperator, this method should not be called");
	}
}
