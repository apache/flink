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
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.RunnableFuture;

/**
 * Basic interface for stream operators. Implementers would implement one of
 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator} or
 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators
 * that process elements.
 * 
 * <p> The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator}
 * offers default implementation for the lifecycle and properties methods.
 *
 * <p> Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 * 
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface StreamOperator<OUT> extends Serializable {
	
	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------
	
	/**
	 * Initializes the operator. Sets access to the context and the output.
	 */
	void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output);

	/**
	 * This method is called immediately before any elements are processed, it should contain the
	 * operator's initialization logic.
	 * 
	 * @throws java.lang.Exception An exception in this method causes the operator to fail.
	 */
	void open() throws Exception;

	/**
	 * This method is called after all records have been added to the operators via the methods
	 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator#processElement(StreamRecord)}, or
	 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement1(StreamRecord)} and
	 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement2(StreamRecord)}.

	 * <p>
	 * The method is expected to flush all remaining buffered data. Exceptions during this flushing
	 * of buffered should be propagated, in order to cause the operation to be recognized as failed,
	 * because the last data items are not processed properly.
	 * 
	 * @throws java.lang.Exception An exception in this method causes the operator to fail.
	 */
	void close() throws Exception;

	/**
	 * This method is called at the very end of the operator's life, both in the case of a successful
	 * completion of the operation, and in the case of a failure and canceling.
	 * 
	 * This method is expected to make a thorough effort to release all resources
	 * that the operator has acquired.
	 */
	void dispose() throws Exception;

	// ------------------------------------------------------------------------
	//  state snapshots
	// ------------------------------------------------------------------------

	/**
	 * Called to draw a state snapshot from the operator.
	 *
	 * @throws Exception Forwards exceptions that occur while preparing for the snapshot
	 */

	/**
	 * Called to draw a state snapshot from the operator.
	 *
	 * @return a runnable future to the state handle that points to the snapshotted state. For synchronous implementations,
	 * the runnable might already be finished.
	 * @throws Exception exception that happened during snapshotting.
	 */
	RunnableFuture<OperatorStateHandle> snapshotState(
			long checkpointId, long timestamp, CheckpointStreamFactory streamFactory) throws Exception;

	/**
	 * Provides state handles to restore the operator state.
	 *
	 * @param stateHandles state handles to the operator state.
	 */
	void restoreState(Collection<OperatorStateHandle> stateHandles);

	/**
	 * Called when the checkpoint with the given ID is completed and acknowledged on the JobManager.
	 *
	 * @param checkpointId The ID of the checkpoint that has been completed.
	 *
	 * @throws Exception Exceptions during checkpoint acknowledgement may be forwarded and will cause
	 *                   the program to fail and enter recovery.
	 */
	void notifyOfCompletedCheckpoint(long checkpointId) throws Exception;

	// ------------------------------------------------------------------------
	//  miscellaneous
	// ------------------------------------------------------------------------
	
	void setKeyContextElement1(StreamRecord<?> record) throws Exception;

	void setKeyContextElement2(StreamRecord<?> record) throws Exception;

	ChainingStrategy getChainingStrategy();

	void setChainingStrategy(ChainingStrategy strategy);
	
	MetricGroup getMetricGroup();
}
