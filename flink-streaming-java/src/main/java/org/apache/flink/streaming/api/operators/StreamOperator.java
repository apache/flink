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

import java.io.Serializable;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

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
	 * of buffered should be propagated, in order to cause the operation to be recognized asa failed,
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
	void dispose();

	// ------------------------------------------------------------------------
	//  state snapshots
	// ------------------------------------------------------------------------

	/**
	 * Called to draw a state snapshot from the operator. This method snapshots the operator state
	 * (if the operator is stateful) and the key/value state (if it is being used and has been
	 * initialized).
	 *
	 * @param checkpointId The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 *
	 * @return The StreamTaskState object, possibly containing the snapshots for the
	 *         operator and key/value state.
	 *
	 * @throws Exception Forwards exceptions that occur while drawing snapshots from the operator
	 *                   and the key/value state.
	 */
	StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception;
	
	/**
	 * Restores the operator state, if this operator's execution is recovering from a checkpoint.
	 * This method restores the operator state (if the operator is stateful) and the key/value state
	 * (if it had been used and was initialized when the snapshot occurred).
	 *
	 * <p>This method is called after {@link #setup(StreamTask, StreamConfig, Output)}
	 * and before {@link #open()}.
	 *
	 * @param state The state of operator that was snapshotted as part of checkpoint
	 *              from which the execution is restored.
	 * 
	 * @param recoveryTimestamp Global recovery timestamp
	 *
	 * @throws Exception Exceptions during state restore should be forwarded, so that the system can
	 *                   properly react to failed state restore and fail the execution attempt.
	 */
	void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception;

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
	
	void setKeyContextElement(StreamRecord<?> record) throws Exception;
	
	/**
	 * An operator can return true here to disable copying of its input elements. This overrides
	 * the object-reuse setting on the {@link org.apache.flink.api.common.ExecutionConfig}
	 */
	boolean isInputCopyingDisabled();
	
	ChainingStrategy getChainingStrategy();

	void setChainingStrategy(ChainingStrategy strategy);
}
