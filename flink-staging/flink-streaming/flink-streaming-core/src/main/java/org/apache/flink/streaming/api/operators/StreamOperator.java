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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

/**
 * Basic interface for stream operators. Implementers would implement one of
 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator} or
 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators
 * that process elements.
 * 
 * <p>
 * The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator}
 * offers default implementation for the lifecycle and properties methods.
 *
 * <p>
 * Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 * 
 * @param <OUT> The output type of the operator
 */
public interface StreamOperator<OUT> extends Serializable {

	// ------------------------------------------------------------------------
	//  Life Cycle
	// ------------------------------------------------------------------------
	
	/**
	 * Initializes the operator. Sets access to the context and the output.
	 */
	void setup(Output<StreamRecord<OUT>> output, StreamingRuntimeContext runtimeContext);

	/**
	 * This method is called immediately before any elements are processed, it should contain the
	 * operator's initialization logic.
	 * 
	 * @throws java.lang.Exception An exception in this method causes the operator to fail.
	 */
	void open(Configuration config) throws Exception;

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
	//  Context and chaining properties
	// ------------------------------------------------------------------------
	
	/**
	 * Returns a context that allows the operator to query information about the execution and also
	 * to interact with systems such as broadcast variables and managed state. This also allows
	 * to register timers.
	 */
	StreamingRuntimeContext getRuntimeContext();

	/**
	 * An operator can return true here to disable copying of its input elements. This overrides
	 * the object-reuse setting on the {@link org.apache.flink.api.common.ExecutionConfig}
	 */
	boolean isInputCopyingDisabled();

	void setChainingStrategy(ChainingStrategy strategy);

	ChainingStrategy getChainingStrategy();

	/**
	 * Defines the chaining scheme for the operator. By default <b>ALWAYS</b> is used,
	 * which means operators will be eagerly chained whenever possible, for
	 * maximal performance. It is generally a good practice to allow maximal
	 * chaining and increase operator parallelism. </p> When the strategy is set
	 * to <b>NEVER</b>, the operator will not be chained to the preceding or succeeding
	 * operators.</p> <b>HEAD</b> strategy marks a start of a new chain, so that the
	 * operator will not be chained to preceding operators, only succeding ones.
	 *
	 * <b>FORCE_ALWAYS</b> will enable chaining even if chaining is disabled on the execution
	 * environment. This should only be used by system-level operators, not operators implemented
	 * by users.
	 */
	public static enum ChainingStrategy {
		FORCE_ALWAYS, ALWAYS, NEVER, HEAD
	}
}
