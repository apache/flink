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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * Basic interface for stream operators. Implementers would implement one of
 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator} or
 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators
 * that process elements. You can use
 * {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class for
 * custom operators.
 * 
 * @param <OUT> The output type of the operator
 */
public interface StreamOperator<OUT> extends Serializable {

	/**
	 * Initializes the {@link StreamOperator} for input and output handling.
	 */
	public void setup(Output<OUT> output, RuntimeContext runtimeContext);

	/**
	 * This method is called before any elements are processed.
	 */
	public void open(Configuration config) throws Exception;

	/**
	 * This method is called after no more elements for can arrive for processing.
	 */
	public void close() throws Exception;

	public void setChainingStrategy(ChainingStrategy strategy);

	public ChainingStrategy getChainingStrategy();

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
