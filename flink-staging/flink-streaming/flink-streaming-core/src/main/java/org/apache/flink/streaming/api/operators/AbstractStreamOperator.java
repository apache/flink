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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * Base class for operators that do not contain a user-defined function.
 * 
 * @param <OUT> The output type of the operator
 */
public abstract class AbstractStreamOperator<OUT> implements StreamOperator<OUT> {

	private static final long serialVersionUID = 1L;

	protected transient RuntimeContext runtimeContext;

	protected transient ExecutionConfig executionConfig;

	public transient Output<OUT> output;

	// A sane default for most operators
	protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

	@Override
	public final void setup(Output<OUT> output, RuntimeContext runtimeContext) {
		this.output = output;
		this.executionConfig = runtimeContext.getExecutionConfig();
		this.runtimeContext = runtimeContext;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		this.chainingStrategy = strategy;
	}

	@Override
	public final ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}
}
