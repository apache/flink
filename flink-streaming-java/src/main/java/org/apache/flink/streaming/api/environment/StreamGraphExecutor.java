/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.Executor;
import org.apache.flink.api.common.ExecutorFactory;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.graph.StreamGraph;

/**
 * A StreamGraphExecutor executes a StreamGraph.
 *
 * <p>The specific implementation (such as the org.apache.flink.client.LocalExecutor
 * and org.apache.flink.client.RemoteExecutor) is created by {@link ExecutorFactory}</p>
 *
 */
@Internal
public interface StreamGraphExecutor extends Executor {

	/**
	 * Execute the given program.
	 *
	 * <p>If the executor has not been started before, then this method will start the
	 * executor and stop it after the execution has completed. This implies that one needs
	 * to explicitly start the executor for all programs where multiple dataflow parts
	 * depend on each other. Otherwise, the previous parts will no longer
	 * be available, because the executor immediately shut down after the execution.</p>
	 *
	 * @param streamGraph The streamGraph to execute.
	 * @return The execution result, containing for example the net runtime of the program, and the accumulators.
	 *
	 * @throws Exception Thrown, if job submission caused an exception.
	 */
	JobExecutionResult executeStreamGraph(StreamGraph streamGraph) throws Exception;
}
