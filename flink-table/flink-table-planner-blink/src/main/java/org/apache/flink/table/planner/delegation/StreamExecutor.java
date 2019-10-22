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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.delegation.Executor;

import java.util.List;

/**
 * An implementation of {@link Executor} that is backed by a {@link StreamExecutionEnvironment}.
 * This is the only executor that {@link org.apache.flink.table.planner.delegation.StreamPlanner} supports.
 */
@Internal
public class StreamExecutor extends ExecutorBase {

	@VisibleForTesting
	public StreamExecutor(StreamExecutionEnvironment executionEnvironment) {
		super(executionEnvironment);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		StreamExecutionEnvironment execEnv = getExecutionEnvironment();
		return execEnv.execute(generateStreamGraph(transformations, jobName));
	}

	@Override
	public StreamGraph generateStreamGraph(List<Transformation<?>> transformations, String jobName) {
		transformations.forEach(getExecutionEnvironment()::addOperator);
		return getExecutionEnvironment().getStreamGraph(getNonEmptyJobName(jobName));
	}

}
