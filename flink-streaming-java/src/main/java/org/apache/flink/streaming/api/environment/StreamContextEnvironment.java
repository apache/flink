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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link StreamExecutionEnvironment} that will be used in cases where the CLI client or
 * testing utilities create a {@link StreamExecutionEnvironment} that should be used when
 * {@link StreamExecutionEnvironment#getExecutionEnvironment()} is called.
 */
@PublicEvolving
public class StreamContextEnvironment extends StreamExecutionEnvironment {

	private final ContextEnvironment ctx;

	StreamContextEnvironment(final ContextEnvironment ctx) {
		super(checkNotNull(ctx).getExecutorServiceLoader(), ctx.getConfiguration(), ctx.getUserCodeClassLoader());

		this.ctx = ctx;

		final int parallelism = ctx.getParallelism();
		if (parallelism > 0) {
			setParallelism(parallelism);
		}
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		transformations.clear();

		final JobExecutionResult jobExecutionResult = super.execute(streamGraph);
		ctx.setJobExecutionResult(jobExecutionResult);
		return jobExecutionResult;
	}
}
