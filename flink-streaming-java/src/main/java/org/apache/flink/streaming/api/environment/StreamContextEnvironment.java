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
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;

/**
 * Special {@link StreamExecutionEnvironment} that will be used in cases where the CLI client or
 * testing utilities create a {@link StreamExecutionEnvironment} that should be used when
 * {@link StreamExecutionEnvironment#getExecutionEnvironment()} is called.
 */
@PublicEvolving
public class StreamContextEnvironment extends StreamExecutionEnvironment {

	private final ContextEnvironment ctx;

	protected StreamContextEnvironment(ContextEnvironment ctx) {
		this.ctx = ctx;
		if (ctx.getParallelism() > 0) {
			setParallelism(ctx.getParallelism());
		}
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		transformations.clear();

		JobGraph jobGraph = FlinkPipelineTranslationUtil.getJobGraph(
				streamGraph,
				ctx.getClient().getFlinkConfiguration(),
				getParallelism());

		ClientUtils.addJarFiles(jobGraph, ctx.getJars());
		jobGraph.setClasspaths(ctx.getClasspaths());

		// running from the CLI will override the savepoint restore settings
		jobGraph.setSavepointRestoreSettings(ctx.getSavepointRestoreSettings());

		JobExecutionResult jobExecutionResult;
		if (ctx.isDetached()) {
			jobExecutionResult = ClientUtils.submitJob(ctx.getClient(), jobGraph);
		} else {
			jobExecutionResult = ClientUtils.submitJobAndWaitForResult(ctx.getClient(), jobGraph, ctx.getUserCodeClassLoader());
		}

		ctx.setJobExecutionResult(jobExecutionResult);

		return jobExecutionResult;
	}
}
