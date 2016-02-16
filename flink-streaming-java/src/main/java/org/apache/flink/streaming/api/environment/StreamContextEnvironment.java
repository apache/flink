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

import com.google.common.base.Preconditions;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.DetachedEnvironment;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PublicEvolving
public class StreamContextEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(StreamContextEnvironment.class);

	private final ContextEnvironment ctx;

	protected StreamContextEnvironment(ContextEnvironment ctx) {
		this.ctx = ctx;
		if (ctx.getParallelism() > 0) {
			setParallelism(ctx.getParallelism());
		} else {
			setParallelism(GlobalConfiguration.getInteger(
					ConfigConstants.DEFAULT_PARALLELISM_KEY,
					ConfigConstants.DEFAULT_PARALLELISM));
		}
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Preconditions.checkNotNull("Streaming Job name should not be null.");

		StreamGraph streamGraph = this.getStreamGraph();
		streamGraph.setJobName(jobName);

		transformations.clear();

		// execute the programs
		if (ctx instanceof DetachedEnvironment) {
			LOG.warn("Job was executed in detached mode, the results will be available on completion.");
			((DetachedEnvironment) ctx).setDetachedPlan(streamGraph);
			return DetachedEnvironment.DetachedJobExecutionResult.INSTANCE;
		} else {
			return ctx.getClient().runBlocking(streamGraph, ctx.getJars(), ctx.getClasspaths(), ctx.getUserCodeClassLoader(), ctx.getSavepointPath());
		}
	}
}
