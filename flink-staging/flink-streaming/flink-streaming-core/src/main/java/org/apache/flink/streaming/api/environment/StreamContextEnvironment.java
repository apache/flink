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

import java.io.File;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.Client;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamContextEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(StreamContextEnvironment.class);

	protected List<File> jars;
	protected Client client;
	private final boolean wait;

	protected StreamContextEnvironment(Client client, List<File> jars, int parallelism, boolean wait) {
		this.client = client;
		this.jars = jars;
		this.wait = wait;
		if (parallelism > 0) {
			setParallelism(parallelism);
		} else {
			// first check for old parallelism config key
			setParallelism(GlobalConfiguration.getInteger(
					ConfigConstants.DEFAULT_PARALLELISM_KEY_OLD,
					ConfigConstants.DEFAULT_PARALLELISM));
			// then for new
			setParallelism(GlobalConfiguration.getInteger(
					ConfigConstants.DEFAULT_PARALLELISM_KEY,
					getParallelism()));
		}
	}

	@Override
	public JobExecutionResult execute() throws Exception {
		return execute(null);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {

		JobGraph jobGraph;
		if (jobName == null) {
			jobGraph = this.getStreamGraph().getJobGraph();
		} else {
			jobGraph = this.getStreamGraph().getJobGraph(jobName);
		}

		transformations.clear();

		for (File file : jars) {
			jobGraph.addJar(new Path(file.getAbsolutePath()));
		}

		JobSubmissionResult result = client.run(jobGraph, wait);
		if(result instanceof JobExecutionResult) {
			return (JobExecutionResult) result;
		} else {
			LOG.warn("The Client didn't return a JobExecutionResult");
			return new JobExecutionResult(result.getJobID(), -1, null);
		}
	}
}
