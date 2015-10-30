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

import java.net.URL;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;

import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamContextEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(StreamContextEnvironment.class);

	private final List<URL> jars;

	private final List<URL> classpaths;
	
	private final Client client;

	private final ClassLoader userCodeClassLoader;
	
	private final boolean wait;

	protected StreamContextEnvironment(Client client, List<URL> jars, List<URL> classpaths, int parallelism,
			boolean wait) {
		this.client = client;
		this.jars = jars;
		this.classpaths = classpaths;
		this.wait = wait;
		
		this.userCodeClassLoader = JobWithJars.buildUserCodeClassLoader(jars, classpaths,
				getClass().getClassLoader());
		
		if (parallelism > 0) {
			setParallelism(parallelism);
		}
		else {
			// determine parallelism
			setParallelism(GlobalConfiguration.getInteger(
					ConfigConstants.DEFAULT_PARALLELISM_KEY,
					ConfigConstants.DEFAULT_PARALLELISM));
		}
	}

	@Override
	public JobExecutionResult execute() throws Exception {
		return execute(DEFAULT_JOB_NAME);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Preconditions.checkNotNull("Streaming Job name should not be null.");

		StreamGraph streamGraph = this.getStreamGraph();
		streamGraph.setJobName(jobName);

		transformations.clear();

		// execute the programs
		if (wait) {
			return client.runBlocking(streamGraph, jars, classpaths, userCodeClassLoader);
		} else {
			JobSubmissionResult result = client.runDetached(streamGraph, jars, classpaths, userCodeClassLoader);
			LOG.warn("Job was executed in detached mode, the results will be available on completion.");
			return JobExecutionResult.fromJobSubmissionResult(result);
		}
	}
}
