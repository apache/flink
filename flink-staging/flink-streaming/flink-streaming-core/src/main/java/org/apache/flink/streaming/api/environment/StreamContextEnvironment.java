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

import org.apache.flink.client.program.Client;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;

public class StreamContextEnvironment extends StreamExecutionEnvironment {

	protected static ClassLoader userClassLoader;
	protected List<File> jars;
	protected Client client;

	protected StreamContextEnvironment(Client client, List<File> jars, int parallelism) {
		this.client = client;
		this.jars = jars;
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
	public void execute() throws Exception {
		execute(null);
	}

	@Override
	public void execute(String jobName) throws Exception {
		currentEnvironment = null;

		JobGraph jobGraph;
		if (jobName == null) {
			jobGraph = this.streamGraph.getJobGraph();
		} else {
			jobGraph = this.streamGraph.getJobGraph(jobName);
		}

		for (File file : jars) {
			jobGraph.addJar(new Path(file.getAbsolutePath()));
		}

		try {
			client.run(jobGraph, true);

		} catch (Exception e) {
			throw e;
		}

	}

}
