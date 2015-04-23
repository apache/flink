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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.util.ClusterUtil;

public class LocalStreamEnvironment extends StreamExecutionEnvironment {

	/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil with a
	 * default name.
	 *
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Override
	public JobExecutionResult execute() throws Exception {
		return execute(DEFAULT_JOB_NAME);
	}

	/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user
	 * specified name.
	 * 
	 * @param jobName
	 *            name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		JobExecutionResult result = ClusterUtil.runOnMiniCluster(this.streamGraph.getJobGraph(jobName), getParallelism(),
				getConfig().isSysoutLoggingEnabled());
		streamGraph.clear(); // clear graph to allow submitting another job via the same environment.
		return result;
	}
}
