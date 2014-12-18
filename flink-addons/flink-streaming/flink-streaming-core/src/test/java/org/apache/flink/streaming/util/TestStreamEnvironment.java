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

package org.apache.flink.streaming.util;

import akka.actor.ActorRef;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

public class TestStreamEnvironment extends StreamExecutionEnvironment {
	private static final String DEFAULT_JOBNAME = "TestStreamingJob";
	private static final String CANNOT_EXECUTE_EMPTY_JOB = "Cannot execute empty job";

	private long memorySize;

	public TestStreamEnvironment(int degreeOfParallelism, long memorySize){
		this.setDegreeOfParallelism(degreeOfParallelism);

		this.memorySize = memorySize;
	}

	@Override
	public void execute() throws Exception {
		execute(DEFAULT_JOBNAME);
	}

	@Override
	public void execute(String jobName) throws Exception {
		JobGraph jobGraph = jobGraphBuilder.getJobGraph(jobName);

		Configuration configuration = jobGraph.getJobConfiguration();

		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS,
				getDegreeOfParallelism());
		configuration.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize);

		ForkableFlinkMiniCluster cluster = new ForkableFlinkMiniCluster(configuration);

		try{
			ActorRef client = cluster.getJobClient();
			JobClient.submitJobAndWait(jobGraph, false, client, cluster.timeout());
		}catch(JobExecutionException e){
			if(e.getMessage().contains("GraphConversionException")){
				throw new Exception(CANNOT_EXECUTE_EMPTY_JOB, e);
			}else{
				throw e;
			}
		}finally{
			cluster.stop();
		}
	}
}
