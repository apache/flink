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

import org.apache.flink.streaming.util.ClusterUtil;

public class LocalStreamEnvironment extends StreamExecutionEnvironment {

	protected static ClassLoader userClassLoader;

	/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil with a
	 * default name.
	 */
	@Override
	public void execute() throws Exception {
		ClusterUtil.runOnMiniCluster(this.jobGraphBuilder.getJobGraph(), getDegreeOfParallelism());
	}

	/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user
	 * specified name.
	 * 
	 * @param jobName
	 *            name of the job
	 */
	@Override
	public void execute(String jobName) throws Exception {
		ClusterUtil.runOnMiniCluster(this.jobGraphBuilder.getJobGraph(jobName),
				getDegreeOfParallelism());
	}
}
