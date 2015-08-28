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

package org.apache.flink.tez.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.tez.dag.api.TezConfiguration;


public class TezExecutorTool extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(TezExecutorTool.class);

	private TezExecutor executor;
	Plan plan;
	private Path jarPath = null;

	public TezExecutorTool(TezExecutor executor, Plan plan) {
		this.executor = executor;
		this.plan = plan;
	}

	public void setJobJar (Path jarPath) {
		this.jarPath = jarPath;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		TezConfiguration tezConf;
		if (conf != null) {
			tezConf = new TezConfiguration(conf);
		} else {
			tezConf = new TezConfiguration();
		}

		UserGroupInformation.setConfiguration(tezConf);

		executor.setConfiguration(tezConf);

		try {
			if (jarPath != null) {
				executor.setJobJar(jarPath);
			}
			JobExecutionResult result = executor.executePlan(plan);
		}
		catch (Exception e) {
			LOG.error("Job execution failed due to: " + e.getMessage());
			throw new RuntimeException(e.getMessage());
		}
		return 0;
	}


}
