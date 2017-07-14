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

package org.apache.flink.api.java;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.CollectionExecutor;

/**
 * Version of {@link ExecutionEnvironment} that allows serial, local, collection-based executions of Flink programs.
 */
@PublicEvolving
public class CollectionEnvironment extends ExecutionEnvironment {

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan p = createProgramPlan(jobName);

		// We need to reverse here. Object-Reuse enabled, means safe mode is disabled.
		CollectionExecutor exec = new CollectionExecutor(getConfig());
		this.lastJobExecutionResult = exec.execute(p);
		return this.lastJobExecutionResult;
	}

	@Override
	public int getParallelism() {
		return 1; // always serial
	}

	@Override
	public String getExecutionPlan() throws Exception {
		throw new UnsupportedOperationException("Execution plans are not used for collection-based execution.");
	}

	@Override
	public void startNewSession() throws Exception {
	}
}
