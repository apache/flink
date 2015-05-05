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

package org.apache.flink.test.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;

public class CollectionTestEnvironment extends CollectionEnvironment {

	@Override
	public JobExecutionResult execute() throws Exception {
		return execute("test job");
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		JobExecutionResult result = super.execute(jobName);
		this.lastJobExecutionResult = result;
		return result;
	}

	protected void setAsContext() {
		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				return CollectionTestEnvironment.this;
			}
		};

		initializeContextEnvironment(factory);
	}
}
