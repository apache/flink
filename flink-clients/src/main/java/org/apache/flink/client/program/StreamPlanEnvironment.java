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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * An {@link ExecutionEnvironment} that never executes a job but only extracts the {@link Pipeline}.
 */
public class StreamPlanEnvironment extends StreamExecutionEnvironment {

	private StreamPlanEnvironment(int parallelism) {
		if (parallelism > 0) {
			setParallelism(parallelism);
		}
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		throw new ProgramAbortException(getStreamGraph(jobName));
	}

	public static void setAsContext(int parallelism) {
		StreamExecutionEnvironmentFactory factory = () -> new StreamPlanEnvironment(parallelism);
		initializeContextEnvironment(factory);
	}

	public static void unsetAsContext() {
		resetContextEnvironment();
	}
}
