/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Session of Machine Learning algorithms.
 */
public class MLSession {

	public static Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().create();
	private static ExecutionEnvironment env;
	private static StreamExecutionEnvironment streamEnv;
	private static BatchTableEnvironment batchTableEnv;
	private static StreamTableEnvironment streamTableEnv;

	public static ExecutionEnvironment getExecutionEnvironment() {
		if (null == env) {
			if (ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
				Configuration conf = new Configuration();
				conf.setBoolean("taskmanager.memory.preallocate", true);
				conf.setBoolean("taskmanager.memory.off-heap", true);
				conf.setFloat("taskmanager.memory.fraction", 0.3f);
				env = ExecutionEnvironment.createLocalEnvironment(conf);
				env.setParallelism(Runtime.getRuntime().availableProcessors());
			} else {
				env = ExecutionEnvironment.getExecutionEnvironment();
			}
		}
		return env;
	}

	public static StreamExecutionEnvironment getStreamExecutionEnvironment() {
		if (null == streamEnv) {
			streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		}
		return streamEnv;
	}

	public static BatchTableEnvironment getBatchTableEnvironment() {
		if (null == batchTableEnv) {
			batchTableEnv = BatchTableEnvironment.create(getExecutionEnvironment());
		}
		return batchTableEnv;
	}

	public static StreamTableEnvironment getStreamTableEnvironment() {
		if (null == streamTableEnv) {
			streamTableEnv = StreamTableEnvironment.create(getStreamExecutionEnvironment());
		}
		return streamTableEnv;
	}

}

