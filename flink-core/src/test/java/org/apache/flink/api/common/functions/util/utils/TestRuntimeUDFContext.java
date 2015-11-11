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

package org.apache.flink.api.common.functions.util.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskRuntimeInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.core.fs.Path;

import java.util.HashMap;
import java.util.concurrent.Future;

public class TestRuntimeUDFContext {

	public static RuntimeUDFContext instance(String name, int subTask, int parallelism, ClassLoader classLoader) {
		return new RuntimeUDFContext(new TaskRuntimeInfo(name, subTask, parallelism, 1),
				classLoader, new ExecutionConfig(), new HashMap<String, Future<Path>>(), new HashMap<String, Accumulator<?, ?>>());
	}

	public static RuntimeUDFContext instance(String name, int subTask, int parallelism, ClassLoader classLoader, ExecutionConfig config) {
		return new RuntimeUDFContext(new TaskRuntimeInfo(name, subTask, parallelism, 1),
				classLoader, config, new HashMap<String, Future<Path>>(), new HashMap<String, Accumulator<?, ?>>());
	}
}
