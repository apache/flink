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

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public class RestartCallback {
	private static final Logger LOG = LoggerFactory.getLogger(RestartCallback.class);

	private final ExecutionGraph executionGraph;
	private final long expectedGlobalModVersion;

	public RestartCallback(ExecutionGraph executionGraph, long expectedGlobalModVersion) {
		this.executionGraph = executionGraph;
		this.expectedGlobalModVersion = expectedGlobalModVersion;
	}

	public void onRestart() {
		long currentModVersion = executionGraph.getGlobalModVersion();
		if (currentModVersion == expectedGlobalModVersion) {
			executionGraph.restart();
		} else {
			LOG.warn("Restart will be ignored for current version[" + currentModVersion + "] != expectedVersion[" + expectedGlobalModVersion + "]");
		}
	}

	public Executor getFutureExecutor() {
		return executionGraph.getFutureExecutor();
	}
}
