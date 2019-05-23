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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;

import java.util.Collections;
import java.util.Map;

/**
 * Tests for {@link TaskManagerMetricsHandler}.
 */
public class TaskManagerMetricsHandlerTest extends
	MetricsHandlerTestBase<TaskManagerMetricsHandler> {

	private static final String TEST_TASK_MANAGER_ID = new InstanceID().toString();

	@Override
	TaskManagerMetricsHandler getMetricsHandler() {
		return new TaskManagerMetricsHandler(
			leaderRetriever,
			TIMEOUT,
			TEST_HEADERS,
			mockMetricFetcher);
	}

	@Override
	QueryScopeInfo getQueryScopeInfo() {
		return new QueryScopeInfo.TaskManagerQueryScopeInfo(TEST_TASK_MANAGER_ID);
	}

	@Override
	Map<String, String> getPathParameters() {
		return Collections.singletonMap(TaskManagerIdPathParameter.KEY, TEST_TASK_MANAGER_ID);
	}

}
