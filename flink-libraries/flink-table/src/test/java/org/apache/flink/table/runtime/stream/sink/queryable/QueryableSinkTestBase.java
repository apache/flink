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

package org.apache.flink.table.runtime.stream.sink.queryable;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.TestBaseUtils;

import org.junit.ClassRule;

/**
 * Base class for queryable sink.
 */
public class QueryableSinkTestBase extends TestBaseUtils {
	private static final int DEFAULT_PARALLELISM = 2;
	private static final int QS_PROXY_PORT = 9084;

	@ClassRule
	public static MiniClusterResource miniClusterResource = new MiniClusterResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfig())
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
			.build());

	public static Configuration getConfig() {
		Configuration config = new Configuration();
		config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "4m");
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, DEFAULT_PARALLELISM);
		config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
		config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 1);
		config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);
		config.setString(QueryableStateOptions.PROXY_PORT_RANGE, String.valueOf(QS_PROXY_PORT));
		config.setString(QueryableStateOptions.SERVER_PORT_RANGE, "9089");
		config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
		return config;
	}
}
