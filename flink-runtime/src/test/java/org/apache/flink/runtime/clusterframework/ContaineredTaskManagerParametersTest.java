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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_OFF_HEAP;
import static org.apache.flink.configuration.TaskManagerOptions.TOTAL_PROCESS_MEMORY;
import static org.junit.Assert.assertTrue;

public class ContaineredTaskManagerParametersTest extends TestLogger {
	private static final long CONTAINER_MEMORY_MB = 8192L;

	/**
	 * This tests that when using off-heap memory the sum of on and off heap memory does not exceed the container
	 * maximum.
	 */
	@Test
	public void testTotalMemoryDoesNotExceedContainerMemoryOnHeap() {
		Configuration conf = new Configuration();
		conf.setBoolean(MEMORY_OFF_HEAP, false);

		ContaineredTaskManagerParameters params =
			ContaineredTaskManagerParameters.create(conf, getTmResourceSpec(), 1);

		assertTrue(params.taskManagerHeapSizeMB() +
			params.taskManagerDirectMemoryLimitMB() <= CONTAINER_MEMORY_MB);
	}

	/**
	 * This tests that when using on-heap memory the sum of on and off heap memory does not exceed the container
	 * maximum.
	 */
	@Test
	public void testTotalMemoryDoesNotExceedContainerMemoryOffHeap() {
		Configuration conf = new Configuration();
		conf.setBoolean(MEMORY_OFF_HEAP, true);

		ContaineredTaskManagerParameters params =
			ContaineredTaskManagerParameters.create(conf, getTmResourceSpec(), 1);

		assertTrue(params.taskManagerHeapSizeMB() +
			params.taskManagerDirectMemoryLimitMB() <= CONTAINER_MEMORY_MB);
	}

	private static TaskExecutorResourceSpec getTmResourceSpec() {
		final Configuration config = new Configuration();
		config.setString(TOTAL_PROCESS_MEMORY, CONTAINER_MEMORY_MB + "m");
		return TaskExecutorResourceUtils.resourceSpecFromConfig(config);
	}
}
