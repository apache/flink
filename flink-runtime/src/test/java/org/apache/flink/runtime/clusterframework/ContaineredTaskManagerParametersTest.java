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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ContaineredTaskManagerParametersTest extends TestLogger {
	private static final long CONTAINER_MEMORY = 8192L;

	/**
	 * This tests that per default the off heap memory is set to -1.
	 */
	@Test
	public void testOffHeapMemoryWithDefaultConfiguration() {
		Configuration conf = new Configuration();

		ContaineredTaskManagerParameters params =
			ContaineredTaskManagerParameters.create(conf, CONTAINER_MEMORY, 1);
		assertEquals(-1L, params.taskManagerDirectMemoryLimitMB());
	}

	/**
	 * This tests that when using off heap memory the sum of on and off heap memory does not exceeds the container
	 * maximum.
	 */
	@Test
	public void testTotalMemoryDoesNotExceedContainerMemory() {
		Configuration conf = new Configuration();
		conf.setBoolean(MEMORY_OFF_HEAP, true);

		ContaineredTaskManagerParameters params =
			ContaineredTaskManagerParameters.create(conf, CONTAINER_MEMORY, 1);

		assertTrue(params.taskManagerDirectMemoryLimitMB() > 0L);

		assertTrue(params.taskManagerHeapSizeMB() +
			params.taskManagerDirectMemoryLimitMB() <= CONTAINER_MEMORY);
	}
}
