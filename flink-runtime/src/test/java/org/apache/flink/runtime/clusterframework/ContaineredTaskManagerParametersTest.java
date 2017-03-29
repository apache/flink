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
import org.junit.Test;

import static org.apache.flink.configuration.ConfigConstants.TASK_MANAGER_MEMORY_OFF_HEAP_KEY;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ContaineredTaskManagerParametersTest {
	private static final long CONTAINER_MEMORY = 8192;

	@Test
	public void testDefaultOffHeapMemory() {
		Configuration conf = new Configuration();
		ContaineredTaskManagerParameters params =
			ContaineredTaskManagerParameters.create(conf, CONTAINER_MEMORY, 1);
		assertEquals(-1, params.taskManagerDirectMemoryLimitMB());
	}

	@Test
	public void testTotalMemoryDoesNotExceedContainerMemory() {
		Configuration conf = new Configuration();
		conf.setBoolean(MANAGED_MEMORY_SIZE.key(), true);
		ContaineredTaskManagerParameters params =
			ContaineredTaskManagerParameters.create(conf, CONTAINER_MEMORY, 1);
		assertTrue(params.taskManagerHeapSizeMB() +
			params.taskManagerDirectMemoryLimitMB() <= CONTAINER_MEMORY);
	}
}
