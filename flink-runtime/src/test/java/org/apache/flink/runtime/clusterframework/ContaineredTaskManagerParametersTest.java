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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_OFF_HEAP;
import static org.apache.flink.runtime.taskexecutor.TaskManagerServices.calculateNetworkBufferMemory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ContaineredTaskManagerParametersTest extends TestLogger {
	private static final long CONTAINER_MEMORY = 8192L;

	/**
	 * This tests that per default the off heap memory is set to what the network buffers require.
	 */
	@Test
	public void testOffHeapMemoryWithDefaultConfiguration() {
		Configuration conf = new Configuration();

		ContaineredTaskManagerParameters params =
			ContaineredTaskManagerParameters.create(conf, CONTAINER_MEMORY, 1);

		final float memoryCutoffRatio = conf.getFloat(
			ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_RATIO,
			ConfigConstants.DEFAULT_YARN_HEAP_CUTOFF_RATIO);
		final int minCutoff = conf.getInteger(
			ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_MIN,
			ConfigConstants.DEFAULT_YARN_HEAP_CUTOFF);

		long cutoff = Math.max((long) (CONTAINER_MEMORY * memoryCutoffRatio), minCutoff);
		final long networkBufMB =
			calculateNetworkBufferMemory(
				(CONTAINER_MEMORY - cutoff) << 20, // megabytes to bytes
				conf) >> 20; // bytes to megabytes
		assertEquals(networkBufMB + cutoff, params.taskManagerDirectMemoryLimitMB());
	}

	/**
	 * This tests that when using off-heap memory the sum of on and off heap memory does not exceed the container
	 * maximum.
	 */
	@Test
	public void testTotalMemoryDoesNotExceedContainerMemoryOnHeap() {
		Configuration conf = new Configuration();
		conf.setBoolean(MEMORY_OFF_HEAP, false);

		ContaineredTaskManagerParameters params =
			ContaineredTaskManagerParameters.create(conf, CONTAINER_MEMORY, 1);

		assertTrue(params.taskManagerDirectMemoryLimitMB() > 0L);

		assertTrue(params.taskManagerHeapSizeMB() +
			params.taskManagerDirectMemoryLimitMB() <= CONTAINER_MEMORY);
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
			ContaineredTaskManagerParameters.create(conf, CONTAINER_MEMORY, 1);

		assertTrue(params.taskManagerDirectMemoryLimitMB() > 0L);

		assertTrue(params.taskManagerHeapSizeMB() +
			params.taskManagerDirectMemoryLimitMB() <= CONTAINER_MEMORY);
	}

	/**
	 * Test to guard {@link ContaineredTaskManagerParameters#calculateCutoffMB(Configuration, long)}.
	 */
	@Test
	public void testCalculateCutoffMB() {

		Configuration config = new Configuration();
		long containerMemoryMB = 1000L;

		config.setFloat(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO, 0.1f);
		config.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 128);

		assertEquals(128,
			ContaineredTaskManagerParameters.calculateCutoffMB(config, containerMemoryMB));

		config.setFloat(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO, 0.2f);
		assertEquals(200,
			ContaineredTaskManagerParameters.calculateCutoffMB(config, containerMemoryMB));

		config.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 1000);

		try {
			ContaineredTaskManagerParameters.calculateCutoffMB(config, containerMemoryMB);
			fail("Expected to fail with an invalid argument exception.");
		} catch (IllegalArgumentException ignored) {
			// we expected it.
		}
	}
}
