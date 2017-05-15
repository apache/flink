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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetAddress;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Unit test for {@link TaskManagerServices}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(EnvironmentInformation.class)
public class TaskManagerServicesTest {

	/**
	 * Test for {@link TaskManagerServices#calculateNetworkBufferMemory(long, Configuration)} using old
	 * configurations via {@link TaskManagerOptions#NETWORK_NUM_BUFFERS}.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void calculateNetworkBufOld() throws Exception {
		Configuration config = new Configuration();
		config.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, 1);

		// note: actual network buffer memory size is independent of the totalJavaMemorySize
		assertEquals(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue().longValue(),
			TaskManagerServices.calculateNetworkBufferMemory(10L << 20, config));
		assertEquals(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue().longValue(),
			TaskManagerServices.calculateNetworkBufferMemory(64L << 20, config));

		// test integer overflow in the memory size
		int numBuffers = (int) ((2L << 32) / TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()); // 2^33
		config.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, numBuffers);
		assertEquals(2L << 32, TaskManagerServices.calculateNetworkBufferMemory(2L << 33, config));
	}

	/**
	 * Test for {@link TaskManagerServices#calculateNetworkBufferMemory(long, Configuration)} using new
	 * configurations via {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION},
	 * {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN} and
	 * {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}.
	 */
	@Test
	public void calculateNetworkBufNew() throws Exception {
		Configuration config = new Configuration();

		// (1) defaults
		final Float defaultFrac = TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.defaultValue();
		final Long defaultMin = TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.defaultValue();
		final Long defaultMax = TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.defaultValue();
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 20)), defaultMin, defaultMax),
			TaskManagerServices.calculateNetworkBufferMemory((64L << 20 + 1), config));
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 30)), defaultMin, defaultMax),
			TaskManagerServices.calculateNetworkBufferMemory((10L << 30), config));

		calculateNetworkBufNew(config);
	}

	/**
	 * Helper to test {@link TaskManagerServices#calculateNetworkBufferMemory(long, Configuration)} with the
	 * new configuration parameters.
	 *
	 * @param config configuration object
	 */
	private static void calculateNetworkBufNew(final Configuration config) {
		// (2) fixed size memory
		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, 1L << 20); // 1MB
		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 1L << 20); // 1MB

		// note: actual network buffer memory size is independent of the totalJavaMemorySize
		assertEquals(1 << 20, TaskManagerServices.calculateNetworkBufferMemory(10L << 20, config));
		assertEquals(1 << 20, TaskManagerServices.calculateNetworkBufferMemory(64L << 20, config));
		assertEquals(1 << 20, TaskManagerServices.calculateNetworkBufferMemory(1L << 30, config));

		// (3) random fraction, min, and max values
		Random ran = new Random();
		for (int i = 0; i < 1_000; ++i){
			float frac = Math.max(ran.nextFloat(), Float.MIN_VALUE);
			config.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, frac);

			long min = Math.max(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue(), ran.nextLong());
			config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, min);

			long max = Math.max(min, ran.nextLong());
			config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, max);

			long javaMem = Math.max(max + 1, ran.nextLong());

			final long networkBufMem = TaskManagerServices.calculateNetworkBufferMemory(javaMem, config);
			assertTrue("Lower bound not met with configuration: " + config.toString(),
				networkBufMem >= min);
			assertTrue("Upper bound not met with configuration: " + config.toString(),
				networkBufMem <= max);
			if (networkBufMem > min && networkBufMem < max) {
				assertEquals(
					"Wrong network buffer memory size with configuration: " + config.toString(),
					(long) (javaMem * frac), networkBufMem);
			}
		}
	}

	/**
	 * Test for {@link TaskManagerServices#calculateNetworkBufferMemory(long, Configuration)} using mixed
	 * old/new configurations.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void calculateNetworkBufMixed() throws Exception {
		Configuration config = new Configuration();
		config.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, 1);

		final Float defaultFrac = TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.defaultValue();
		final Long defaultMin = TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.defaultValue();
		final Long defaultMax = TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.defaultValue();

		// old + 1 new parameter = new:
		Configuration config1 = config.clone();
		config1.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		assertEquals(enforceBounds((long) (0.1f * (10L << 20)), defaultMin, defaultMax),
			TaskManagerServices.calculateNetworkBufferMemory((64L << 20 + 1), config1));
		assertEquals(enforceBounds((long) (0.1f * (10L << 30)), defaultMin, defaultMax),
			TaskManagerServices.calculateNetworkBufferMemory((10L << 30), config1));

		config1 = config.clone();
		long newMin = TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue(); // smallest value possible
		config1.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, newMin);
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 20)), newMin, defaultMax),
			TaskManagerServices.calculateNetworkBufferMemory((10L << 20), config1));
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 30)), newMin, defaultMax),
			TaskManagerServices.calculateNetworkBufferMemory((10L << 30), config1));

		config1 = config.clone();
		long newMax = Math.max(64L << 20 + 1, TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.defaultValue());
		config1.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, newMax);
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 20)), defaultMin, newMax),
			TaskManagerServices.calculateNetworkBufferMemory((64L << 20 + 1), config1));
		assertEquals(enforceBounds((long) (defaultFrac * (10L << 30)), defaultMin, newMax),
			TaskManagerServices.calculateNetworkBufferMemory((10L << 30), config1));
		assertTrue(TaskManagerServicesConfiguration.hasNewNetworkBufConf(config1));

		// old + any new parameter = new:
		calculateNetworkBufNew(config);
	}

	/**
	 * Returns the value or the lower/upper bound in case the value is less/greater than the lower/upper bound, respectively.
	 *
	 * @param value value to inspec
	 * @param lower lower bound
	 * @param upper upper bound
	 *
	 * @return <tt>min(upper, max(lower, value))</tt>
	 */
	private static long enforceBounds(final long value, final long lower, final long upper) {
		return Math.min(upper, Math.max(lower, value));
	}

	/**
	 * Test for {@link TaskManagerServices#calculateNetworkBufferMemory(TaskManagerServicesConfiguration)}
	 * using the same (manual) test cases as in {@link #calculateHeapSizeMB()}.
	 */
	@Test
	public void calculateNetworkBufFromHeapSize() throws Exception {
		PowerMockito.mockStatic(EnvironmentInformation.class);
		// some defaults:
		when(EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag()).thenReturn(1000L << 20); // 1000MB
		when(EnvironmentInformation.getMaxJvmHeapMemory()).thenReturn(1000L << 20); // 1000MB

		TaskManagerServicesConfiguration tmConfig;

		tmConfig = getTmConfig(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue(),
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.1f, 60L << 20, 1L << 30, MemoryType.HEAP);
		when(EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag()).thenReturn(1000L << 20); // 1000MB
		assertEquals(100L << 20, TaskManagerServices.calculateNetworkBufferMemory(tmConfig));

		tmConfig = getTmConfig(10, TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.1f, 60L << 20, 1L << 30, MemoryType.OFF_HEAP);
		when(EnvironmentInformation.getMaxJvmHeapMemory()).thenReturn(890L << 20); // 890MB
		assertEquals((100L << 20) + 1 /* one too many due to floating point imprecision */,
			TaskManagerServices.calculateNetworkBufferMemory(tmConfig));

		tmConfig = getTmConfig(-1, 0.1f,
			0.1f, 60L << 20, 1L << 30, MemoryType.OFF_HEAP);
		when(EnvironmentInformation.getMaxJvmHeapMemory()).thenReturn(810L << 20); // 810MB
		assertEquals((100L << 20) + 1 /* one too many due to floating point imprecision */,
			TaskManagerServices.calculateNetworkBufferMemory(tmConfig));
	}

	/**
	 * Returns a task manager services configuration for the tests
	 *
	 * @param managedMemory         see {@link TaskManagerOptions#MANAGED_MEMORY_SIZE}
	 * @param managedMemoryFraction see {@link TaskManagerOptions#MANAGED_MEMORY_FRACTION}
	 * @param networkBufFraction	see {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION}
	 * @param networkBufMin			see {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN}
	 * @param networkBufMax			see {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}
	 * @param memType				on-heap or off-heap
	 *
	 * @return configuration object
	 */
	private static TaskManagerServicesConfiguration getTmConfig(
		final long managedMemory, final float managedMemoryFraction, float networkBufFraction,
		long networkBufMin, long networkBufMax,
		final MemoryType memType) {

		final NetworkEnvironmentConfiguration networkConfig = new NetworkEnvironmentConfiguration(
			networkBufFraction,
			networkBufMin,
			networkBufMax,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue(),
			memType,
			null,
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL.defaultValue(),
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX.defaultValue(),
			TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue(),
			TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.defaultValue(),
			null);

		return new TaskManagerServicesConfiguration(
			mock(InetAddress.class),
			new String[] {},
			networkConfig,
			QueryableStateConfiguration.disabled(),
			1,
			managedMemory,
			false,
			managedMemoryFraction,
			mock(MetricRegistryConfiguration.class),
			0);
	}

	/**
	 * Test for {@link TaskManagerServices#calculateHeapSizeMB(long, Configuration)} with some
	 * manually calculated scenarios.
	 */
	@Test
	public void calculateHeapSizeMB() throws Exception {
		Configuration config = new Configuration();
		config.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, 0.1f);
		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, 64L << 20); // 64MB
		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 1L << 30); // 1GB

		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, false);
		assertEquals(1000, TaskManagerServices.calculateHeapSizeMB(1000, config));

		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, true);
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 10); // 10MB
		assertEquals(890, TaskManagerServices.calculateHeapSizeMB(1000, config));

		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, -1); // use fraction of given memory
		config.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.1f); // 10%
		assertEquals(810, TaskManagerServices.calculateHeapSizeMB(1000, config));
	}

}
