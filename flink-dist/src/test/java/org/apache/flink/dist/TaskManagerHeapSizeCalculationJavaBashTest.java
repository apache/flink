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

package org.apache.flink.dist;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit test that verifies that the task manager heap size calculation used by the bash script
 * <tt>taskmanager.sh</tt> returns the same values as the heap size calculation of
 * {@link TaskManagerServices#calculateHeapSizeMB(long, Configuration)}.
 *
 * <p>NOTE: the shell script uses <tt>awk</tt> to perform floating-point arithmetic which uses
 * <tt>double</tt> precision but our Java code restrains to <tt>float</tt> because we actually do
 * not need high precision.
 */
public class TaskManagerHeapSizeCalculationJavaBashTest extends TestLogger {

	/** Key that is used by <tt>config.sh</tt>. */
	private static final String KEY_TASKM_MEM_SIZE = "taskmanager.heap.size";

	/**
	 * Number of tests with random values.
	 *
	 * <p>NOTE: calling the external test script is slow and thus low numbers are preferred for general
	 * testing.
	 */
	private static final int NUM_RANDOM_TESTS = 20;

	@Before
	public void checkOperatingSystem() {
		Assume.assumeTrue("This test checks shell scripts which are not available on Windows.",
			!OperatingSystem.isWindows());
	}

	/**
	 * Tests that {@link TaskManagerServices#calculateNetworkBufferMemory(long, Configuration)} has the same
	 * result as the shell script.
	 */
	@Test
	public void compareNetworkBufShellScriptWithJava() throws Exception {
		int managedMemSize = Integer.valueOf(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue());
		float managedMemFrac = TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue();

		// manual tests from org.apache.flink.runtime.taskexecutor.TaskManagerServices.calculateHeapSizeMB()

		compareNetworkBufJavaVsScript(
			getConfig(1000, false, 0.1f, 64L << 20, 1L << 30, managedMemSize, managedMemFrac), 0.0f);

		compareNetworkBufJavaVsScript(
			getConfig(1000, true, 0.1f, 64L << 20, 1L << 30, 10 /*MB*/, managedMemFrac), 0.0f);

		compareNetworkBufJavaVsScript(
			getConfig(1000, true, 0.1f, 64L << 20, 1L << 30, managedMemSize, 0.1f), 0.0f);

		// some automated tests with random (but valid) values

		Random ran = new Random();
		for (int i = 0; i < NUM_RANDOM_TESTS; ++i) {
			// tolerate that values differ by 1% (due to different floating point precisions)
			compareNetworkBufJavaVsScript(getRandomConfig(ran), 0.01f);
		}
	}

	/**
	 * Tests that {@link TaskManagerServices#calculateHeapSizeMB(long, Configuration)} has the same
	 * result as the shell script.
	 */
	@Test
	public void compareHeapSizeShellScriptWithJava() throws Exception {
		int managedMemSize = Integer.valueOf(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue());
		float managedMemFrac = TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue();

		// manual tests from org.apache.flink.runtime.taskexecutor.TaskManagerServices.calculateHeapSizeMB()

		compareHeapSizeJavaVsScript(
			getConfig(1000, false, 0.1f, 64L << 20, 1L << 30, managedMemSize, managedMemFrac), 0.0f);

		compareHeapSizeJavaVsScript(
			getConfig(1000, true, 0.1f, 64L << 20, 1L << 30, 10 /*MB*/, managedMemFrac), 0.0f);

		compareHeapSizeJavaVsScript(
			getConfig(1000, true, 0.1f, 64L << 20, 1L << 30, managedMemSize, 0.1f), 0.0f);

		// some automated tests with random (but valid) values

		Random ran = new Random();
		for (int i = 0; i < NUM_RANDOM_TESTS; ++i) {
			// tolerate that values differ by 1% (due to different floating point precisions)
			compareHeapSizeJavaVsScript(getRandomConfig(ran), 0.01f);
		}
	}

	/**
	 * Returns a flink configuration object with the given values.
	 *
	 * @param javaMemMB
	 * 		total JVM memory to use (in megabytes)
	 * @param useOffHeap
	 * 		whether to use off-heap memory (<tt>true</tt>) or not (<tt>false</tt>)
	 * @param netBufMemFrac
	 * 		fraction of JVM memory to use for network buffers
	 * @param netBufMemMin
	 * 		minimum memory size for network buffers (in bytes)
	 * @param netBufMemMax
	 * 		maximum memory size for network buffers (in bytes)
	 * @param managedMemSizeMB
	 * 		amount of managed memory (in megabytes)
	 * @param managedMemFrac
	 * 		fraction of free memory to use for managed memory (if <tt>managedMemSizeMB</tt> is
	 * 		<tt>-1</tt>)
	 *
	 * @return flink configuration
	 */
	private static Configuration getConfig(
			final int javaMemMB, final boolean useOffHeap, final float netBufMemFrac,
			final long netBufMemMin, final long netBufMemMax, final int managedMemSizeMB,
			final float managedMemFrac) {

		Configuration config = new Configuration();

		config.setLong(KEY_TASKM_MEM_SIZE, javaMemMB);
		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, useOffHeap);

		config.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION, netBufMemFrac);
		config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, String.valueOf(netBufMemMin));
		config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, String.valueOf(netBufMemMax));

		if (managedMemSizeMB == 0) {
			config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "0");
		} else {
			config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, managedMemSizeMB + "m");
		}
		config.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, managedMemFrac);

		return config;
	}

	/**
	 * Returns a flink configuration object with random values (only those relevant to the tests in
	 * this class.
	 *
	 * @param ran  random number generator
	 *
	 * @return flink configuration
	 */
	private static Configuration getRandomConfig(final Random ran) {

		float frac = Math.max(ran.nextFloat(), Float.MIN_VALUE);

		// note: we are testing with integers only here to avoid overly complicated checks for
		// overflowing or negative Long values - this should be enough for any practical scenario
		// though
		long min = MemorySize.parse(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()).getBytes() + ran.nextInt(Integer.MAX_VALUE);
		long max = ran.nextInt(Integer.MAX_VALUE) + min;

		int javaMemMB = Math.max((int) (max >> 20), ran.nextInt(Integer.MAX_VALUE)) + 1;
		boolean useOffHeap = ran.nextBoolean();

		int managedMemSize = Integer.valueOf(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue());
		float managedMemFrac = TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue();

		if (ran.nextBoolean()) {
			// use fixed-size managed memory
			Configuration config = getConfig(javaMemMB, useOffHeap, frac, min, max, managedMemSize, managedMemFrac);
			long totalJavaMemorySize = ((long) javaMemMB) << 20; // megabytes to bytes
			final int networkBufMB =
				(int) (TaskManagerServices.calculateNetworkBufferMemory(totalJavaMemorySize, config) >> 20);
			// max (exclusive): total - netbuf
			managedMemSize = Math.min(javaMemMB - networkBufMB - 1, ran.nextInt(Integer.MAX_VALUE));
		} else {
			// use fraction of given memory
			managedMemFrac = Math.max(ran.nextFloat(), Float.MIN_VALUE);
		}

		return getConfig(javaMemMB, useOffHeap, frac, min, max, managedMemSize, managedMemFrac);
	}

	// Helper functions

	/**
	 * Calculates the heap size via
	 * {@link TaskManagerServices#calculateHeapSizeMB(long, Configuration)} and the shell script
	 * and verifies that these are equal.
	 *
	 * @param config     flink configuration
	 * @param tolerance  tolerate values that are off by this factor (0.01 = 1%)
	 */
	private void compareNetworkBufJavaVsScript(final Configuration config, final float tolerance)
			throws IOException {

		final long totalJavaMemorySizeMB = config.getLong(KEY_TASKM_MEM_SIZE, 0L);

		long javaNetworkBufMem = TaskManagerServices.calculateNetworkBufferMemory(totalJavaMemorySizeMB << 20, config);

		String[] command = {"src/test/bin/calcTMNetBufMem.sh",
			totalJavaMemorySizeMB + "m",
			String.valueOf(config.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION)),
			config.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN),
			config.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX)};

		String scriptOutput = executeScript(command);

		long absoluteTolerance = (long) (javaNetworkBufMem * tolerance);
		if (absoluteTolerance < 1) {
			assertEquals(
				"Different network buffer memory sizes with configuration: " + config.toString(),
				String.valueOf(javaNetworkBufMem), scriptOutput);
		} else {
			Long scriptNetworkBufMem = Long.valueOf(scriptOutput);
			assertThat(
				"Different network buffer memory sizes (Java: " + javaNetworkBufMem + ", Script: " + scriptNetworkBufMem +
					") with configuration: " + config.toString(), scriptNetworkBufMem,
				allOf(greaterThanOrEqualTo(javaNetworkBufMem - absoluteTolerance),
					lessThanOrEqualTo(javaNetworkBufMem + absoluteTolerance)));
		}
	}

	/**
	 * Calculates the heap size via
	 * {@link TaskManagerServices#calculateHeapSizeMB(long, Configuration)} and the shell script
	 * and verifies that these are equal.
	 *
	 * @param config     flink configuration
	 * @param tolerance  tolerate values that are off by this factor (0.01 = 1%)
	 */
	private void compareHeapSizeJavaVsScript(final Configuration config, float tolerance)
			throws IOException {

		final long totalJavaMemorySizeMB = config.getLong(KEY_TASKM_MEM_SIZE, 0L);

		long javaHeapSizeMB = TaskManagerServices.calculateHeapSizeMB(totalJavaMemorySizeMB, config);

		String[] command = {"src/test/bin/calcTMHeapSizeMB.sh",
			totalJavaMemorySizeMB + "m",
			String.valueOf(config.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP)),
			String.valueOf(config.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION)),
			config.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN),
			config.getString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX),
			config.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE),
			String.valueOf(config.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION))};
		String scriptOutput = executeScript(command);

		long absoluteTolerance = (long) (javaHeapSizeMB * tolerance);
		if (absoluteTolerance < 1) {
			assertEquals("Different heap sizes with configuration: " + config.toString(),
				String.valueOf(javaHeapSizeMB), scriptOutput);
		} else {
			Long scriptHeapSizeMB = Long.valueOf(scriptOutput);
			assertThat(
				"Different heap sizes (Java: " + javaHeapSizeMB + ", Script: " + scriptHeapSizeMB +
					") with configuration: " + config.toString(), scriptHeapSizeMB,
				allOf(greaterThanOrEqualTo(javaHeapSizeMB - absoluteTolerance),
					lessThanOrEqualTo(javaHeapSizeMB + absoluteTolerance)));
		}
	}

	/**
	 * Executes the given shell script wrapper and returns its output.
	 *
	 * @param command  command to run
	 *
	 * @return raw script output
	 */
	private String executeScript(final String[] command) throws IOException {
		ProcessBuilder pb = new ProcessBuilder(command);
		pb.redirectErrorStream(true);
		Process process = pb.start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
		StringBuilder sb = new StringBuilder();
		String s;
		while ((s = reader.readLine()) != null) {
			sb.append(s);
		}
		return sb.toString();
	}

}
