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

package org.apache.flink.runtime.profiling.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.ManagementTestUtils;
import org.junit.Test;

/**
 * This test checks the proper serialization and deserialization of profiling events.
 */
public class ProfilingTypesTest {

	private static final int PROFILING_INTERVAL = 4321;

	private static final int IOWAIT_CPU = 10;

	private static final int IDLE_CPU = 11;

	private static final int USER_CPU = 12;

	private static final int SYSTEM_CPU = 13;

	private static final int HARD_IRQ_CPU = 14;

	private static final int SOFT_IRQ_CPU = 15;

	private static final long TOTAL_MEMORY = 10001L;

	private static final long FREE_MEMORY = 10002L;

	private static final long BUFFERED_MEMORY = 10003L;

	private static final long CACHED_MEMORY = 10004L;

	private static final long CACHED_SWAP_MEMORY = 10005L;

	private static final long RECEIVED_BYTES = 100006L;

	private static final long TRANSMITTED_BYTES = 100007L;

	private static final long TIMESTAMP = 100008L;

	private static final long PROFILING_TIMESTAMP = 100009L;

	private static final String INSTANCE_NAME = "Test Instance";

	private static final int USER_TIME = 17;

	private static final int SYSTEM_TIME = 18;

	private static final int BLOCKED_TIME = 19;

	private static final int WAITED_TIME = 20;

	/**
	 * Tests serialization/deserialization for {@link InstanceSummaryProfilingEvent}.
	 */
	@Test
	public void testInstanceSummaryProfilingEvent() {

		final InstanceSummaryProfilingEvent orig = new InstanceSummaryProfilingEvent(PROFILING_INTERVAL, IOWAIT_CPU,
			IDLE_CPU, USER_CPU, SYSTEM_CPU, HARD_IRQ_CPU, SOFT_IRQ_CPU, TOTAL_MEMORY, FREE_MEMORY, BUFFERED_MEMORY,
			CACHED_MEMORY, CACHED_SWAP_MEMORY, RECEIVED_BYTES, TRANSMITTED_BYTES, new JobID(), TIMESTAMP,
			PROFILING_TIMESTAMP);

		final InstanceSummaryProfilingEvent copy = (InstanceSummaryProfilingEvent) ManagementTestUtils.createCopy(orig);

		assertEquals(orig.getProfilingInterval(), copy.getProfilingInterval());
		assertEquals(orig.getIOWaitCPU(), copy.getIOWaitCPU());
		assertEquals(orig.getIdleCPU(), copy.getIdleCPU());
		assertEquals(orig.getUserCPU(), copy.getUserCPU());
		assertEquals(orig.getSystemCPU(), copy.getSystemCPU());
		assertEquals(orig.getHardIrqCPU(), copy.getHardIrqCPU());
		assertEquals(orig.getSoftIrqCPU(), copy.getSoftIrqCPU());
		assertEquals(orig.getTotalMemory(), copy.getTotalMemory());
		assertEquals(orig.getFreeMemory(), copy.getFreeMemory());
		assertEquals(orig.getBufferedMemory(), copy.getBufferedMemory());
		assertEquals(orig.getCachedMemory(), copy.getCachedMemory());
		assertEquals(orig.getCachedSwapMemory(), copy.getCachedSwapMemory());
		assertEquals(orig.getReceivedBytes(), copy.getReceivedBytes());
		assertEquals(orig.getTransmittedBytes(), copy.getTransmittedBytes());
		assertEquals(orig.getJobID(), copy.getJobID());
		assertEquals(orig.getTimestamp(), copy.getTimestamp());
		assertEquals(orig.getProfilingTimestamp(), copy.getProfilingTimestamp());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));
	}

	/**
	 * Tests serialization/deserialization for {@link SingleInstanceProfilingEvent}.
	 */
	@Test
	public void testSingleInstanceProfilingEvent() {
		try {
			final SingleInstanceProfilingEvent orig = new SingleInstanceProfilingEvent(PROFILING_INTERVAL, IOWAIT_CPU,
				IDLE_CPU, USER_CPU, SYSTEM_CPU, HARD_IRQ_CPU, SOFT_IRQ_CPU, TOTAL_MEMORY, FREE_MEMORY, BUFFERED_MEMORY,
				CACHED_MEMORY, CACHED_SWAP_MEMORY, RECEIVED_BYTES, TRANSMITTED_BYTES, new JobID(), TIMESTAMP,
				PROFILING_TIMESTAMP, INSTANCE_NAME);
	
			final SingleInstanceProfilingEvent copy = (SingleInstanceProfilingEvent) CommonTestUtils.createCopyWritable(orig);
	
			assertEquals(orig.getProfilingInterval(), copy.getProfilingInterval());
			assertEquals(orig.getIOWaitCPU(), copy.getIOWaitCPU());
			assertEquals(orig.getIdleCPU(), copy.getIdleCPU());
			assertEquals(orig.getUserCPU(), copy.getUserCPU());
			assertEquals(orig.getSystemCPU(), copy.getSystemCPU());
			assertEquals(orig.getHardIrqCPU(), copy.getHardIrqCPU());
			assertEquals(orig.getSoftIrqCPU(), copy.getSoftIrqCPU());
			assertEquals(orig.getTotalMemory(), copy.getTotalMemory());
			assertEquals(orig.getFreeMemory(), copy.getFreeMemory());
			assertEquals(orig.getBufferedMemory(), copy.getBufferedMemory());
			assertEquals(orig.getCachedMemory(), copy.getCachedMemory());
			assertEquals(orig.getCachedSwapMemory(), copy.getCachedSwapMemory());
			assertEquals(orig.getReceivedBytes(), copy.getReceivedBytes());
			assertEquals(orig.getTransmittedBytes(), copy.getTransmittedBytes());
			assertEquals(orig.getJobID(), copy.getJobID());
			assertEquals(orig.getTimestamp(), copy.getTimestamp());
			assertEquals(orig.getProfilingTimestamp(), copy.getProfilingTimestamp());
			assertEquals(orig.getInstanceName(), copy.getInstanceName());
			assertEquals(orig.hashCode(), copy.hashCode());
			assertTrue(orig.equals(copy));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	/**
	 * Tests serialization/deserialization for {@link ThreadProfilingEvent}.
	 */
	@Test
	public void testThreadProfilingEvent() {

		final ThreadProfilingEvent orig = new ThreadProfilingEvent(USER_TIME, SYSTEM_TIME, BLOCKED_TIME, WAITED_TIME,
			new JobVertexID(), 17, new ExecutionAttemptID(), PROFILING_INTERVAL, new JobID(), TIMESTAMP, PROFILING_TIMESTAMP);

		final ThreadProfilingEvent copy = (ThreadProfilingEvent) ManagementTestUtils.createCopy(orig);

		assertEquals(orig.getUserTime(), copy.getUserTime());
		assertEquals(orig.getSystemTime(), copy.getSystemTime());
		assertEquals(orig.getBlockedTime(), copy.getBlockedTime());
		assertEquals(orig.getWaitedTime(), copy.getWaitedTime());
		assertEquals(orig.getVertexID(), copy.getVertexID());
		assertEquals(orig.getProfilingInterval(), copy.getProfilingInterval());
		assertEquals(orig.getJobID(), copy.getJobID());
		assertEquals(orig.getTimestamp(), copy.getTimestamp());
		assertEquals(orig.getProfilingTimestamp(), copy.getProfilingTimestamp());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));
	}
}
