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

package org.apache.flink.runtime.deployment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.partition.BlockingShuffleType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceOptions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for ResultPartitionLocationTrackerProxy.
 */
public class ResultPartitionLocationTrackerProxyTest {

	@Test
	public void testUsingInternalShuffle() throws UnknownHostException {
		Configuration configuration = new Configuration();
		ResultPartitionLocationTrackerProxy resultPartitionLocationTrackerProxy = new ResultPartitionLocationTrackerProxy(configuration);

		TaskManagerLocation producer1 = new TaskManagerLocation(new ResourceID("TM1"), InetAddress.getLocalHost(), 12345);
		TaskManagerLocation consumer1 = new TaskManagerLocation(new ResourceID("TM1"), InetAddress.getLocalHost(), 12345);
		IntermediateResult intermediateResult1 = mock(IntermediateResult.class);
		assertTrue(resultPartitionLocationTrackerProxy.getResultPartitionLocation(producer1, consumer1, intermediateResult1).isLocal());

		TaskManagerLocation producer2 = new TaskManagerLocation(new ResourceID("TM1"), InetAddress.getLocalHost(), 12345);
		TaskManagerLocation consumer2 = new TaskManagerLocation(new ResourceID("TM2"), InetAddress.getLocalHost(), 54321);
		IntermediateResult intermediateResult2 = mock(IntermediateResult.class);
		when(intermediateResult2.getResultType()).thenReturn(ResultPartitionType.PIPELINED);
		ResultPartitionLocation resultPartitionLocation2 = resultPartitionLocationTrackerProxy.getResultPartitionLocation(producer2, consumer2, intermediateResult2);
		assertTrue(resultPartitionLocation2.isRemote());
		assertEquals(12345, resultPartitionLocation2.getConnectionId().getAddress().getPort());

		TaskManagerLocation producer3 = new TaskManagerLocation(new ResourceID("TM1"), InetAddress.getLocalHost(), 12345);
		TaskManagerLocation consumer3 = new TaskManagerLocation(new ResourceID("TM2"), InetAddress.getLocalHost(), 54321);
		IntermediateResult intermediateResult3 = mock(IntermediateResult.class);
		when(intermediateResult3.getResultType()).thenReturn(ResultPartitionType.BLOCKING);
		ResultPartitionLocation resultPartitionLocation3 = resultPartitionLocationTrackerProxy.getResultPartitionLocation(producer3, consumer3, intermediateResult3);
		assertTrue(resultPartitionLocation3.isRemote());
		assertEquals(12345, resultPartitionLocation3.getConnectionId().getAddress().getPort());
	}

	@Test
	public void testUsingExternalShuffle() throws UnknownHostException {
		Configuration configuration = new Configuration();
		configuration.setString(
			TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE, BlockingShuffleType.YARN.toString());
		configuration.setInteger(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY.key(), 23456);
		ResultPartitionLocationTrackerProxy resultPartitionLocationTrackerProxy = new ResultPartitionLocationTrackerProxy(configuration);

		TaskManagerLocation producer1 = new TaskManagerLocation(new ResourceID("TM1"), InetAddress.getLocalHost(), 12345);
		TaskManagerLocation consumer1 = new TaskManagerLocation(new ResourceID("TM1"), InetAddress.getLocalHost(), 12345);
		IntermediateResult intermediateResult1 = mock(IntermediateResult.class);
		assertTrue(resultPartitionLocationTrackerProxy.getResultPartitionLocation(producer1, consumer1, intermediateResult1).isLocal());

		TaskManagerLocation producer2 = new TaskManagerLocation(new ResourceID("TM1"), InetAddress.getLocalHost(), 12345);
		TaskManagerLocation consumer2 = new TaskManagerLocation(new ResourceID("TM2"), InetAddress.getLocalHost(), 54321);
		IntermediateResult intermediateResult2 = mock(IntermediateResult.class);
		when(intermediateResult2.getResultType()).thenReturn(ResultPartitionType.PIPELINED);
		ResultPartitionLocation resultPartitionLocation2 = resultPartitionLocationTrackerProxy.getResultPartitionLocation(producer2, consumer2, intermediateResult2);
		assertTrue(resultPartitionLocation2.isRemote());
		assertEquals(12345, resultPartitionLocation2.getConnectionId().getAddress().getPort());

		TaskManagerLocation producer3 = new TaskManagerLocation(new ResourceID("TM1"), InetAddress.getLocalHost(), 12345);
		TaskManagerLocation consumer3 = new TaskManagerLocation(new ResourceID("TM2"), InetAddress.getLocalHost(), 54321);
		IntermediateResult intermediateResult3 = mock(IntermediateResult.class);
		when(intermediateResult3.getResultType()).thenReturn(ResultPartitionType.BLOCKING);
		ResultPartitionLocation resultPartitionLocation3 = resultPartitionLocationTrackerProxy.getResultPartitionLocation(producer3, consumer3, intermediateResult3);
		assertTrue(resultPartitionLocation3.isRemote());
		assertEquals(23456, resultPartitionLocation3.getConnectionId().getAddress().getPort());
	}
}
