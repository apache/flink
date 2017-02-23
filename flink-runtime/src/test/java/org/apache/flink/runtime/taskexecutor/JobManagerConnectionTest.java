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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JobManagerConnectionTest {

	@Test
	public void testJobManagerUpdateListener() {
		JobManagerConnection jobManagerConnection = new JobManagerConnection(
			mock(JobMasterGateway.class),
			UUID.randomUUID(),
			mock(TaskManagerActions.class),
			mock(CheckpointResponder.class),
			mock(LibraryCacheManager.class),
			mock(ResultPartitionConsumableNotifier.class),
			mock(PartitionProducerStateChecker.class)
		);

		final AtomicInteger listenerNotified1 = new AtomicInteger(0);
		final AtomicInteger listenerNotified2 = new AtomicInteger(0);

		jobManagerConnection.registerListener(new JobManagerConnectionListener() {
			@Override
			public void notifyJobManagerConnectionChanged(JobMasterGateway jobMasterGateway,
														  UUID jobMasterLeaderID) {
				listenerNotified1.incrementAndGet();
			}
		});

		jobManagerConnection.registerListener(new JobManagerConnectionListener() {
			@Override
			public void notifyJobManagerConnectionChanged(JobMasterGateway jobMasterGateway,
														  UUID jobMasterLeaderID) {
				listenerNotified2.incrementAndGet();
			}
		});

		JobManagerConnection newJobManagerConnection = mock(JobManagerConnection.class);
		jobManagerConnection.notifyListener(newJobManagerConnection);

		verify(newJobManagerConnection, times(2)).getJobManagerGateway();
		verify(newJobManagerConnection, times(2)).getLeaderId();

		assertEquals(1, listenerNotified1.get());
		assertEquals(1, listenerNotified2.get());
	}
}

