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

package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.UUID;
import java.util.concurrent.Executor;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RpcResultPartitionConsumableNotifierTest {
	@Test
	public void testNotifyJobManagerConnectionChanged() {
		JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		UUID jobMasterLeaderId = UUID.randomUUID();

		RpcResultPartitionConsumableNotifier rpcResultPartitionConsumableNotifier =
			new RpcResultPartitionConsumableNotifier(
				jobMasterLeaderId, jobMasterGateway, mock(Executor.class), Time.minutes(1L));

		JobMasterGateway newJobMasterGateway = mock(JobMasterGateway.class);
		UUID newJobMasterLeaderId = UUID.randomUUID();

		Future<Acknowledge> future = mock(Future.class);

		when(jobMasterGateway.scheduleOrUpdateConsumers(any(UUID.class), any(ResultPartitionID.class), any(Time.class)))
			.thenReturn(future);

		when(newJobMasterGateway
			.scheduleOrUpdateConsumers(any(UUID.class), any(ResultPartitionID.class), any(Time.class)))
			.thenReturn(future);

		rpcResultPartitionConsumableNotifier.notifyJobManagerConnectionChanged(newJobMasterGateway, newJobMasterLeaderId);

		rpcResultPartitionConsumableNotifier
			.notifyPartitionConsumable(JobID.generate(), new ResultPartitionID(), mock(TaskActions.class));

		// Verification
		verify(jobMasterGateway, never())
			.scheduleOrUpdateConsumers(any(UUID.class), any(ResultPartitionID.class), any(Time.class));

		ArgumentCaptor<UUID> argumentCaptor = ArgumentCaptor.forClass(UUID.class);
		verify(newJobMasterGateway, times(1))
			.scheduleOrUpdateConsumers(argumentCaptor.capture(), any(ResultPartitionID.class), any(Time.class));

		assertEquals(newJobMasterLeaderId, argumentCaptor.getValue());
	}
}
