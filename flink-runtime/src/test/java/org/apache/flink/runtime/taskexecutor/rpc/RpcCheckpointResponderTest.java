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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public class RpcCheckpointResponderTest {
	@Test
	public void testNotifyJobManagerConnectionChanged() {
		JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);

		RpcCheckpointResponder rpcCheckpointResponder = new RpcCheckpointResponder(jobMasterGateway);

		JobMasterGateway newJobMasterGateway = mock(JobMasterGateway.class);

		doNothing().when(jobMasterGateway)
			.declineCheckpoint(any(JobID.class), any(ExecutionAttemptID.class), anyLong(), any(Exception.class));
		doNothing().when(newJobMasterGateway)
			.declineCheckpoint(any(JobID.class), any(ExecutionAttemptID.class), anyLong(), any(Exception.class));

		rpcCheckpointResponder.notifyJobManagerConnectionChanged(newJobMasterGateway, UUID.randomUUID());

		rpcCheckpointResponder.declineCheckpoint(JobID.generate(), new ExecutionAttemptID(), 0L, new Exception());

		// Verification
		verify(jobMasterGateway, never())
			.declineCheckpoint(any(JobID.class), any(ExecutionAttemptID.class), anyLong(), any(Exception.class));
		verify(newJobMasterGateway, times(1))
			.declineCheckpoint(any(JobID.class), any(ExecutionAttemptID.class), anyLong(), any(Exception.class));
	}
}
