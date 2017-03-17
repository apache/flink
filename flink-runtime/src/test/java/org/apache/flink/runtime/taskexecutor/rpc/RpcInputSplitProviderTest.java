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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RpcInputSplitProviderTest {
	@Test
	public void notifyJobManagerConnectionChanged() throws Exception {
		JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		UUID jobMasterLeaderId = UUID.randomUUID();

		RpcInputSplitProvider rpcInputSplitProvider =
			new RpcInputSplitProvider(
				jobMasterLeaderId,
				jobMasterGateway,
				JobID.generate(),
				new JobVertexID(),
				new ExecutionAttemptID(),
				Time.minutes(1L));

		JobMasterGateway newJobMasterGateway = mock(JobMasterGateway.class);
		UUID newJobMasterLeaderId = UUID.randomUUID();

		Future<SerializedInputSplit> future = mock(Future.class);

		when(future.get(anyLong(), any(TimeUnit.class))).thenReturn(new SerializedInputSplit(null));

		when(jobMasterGateway.requestNextInputSplit(
			any(UUID.class), any(JobVertexID.class), any(ExecutionAttemptID.class)))
			.thenReturn(future);

		when(newJobMasterGateway.requestNextInputSplit(
			any(UUID.class), any(JobVertexID.class), any(ExecutionAttemptID.class)))
			.thenReturn(future);

		rpcInputSplitProvider.notifyJobManagerConnectionChanged(newJobMasterGateway, newJobMasterLeaderId);

		rpcInputSplitProvider.getNextInputSplit(Thread.currentThread().getContextClassLoader());

		// Verification
		verify(jobMasterGateway, never())
			.requestNextInputSplit(any(UUID.class), any(JobVertexID.class), any(ExecutionAttemptID.class));

		ArgumentCaptor<UUID> argumentCaptor = ArgumentCaptor.forClass(UUID.class);
		verify(newJobMasterGateway, times(1))
			.requestNextInputSplit(argumentCaptor.capture(), any(JobVertexID.class), any(ExecutionAttemptID.class));

		assertEquals(newJobMasterLeaderId, argumentCaptor.getValue());
	}

}
