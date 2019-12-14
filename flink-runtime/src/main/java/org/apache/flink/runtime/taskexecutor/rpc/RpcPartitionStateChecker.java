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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;

public class RpcPartitionStateChecker implements PartitionProducerStateChecker {

	private final JobMasterGateway jobMasterGateway;

	public RpcPartitionStateChecker(JobMasterGateway jobMasterGateway) {
		this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionProducerState(
			JobID jobId,
			IntermediateDataSetID resultId,
			ResultPartitionID partitionId) {

		return jobMasterGateway.requestPartitionState(resultId, partitionId);
	}
}
