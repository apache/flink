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

package org.apache.flink.runtime.taskmanager;

import akka.dispatch.OnFailure;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * This implementation uses {@link ActorGateway} to notify the job manager about consumable
 * partitions.
 */
public class ActorGatewayResultPartitionConsumableNotifier implements ResultPartitionConsumableNotifier {

	private static final Logger LOG = LoggerFactory.getLogger(ActorGatewayResultPartitionConsumableNotifier.class);

	/**
	 * {@link ExecutionContext} which is used for the failure handler of
	 * {@link JobManagerMessages.ScheduleOrUpdateConsumers} messages.
	 */
	private final ExecutionContext executionContext;

	private final ActorGateway jobManager;

	private final FiniteDuration jobManagerMessageTimeout;

	public ActorGatewayResultPartitionConsumableNotifier(
		ExecutionContext executionContext,
		ActorGateway jobManager,
		FiniteDuration jobManagerMessageTimeout) {

		this.executionContext = Preconditions.checkNotNull(executionContext);
		this.jobManager = Preconditions.checkNotNull(jobManager);
		this.jobManagerMessageTimeout = Preconditions.checkNotNull(jobManagerMessageTimeout);
	}

	@Override
	public void notifyPartitionConsumable(JobID jobId, final ResultPartitionID partitionId, final TaskActions taskActions) {

		final JobManagerMessages.ScheduleOrUpdateConsumers msg = new JobManagerMessages.ScheduleOrUpdateConsumers(jobId, partitionId);

		Future<Object> futureResponse = jobManager.ask(msg, jobManagerMessageTimeout);

		futureResponse.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) {
				LOG.error("Could not schedule or update consumers at the JobManager.", failure);

				taskActions.failExternally(new RuntimeException("Could not notify JobManager to schedule or update consumers", failure));
			}
		}, executionContext);
	}
}
