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

import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.util.Preconditions;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.FiniteDuration;

/**
 * Factory implementation which generates {@link ActorGateway} based job manager communication
 * components.
 */
public class ActorGatewayJobManagerCommunicationFactory implements JobManagerCommunicationFactory {
	private final ExecutionContext executionContext;
	private final ActorGateway jobManagerGateway;
	private final ActorGateway taskManagerGateway;
	private final FiniteDuration jobManagerMessageTimeout;

	public ActorGatewayJobManagerCommunicationFactory(
		ExecutionContext executionContext,
		ActorGateway jobManagerGateway,
		ActorGateway taskManagerGateway,
		FiniteDuration jobManagerMessageTimeout) {

		this.executionContext = Preconditions.checkNotNull(executionContext);
		this.jobManagerGateway = Preconditions.checkNotNull(jobManagerGateway);
		this.taskManagerGateway = Preconditions.checkNotNull(taskManagerGateway);
		this.jobManagerMessageTimeout = Preconditions.checkNotNull(jobManagerMessageTimeout);
	}

	public PartitionStateChecker createPartitionStateChecker() {
		return new ActorGatewayPartitionStateChecker(jobManagerGateway, taskManagerGateway);
	}

	public ResultPartitionConsumableNotifier createResultPartitionConsumableNotifier(Task owningTask) {
		return new ActorGatewayResultPartitionConsumableNotifier(
			executionContext,
			jobManagerGateway,
			owningTask,
			jobManagerMessageTimeout);
	}
}
