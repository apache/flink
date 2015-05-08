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

package org.apache.flink.runtime.checkpoint;

import akka.actor.UntypedActor;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;

/**
 * This actor listens to changes in the JobStatus and activates or deactivates the periodic
 * checkpoint scheduler.
 */
public class CheckpointCoordinatorDeActivator extends UntypedActor {

	private final CheckpointCoordinator coordinator;
	private final long interval;
	
	public CheckpointCoordinatorDeActivator(CheckpointCoordinator coordinator, long interval) {
		this.coordinator = coordinator;
		this.interval = interval;
	}

	@Override
	public void onReceive(Object message) {
		if (message instanceof ExecutionGraphMessages.JobStatusChanged) {
			JobStatus status = ((ExecutionGraphMessages.JobStatusChanged) message).newJobStatus();
			
			if (status == JobStatus.RUNNING) {
				// start the checkpoint scheduler
				coordinator.startPeriodicCheckpointScheduler(interval);
			}
			else {
				// anything else should stop the trigger for now
				coordinator.stopPeriodicCheckpointScheduler();
			}
		}
		
		// we ignore all other messages
	}
}
