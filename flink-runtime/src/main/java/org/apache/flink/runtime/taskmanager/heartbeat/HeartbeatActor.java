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

package org.apache.flink.runtime.taskmanager.heartbeat;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Kill;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.Heartbeat;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.HeartbeatResponse;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.HeartbeatTimeout;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.RequestAccumulatorSnapshots;
import org.apache.flink.runtime.taskmanager.heartbeat.messages.UpdateAccumulatorSnapshots;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

/**
 * Actor which monitors the given heartbeat target by sending constantly heartbeat request to this
 * target. If the heartbeat response fails to appear for a given heartbeat pause, then the owner
 * of is notified about the heartbeat timeout.
 *
 * The heartbeat pause can be extended if the actor detects that it has been stalled due to garbage
 * collection, for example.
 */
public class HeartbeatActor extends FlinkUntypedActor {

	/** Receiver of the heartbeat timeout and provider of the accumulator snapshots */
	private final ActorRef owner;

	/** Instance id of the owner */
	private final InstanceID instanceId;

	private final ActorRef heartbeatTarget;

	private final UUID leaderId;

	private final FiniteDuration heartbeatInterval;

	/** Initial acceptable heartbeat pause */
	private final long initialHeartbeatPause;

	/** Maximum acceptable heartbeat pause including garbage collection pauses */
	private final long maxHeartbeatPause;

	private final Logger log;

	private Cancellable heartbeatCancellable;

	private Collection<AccumulatorSnapshot> lastAccumulatorSnapshot;

	/** Current acceptable heartbeat pause */
	private long acceptableHeartbeatPause;

	/** Time of last sent heartbeat request */
	private long lastHeartbeatRequest;

	/** Time of last received heartbeat response */
	private long lastHeartbeatResponse;

	private boolean running;

	public HeartbeatActor(
			ActorRef owner,
			InstanceID instanceId,
			ActorRef heartbeatTarget,
			UUID leaderId,
			FiniteDuration heartbeatInterval,
			FiniteDuration initialHeartbeatPause,
			FiniteDuration maxHeartbeatPause,
			Logger log) {

		this.owner = Preconditions.checkNotNull(owner);
		this.instanceId = Preconditions.checkNotNull(instanceId);
		this.heartbeatTarget = Preconditions.checkNotNull(heartbeatTarget);
		this.leaderId = leaderId;

		Preconditions.checkNotNull(heartbeatInterval);
		Preconditions.checkArgument(heartbeatInterval.toMillis() > 0L,
			"The heartbeat interval must be larger than 0.");
		this.heartbeatInterval = heartbeatInterval;

		Preconditions.checkNotNull(initialHeartbeatPause);
		Preconditions.checkArgument(initialHeartbeatPause.compare(heartbeatInterval) >= 0,
			"The acceptable heartbeat pause should be larger than the heartbeat interval.");
		this.initialHeartbeatPause = initialHeartbeatPause.toMillis();

		Preconditions.checkNotNull(maxHeartbeatPause);
		Preconditions.checkArgument(maxHeartbeatPause.compare(initialHeartbeatPause) >= 0,
			"The max heartbeat interval must be larger than the heartbeat interval.");
		this.maxHeartbeatPause = maxHeartbeatPause.toMillis();

		this.log = Preconditions.checkNotNull(log);

		lastAccumulatorSnapshot = Collections.emptyList();
		heartbeatCancellable = null;
		acceptableHeartbeatPause = 0L;
		lastHeartbeatRequest = 0L;
		lastHeartbeatResponse = 0L;

		running = true;
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();

		lastHeartbeatRequest = System.currentTimeMillis();
		lastHeartbeatResponse = lastHeartbeatRequest;

		acceptableHeartbeatPause = initialHeartbeatPause;

		log.info("Start sending heartbeat requests to {}.", heartbeatTarget.path());

		heartbeatCancellable = getContext().system().scheduler().schedule(
			heartbeatInterval,
			heartbeatInterval,
			getSelf(),
			decorateMessage(SendHeartbeat.getInstance()),
			getContext().dispatcher(),
			getSelf());
	}

	@Override
	public void postStop() throws Exception {
		log.info("Stop sending heartbeat requests to {}.", heartbeatTarget.path());
		stopHeartbeat();

		super.postStop();
	}

	@Override
	protected void handleMessage(Object message) throws Exception {
		if (running) {
			if (message instanceof SendHeartbeat) {
				// 1. Adapt timeoutValue for garbage collection pause
				long currentTime = System.currentTimeMillis();
				long intervalDeviation = Math.max(currentTime - lastHeartbeatRequest - heartbeatInterval.toMillis(), 0L);

				lastHeartbeatRequest = currentTime;

				acceptableHeartbeatPause = Math.min(acceptableHeartbeatPause + intervalDeviation, maxHeartbeatPause);

				// 2. check whether we've timed out
				if (currentTime > lastHeartbeatResponse + acceptableHeartbeatPause) {
					log.debug("Heartbeat timed out for {}@{}.", leaderId, heartbeatTarget.path());

					// too bad, the job manager seems to be dead. Let's notify the TM
					owner.tell(
						decorateMessage(new HeartbeatTimeout(heartbeatTarget, leaderId)),
						getSelf());
				} else {
					// 1. trigger accumulator update
					owner.tell(
						decorateMessage(RequestAccumulatorSnapshots.getInstance()), getSelf());

					// 2. send heartbeat
					heartbeatTarget.tell(
						decorateMessage(new Heartbeat(instanceId, lastAccumulatorSnapshot)),
						getSelf());
				}
			} else if (message instanceof HeartbeatResponse) {
				// let's update the last seen heartbeat response
				lastHeartbeatResponse = System.currentTimeMillis();
				acceptableHeartbeatPause = initialHeartbeatPause;
				log.debug("Received heartbeat response for {}.", leaderId);

			} else if (message instanceof UpdateAccumulatorSnapshots) {
				UpdateAccumulatorSnapshots updateAccumulatorSnapshots = (UpdateAccumulatorSnapshots) message;

				lastAccumulatorSnapshot = updateAccumulatorSnapshots.getAccumulatorSnapshots();
			} else {
				log.warn("Received unknown message {}. Discarding this message.", message);
			}
		} else {
			log.warn("Received message {} after being stopped. Commit suicide.", message);
			getSelf().tell(Kill.getInstance(), getSelf());
		}
	}

	@Override
	protected UUID getLeaderSessionID() {
		return leaderId;
	}

	private void stopHeartbeat() {
		running = false;

		if (heartbeatCancellable != null) {
			heartbeatCancellable.cancel();
			heartbeatCancellable = null;
		}
	}

	private static final class SendHeartbeat implements Serializable, RequiresLeaderSessionID {

		private static final long serialVersionUID = -6248028961658787549L;

		private static final SendHeartbeat INSTANCE = new SendHeartbeat();

		public static SendHeartbeat getInstance() {
			return INSTANCE;
		}

		private SendHeartbeat() {}
	}
}
