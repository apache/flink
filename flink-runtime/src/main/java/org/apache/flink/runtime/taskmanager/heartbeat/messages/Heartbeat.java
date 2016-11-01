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

package org.apache.flink.runtime.taskmanager.heartbeat.messages;

import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;

/**
 * Heartbeat message sent by the {@link org.apache.flink.runtime.taskmanager.heartbeat.HeartbeatActor}
 * to the heartbeat target.
 */
public class Heartbeat implements Serializable {

	private static final long serialVersionUID = -4741472008424382598L;

	/** Instance ID identifying the sender */
	private final InstanceID instanceId;

	/** Heartbeat payload: Set of accumulator snapshots */
	private final Collection<AccumulatorSnapshot> accumulatorSnapshots;

	public Heartbeat(InstanceID instanceId, Collection<AccumulatorSnapshot> accumulatorSnapshots) {
		this.instanceId = Preconditions.checkNotNull(instanceId);
		this.accumulatorSnapshots = Preconditions.checkNotNull(accumulatorSnapshots);
	}

	public InstanceID getInstanceId() {
		return instanceId;
	}

	public Collection<AccumulatorSnapshot> getAccumulatorSnapshots() {
		return accumulatorSnapshots;
	}
}
