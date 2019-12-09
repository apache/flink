/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;

import java.io.Serializable;

/**
 * Payload for heartbeats sent from the TaskExecutor to the ResourceManager.
 */
public class TaskExecutorHeartbeatPayload implements Serializable {

	private static final long serialVersionUID = -4556838854992435612L;

	private final SlotReport slotReport;
	private final ClusterPartitionReport clusterPartitionReport;

	public TaskExecutorHeartbeatPayload(SlotReport slotReport, ClusterPartitionReport clusterPartitionReport) {
		this.slotReport = slotReport;
		this.clusterPartitionReport = clusterPartitionReport;
	}

	public SlotReport getSlotReport() {
		return slotReport;
	}

	public ClusterPartitionReport getClusterPartitionReport() {
		return clusterPartitionReport;
	}

	@Override
	public String toString() {
		return "TaskExecutorHeartbeatPayload{" +
			"slotReport=" + slotReport +
			", clusterPartitionReport=" + clusterPartitionReport +
			'}';
	}
}
