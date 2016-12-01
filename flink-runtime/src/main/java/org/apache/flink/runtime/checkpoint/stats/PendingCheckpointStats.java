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

package org.apache.flink.runtime.checkpoint.stats;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Statistics for a pending checkpoint.
 */
public class PendingCheckpointStats implements Serializable {

	/** ID of the checkpoint. */
	private final long checkpointId;

	/** Timestamp when the checkpoint was triggered. */
	private final long triggerTimestamp;

	/** Number Of Acknowledged Tasks. */
	private final int numberOfAcknowledgedTasks;

	/** Number Of Not yet Acknowledged Tasks. */
	private final int numberOfNonAcknowledgedTasks;

	/** Not yet Acknowledged Tasks. */
	private final Map<JobVertexID, Set<Integer>> notYetAcknowledgedTasks;

	/**
	 * Creates a pending checkpoint statistic.
	 *
	 * @param checkpointId                   Checkpoint ID
	 * @param triggerTimestamp               Timestamp when the checkpoint was triggered
	 * @param numberOfAcknowledgedTasks      Number Of Acknowledged Tasks
	 * @param numberOfNonAcknowledgedTasks   Number Of Not yet Acknowledged Tasks
	 * @param notYetAcknowledgedTasks        Not yet Acknowledged Tasks
	 */
	public PendingCheckpointStats(
		long checkpointId,
		long triggerTimestamp,
		int numberOfAcknowledgedTasks,
		int numberOfNonAcknowledgedTasks,
		Map<JobVertexID, Set<Integer>> notYetAcknowledgedTasks) {

		this.checkpointId = checkpointId;
		this.triggerTimestamp = triggerTimestamp;
		this.numberOfAcknowledgedTasks = numberOfAcknowledgedTasks;
		this.numberOfNonAcknowledgedTasks = numberOfNonAcknowledgedTasks;
		this.notYetAcknowledgedTasks = notYetAcknowledgedTasks;
	}

	/**
	 * Returns the ID of the checkpoint.
	 *
	 * @return ID of the checkpoint.
	 */
	public long getCheckpointId() {
		return checkpointId;
	}

	/**
	 * Returns the timestamp when the checkpoint was triggered.
	 *
	 * @return Timestamp when the checkpoint was triggered.
	 */
	public long getTriggerTimestamp() {
		return triggerTimestamp;
	}

	/**
	 * Returns the number of acknowledged tasks of the checkpoint.
	 *
	 * @return Number Of acknowledged tasks of the checkpoint.
	 */
	public int getNumberOfAcknowledgedTasks() {
		return numberOfAcknowledgedTasks;
	}

	/**
	 * Returns the number of not yet acknowledged tasks of the checkpoint.
	 *
	 * @return Number of not yet acknowledged tasks of the checkpoint.
	 */
	public int getNumberOfNonAcknowledgedTasks() {
		return numberOfNonAcknowledgedTasks;
	}

	/**
	 * Returns the not yet acknowledged tasks of the checkpoint.
	 *
	 * @return Not yet acknowledged tasks of the checkpoint.
	 */
	public Map<JobVertexID, Set<Integer>> getNotYetAcknowledgedTasks() {
		return notYetAcknowledgedTasks;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Checkpoint{ID=").append(checkpointId).
			append(",triggerTime=").append(triggerTimestamp).
			append(",numOfAckTasks=").append(numberOfAcknowledgedTasks).
			append(",numOfNonAckTasks=").append(numberOfNonAcknowledgedTasks).
			append(",nonAckTasks=[");

		Iterator<Entry<JobVertexID, Set<Integer>>> nonAckTasks = notYetAcknowledgedTasks.entrySet().iterator();
		while (nonAckTasks.hasNext()) {
			Entry<JobVertexID, Set<Integer>> jobVertex = nonAckTasks.next();

			sb.append("JobVertex{ID=").append(jobVertex.getKey()).
				append(",subtaskIDs=").append(jobVertex.getValue()).append("}");

			if (nonAckTasks.hasNext()) {
				sb.append(",");
			}
		}

		sb.append("]}");

		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		PendingCheckpointStats that = (PendingCheckpointStats) o;

		if (getCheckpointId() != that.getCheckpointId() ||
			getTriggerTimestamp() != that.getTriggerTimestamp() ||
			getNumberOfAcknowledgedTasks() != that.getNumberOfAcknowledgedTasks() ||
			getNumberOfNonAcknowledgedTasks() != that.getNumberOfNonAcknowledgedTasks() ||
			getNotYetAcknowledgedTasks().size() != that.getNotYetAcknowledgedTasks().size()) {
			return false;
		}

		for (Entry<JobVertexID, Set<Integer>> jobVertex : that.getNotYetAcknowledgedTasks().entrySet()) {
			if ((!notYetAcknowledgedTasks.containsKey(jobVertex.getKey())) ||
				notYetAcknowledgedTasks.get(jobVertex.getKey()).size() != jobVertex.getValue().size()) {
				return false;
			}

			for (Integer subtaskId : notYetAcknowledgedTasks.get(jobVertex.getKey())) {
				if (!jobVertex.getValue().contains(subtaskId)) {
					return false;
				}
			}
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = (int) (getCheckpointId() ^ (getCheckpointId() >>> 32));
		result = 31 * result + (int) (getTriggerTimestamp() ^ (getTriggerTimestamp() >>> 32));
		result = 31 * result + (int) (getNumberOfAcknowledgedTasks() ^ (getNumberOfAcknowledgedTasks() >>> 32));
		result = 31 * result + (int) (getNumberOfNonAcknowledgedTasks() ^ (getNumberOfNonAcknowledgedTasks() >>> 32));
		result = 31 * result + (int) (notYetAcknowledgedTasks.size() ^ (notYetAcknowledgedTasks.size() >>> 32));
		return result;
	}
}
