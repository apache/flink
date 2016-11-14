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

import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.JobCheckpointStats;
import org.apache.flink.runtime.checkpoint.stats.OperatorCheckpointStats;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import scala.Option;

import java.io.Serializable;
import java.util.Map;

public class ArchivedCheckpointStatsTracker implements CheckpointStatsTracker, Serializable {
	private static final long serialVersionUID = 1469003563086353555L;

	private final Option<JobCheckpointStats> jobStats;
	private final Map<JobVertexID, OperatorCheckpointStats> operatorStats;

	public ArchivedCheckpointStatsTracker(Option<JobCheckpointStats> jobStats, Map<JobVertexID, OperatorCheckpointStats> operatorStats) {
		this.jobStats = jobStats;
		this.operatorStats = operatorStats;
	}

	@Override
	public void onCompletedCheckpoint(CompletedCheckpoint checkpoint) {
	}

	@Override
	public Option<JobCheckpointStats> getJobStats() {
		return jobStats;
	}

	@Override
	public Option<OperatorCheckpointStats> getOperatorStats(JobVertexID operatorId) {
		return Option.apply(operatorStats.get(operatorId));
	}
}
