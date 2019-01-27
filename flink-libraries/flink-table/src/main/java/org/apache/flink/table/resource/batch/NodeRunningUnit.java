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

package org.apache.flink.table.resource.batch;

import org.apache.flink.table.plan.nodes.exec.BatchExecNode;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * NodeRunningUnit contains some batchExecNodeStages that run at the same time. It can be considered
 * as schedule unit.
 *
 * <p>e.g. source -> calc1 -> sort -> calc2
 *
 * <p>When cal2 begin to receive data from sort, source1 and calc1 finish running.
 * It will be divide to two runningUnits:
 *  1. source -> calc1 -> sort
 *  2. sort -> calc2
 */
public class NodeRunningUnit implements Serializable {

	private final List<BatchExecNodeStage> nodeStageList = new LinkedList<>();
	private transient Set<BatchExecNode<?>> nodeSet = new LinkedHashSet<>();

	public void addNodeStage(BatchExecNodeStage nodeStage) {
		this.nodeStageList.add(nodeStage);
		this.nodeSet.add(nodeStage.getBatchExecNode());
	}

	public Set<BatchExecNode<?>> getNodeSet() {
		return nodeSet;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("NodeRunningUnit{");
		for (BatchExecNodeStage nodeStage : nodeStageList) {
			sb.append("\n\t").append(nodeStage);
		}
		sb.append("\n}");
		return sb.toString();
	}

	public List<BatchExecNodeStage> getAllNodeStages() {
		return nodeStageList;
	}
}
