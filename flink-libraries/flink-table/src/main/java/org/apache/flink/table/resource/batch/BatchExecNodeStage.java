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

import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.util.FlinkRelOptUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describe which stage of a batchExecNode.
 * e.g. SortAgg node has two stages, the first stage process input data and the second stage
 * output results. There is a pause between them and the second stage works after the first ends.
 *
 * <p>e.g. Calc node has only a stage, it receives input data and output result in a stage.
 *
 * <p>BatchExecNodeStage has three elements: {@link BatchExecNode}, stageID, other nodeStages that the stage
 * depends on.
 * There are two depend type: DATA_TRIGGER, PRIORITY.
 *
 * <p>e.g. When the first stage of sortAggNode ends, the node needs to output data, trigger the second
 * stage to run. So its depend type is DATA_TRIGGER.
 *
 * <p>e.g. SortAgg node output data to a calc node, but the exchangeMode between them is batch mode(spilling
 * data to disk), so the calc node stage depends on the second stage of sortAgg node. Depend type is DATA_TRIGGER.
 *
 * <p>e.g. HashJoin node has two stages: build stage(receive build input and build hashTable) and
 * probe stage(receive probe input and output result). The hashJoin node prefers to read build input
 * if its build and probe inputs are all ready. So the depend type is PRIORITY.
 *
 * <p>e.g. SortMergeJoin node has three stages: two input stages and output stage. its two input stages
 * are parallel. So the output stage depend on two input stages and depend type is DATA_TRIGGER.
 */
public class BatchExecNodeStage implements Serializable {

	/**
	 * Depend type.
	 */
	public enum DependType {
		DATA_TRIGGER, PRIORITY
	}

	private final transient BatchExecNode<?> batchExecNode;
	private final Set<NodeRunningUnit> runningUnitSet = new LinkedHashSet<>();
	private int stageID;

	// the node stage may depend on many nodeStages, e.g. SortMergeJoinNode.
	private final Map<DependType, List<BatchExecNodeStage>> dependStagesMap = new LinkedHashMap<>();

	/**
	 * There may be more than one transformation in a BatchExecNode, like BatchExecScan.
	 * But these transformations must chain in a jobVertex in runtime.
	 */
	private final List<Integer> transformationIDList = new LinkedList<>();
	private final String nodeName;

	public BatchExecNodeStage(BatchExecNode<?> batchExecNode, int stageID) {
		this.batchExecNode = batchExecNode;
		this.stageID = stageID;
		this.nodeName = FlinkRelOptUtil.getDigest(batchExecNode.getFlinkPhysicalRel(), false);
	}

	public void addTransformation(StreamTransformation<?> transformation) {
		transformationIDList.add(transformation.getId());
	}

	public List<Integer> getTransformationIDList() {
		return transformationIDList;
	}

	public List<BatchExecNodeStage> getDependStageList(DependType type) {
		return dependStagesMap.computeIfAbsent(type, k -> new LinkedList<>());
	}

	public void removeDependStage(BatchExecNodeStage toRemove) {
		for (List<BatchExecNodeStage> stageList : dependStagesMap.values()) {
			stageList.remove(toRemove);
		}
	}

	public void addDependStage(BatchExecNodeStage nodeStage, DependType type) {
		dependStagesMap.computeIfAbsent(type, k -> new LinkedList<>()).add(nodeStage);
	}

	public BatchExecNode<?> getBatchExecNode() {
		return batchExecNode;
	}

	public Set<NodeRunningUnit> getRunningUnitList() {
		return runningUnitSet;
	}

	public void addRunningUnit(NodeRunningUnit nodeRunningUnit) {
		runningUnitSet.add(nodeRunningUnit);
	}

	public List<BatchExecNodeStage> getAllDependStageList() {
		List<BatchExecNodeStage> allStageList = new ArrayList<>();
		dependStagesMap.values().forEach(allStageList::addAll);
		return allStageList;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		BatchExecNodeStage stage = (BatchExecNodeStage) o;

		if (stageID != stage.stageID) {
			return false;
		}
		return batchExecNode != null ? batchExecNode.equals(stage.batchExecNode) : stage.batchExecNode == null;
	}

	@Override
	public int hashCode() {
		int result = batchExecNode != null ? batchExecNode.hashCode() : 0;
		result = 31 * result + stageID;
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("nodeStage(");
		sb.append("batchExecNode=").append(nodeName)
				.append(", stageID=").append(stageID);
		dependStagesMap.forEach((k, v) -> {
			sb.append(", depend type: ").append(k).append(" =[");
			for (BatchExecNodeStage nodeStage : v) {
				sb.append("batchExecNode=").append(nodeStage.nodeName)
						.append(", stageID=").append(nodeStage.stageID).append(";");
			}
			sb.append("]");
		});
		return sb.toString();
	}
}
