/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.resource.batch.schedule;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.schedule.GraphManagerPlugin;
import org.apache.flink.runtime.schedule.SchedulingConfig;
import org.apache.flink.runtime.schedule.VertexScheduler;
import org.apache.flink.table.resource.batch.BatchExecNodeStage;
import org.apache.flink.table.resource.batch.NodeRunningUnit;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.JOB_VERTEX_TO_STREAM_NODE_MAP;

/**
 * Schedule job based on runningUnit.
 */
public class RunningUnitGraphManagerPlugin implements GraphManagerPlugin {

	private static final Logger LOG = LoggerFactory.getLogger(RunningUnitGraphManagerPlugin.class);
	public static final String RUNNING_UNIT_CONF_KEY = "runningUnit.key";

	private Map<JobVertexID, LogicalJobVertex> logicalJobVertices = new LinkedHashMap<>();
	private Map<NodeRunningUnit, LogicalJobVertexRunningUnit> runningUnitMap = new LinkedHashMap<>();
	// for sink who not chain with pre vertex.
	private LogicalJobVertexRunningUnit leftJobVertexRunningUnit;

	private LinkedList<LogicalJobVertexRunningUnit> scheduleQueue = new LinkedList<>();
	private LogicalJobVertexRunningUnit currentScheduledUnit = null;
	private Set<LogicalJobVertexRunningUnit> scheduledRunningUnitSet = Collections.newSetFromMap(new IdentityHashMap<>());

	// A jobVertex producers data and triggers the input of a set of runningUnits.
	private Map<JobVertexID, Set<LogicalJobVertexRunningUnit>> inputRunningUnitMap = new LinkedHashMap<>();
	// build runningUnit -> probe runningUnit
	private Map<LogicalJobVertexRunningUnit, Set<LogicalJobVertexRunningUnit>> joinDependRunningUnitMap = new LinkedHashMap<>();

	private VertexScheduler scheduler;
	private JobGraph jobGraph;

	@Override
	public void open(VertexScheduler scheduler, JobGraph jobGraph, SchedulingConfig schedulingConfig) {
		try {
			Map<JobVertexID, ArrayList<Integer>> vertexToStreamNodeIds = InstantiationUtil.readObjectFromConfig(schedulingConfig.getConfiguration(), JOB_VERTEX_TO_STREAM_NODE_MAP, schedulingConfig.getUserClassLoader());
			List<NodeRunningUnit> nodeRunningUnits = InstantiationUtil.readObjectFromConfig(schedulingConfig.getConfiguration(), RUNNING_UNIT_CONF_KEY, schedulingConfig.getUserClassLoader());
			open(scheduler, jobGraph,  vertexToStreamNodeIds, nodeRunningUnits);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@VisibleForTesting
	public void open(VertexScheduler scheduler,
			JobGraph jobGraph,
			Map<JobVertexID,
			ArrayList<Integer>> vertexToStreamNodeIds,
			List<NodeRunningUnit> nodeRunningUnits) {
		this.scheduler = scheduler;
		this.jobGraph = jobGraph;
		Map<Integer, JobVertexID> streamNodeIdToVertex = reverseMap(vertexToStreamNodeIds);
		buildJobVertices(Arrays.asList(jobGraph.getVerticesAsArray()));
		buildJobRunningUnits(nodeRunningUnits, streamNodeIdToVertex);
	}

	private void buildJobRunningUnits(List<NodeRunningUnit> nodeRunningUnits, Map<Integer, JobVertexID> streamNodeIdToVertex) {

		avoidDeadLockDepend(nodeRunningUnits);

		for (NodeRunningUnit nodeRunningUnit : nodeRunningUnits) {
			LogicalJobVertexRunningUnit jobVertexRunningUnit = transformRunningUnit(nodeRunningUnit, streamNodeIdToVertex);
			joinDependRunningUnitMap.computeIfAbsent(jobVertexRunningUnit, k-> new LinkedHashSet<>());

			// jobVertex runningUnit input depend.
			for (BatchExecNodeStage stage : nodeRunningUnit.getAllNodeStages()) {
				for (BatchExecNodeStage dependStage : stage.getDependStageList(BatchExecNodeStage.DependType.DATA_TRIGGER)) {
					for (int id : dependStage.getTransformationIDList()) {
						JobVertexID inputJobVertexID = streamNodeIdToVertex.get(id);
						if (inputJobVertexID != null) {
							inputRunningUnitMap.computeIfAbsent(inputJobVertexID, k -> new LinkedHashSet<>()).add(jobVertexRunningUnit);
							jobVertexRunningUnit.addInputDepend(inputJobVertexID);
						}
					}
				}
			}
			runningUnitMap.put(nodeRunningUnit, jobVertexRunningUnit);
		}

		for (NodeRunningUnit unit : nodeRunningUnits) {
			for (BatchExecNodeStage stage : unit.getAllNodeStages()) {
				List<BatchExecNodeStage> joinDependStageList = stage.getDependStageList(BatchExecNodeStage.DependType.PRIORITY);
				if (joinDependStageList.isEmpty()) {
					continue;
				}
				LogicalJobVertexRunningUnit probeRunningUnit = runningUnitMap.get(unit);
				for (BatchExecNodeStage dependStage : joinDependStageList) {
					for (NodeRunningUnit dependUnit : dependStage.getRunningUnitList()) {
						LogicalJobVertexRunningUnit buildRunningUnit = runningUnitMap.get(dependUnit);
						joinDependRunningUnitMap.get(buildRunningUnit).add(probeRunningUnit);
						probeRunningUnit.addJoinDepend(buildRunningUnit);
					}
				}
			}
		}

		// deal with left jobIDs
		Set<LogicalJobVertex> jobVertexSet = logicalJobVertices.values().stream()
				.filter(j -> j.getRunningUnitSet().isEmpty())
				.collect(Collectors.toCollection(LinkedHashSet::new));
		leftJobVertexRunningUnit = new LogicalJobVertexRunningUnit(jobVertexSet);
	}

	private LogicalJobVertexRunningUnit transformRunningUnit(NodeRunningUnit nodeRunningUnit, Map<Integer, JobVertexID> streamNodeIdToVertex) {
		List<BatchExecNodeStage> allStages = nodeRunningUnit.getAllNodeStages();

		Set<LogicalJobVertex> jobVertexSet = new LinkedHashSet<>();
		for (BatchExecNodeStage stage : allStages) {
			for (Integer transformationID : stage.getTransformationIDList()) {
				jobVertexSet.add(logicalJobVertices.get(streamNodeIdToVertex.get(transformationID)));
			}
		}
		LogicalJobVertexRunningUnit jobVertexRunningUnit = new LogicalJobVertexRunningUnit(jobVertexSet);
		jobVertexSet.forEach(j -> j.addRunningUnit(jobVertexRunningUnit));
		return jobVertexRunningUnit;
	}

	private void buildJobVertices(List<JobVertex> jobVertices) {
		jobVertices.forEach(j -> logicalJobVertices.put(j.getID(), new LogicalJobVertex(j)));
		jobVertices.forEach(j -> inputRunningUnitMap.put(j.getID(), new LinkedHashSet<>()));
	}

	private Map<Integer, JobVertexID> reverseMap(Map<JobVertexID, ArrayList<Integer>> vertexToStreamNodeIds) {
		Map<Integer, JobVertexID> streamNodeIdToVertex = new LinkedHashMap<>();
		for (Map.Entry<JobVertexID, ArrayList<Integer>> entry : vertexToStreamNodeIds.entrySet()) {
			for (Integer transId : entry.getValue()) {
				streamNodeIdToVertex.put(transId, entry.getKey());
			}
		}
		return streamNodeIdToVertex;
	}

	@Override
	public void close() {
		// do nothing.
	}

	@Override
	public void reset() {

	}

	@Override
	public void onSchedulingStarted() {
		runningUnitMap.values().stream()
				.filter(LogicalJobVertexRunningUnit::allDependReady)
				.forEach(this::addToScheduleQueue);
		checkScheduleNewRunningUnit();
	}

	@Override
	public void onResultPartitionConsumable(ResultPartitionConsumableEvent event) {
		JobVertexID producerJobVertexID = jobGraph.getResultProducerID(event.getResultID());
		produceResultPartition(producerJobVertexID);
		checkScheduleNewRunningUnit();
	}

	private void produceResultPartition(JobVertexID producerJobVertexID) {
		Set<LogicalJobVertexRunningUnit> runningUnitList = inputRunningUnitMap.get(producerJobVertexID);
		for (LogicalJobVertexRunningUnit jobVertexRunningUnit : runningUnitList) {
			jobVertexRunningUnit.receiveInput(producerJobVertexID);
			if (jobVertexRunningUnit.allDependReady()) {
				addToScheduleQueue(jobVertexRunningUnit);
			}
		}
	}

	@Override
	public void onExecutionVertexFailover(ExecutionVertexFailoverEvent event) {
		scheduleQueue.clear();
		currentScheduledUnit = null;
		scheduledRunningUnitSet.clear();
		event.getAffectedExecutionVertexIDs().forEach(t -> logicalJobVertices.get(t.getJobVertexID()).failoverTask());
		runningUnitMap.values().forEach(LogicalJobVertexRunningUnit::reset);
		logicalJobVertices.values().stream().filter(LogicalJobVertex::allTasksDeploying).forEach(j -> {
			allTaskDeploying(j);
			for (int i = 0; i < j.getParallelism(); i++) {
				produceResultPartition(j.getJobVertexID());
			}
		});
		onSchedulingStarted();
	}

	private synchronized void checkScheduleNewRunningUnit() {
		if (currentScheduledUnit == null || currentScheduledUnit.allTasksDeploying()) {
			LogicalJobVertexRunningUnit jobVertexRunningUnit;
			while ((jobVertexRunningUnit = scheduleQueue.pollFirst()) != null) {
				if (jobVertexRunningUnit.allTasksDeploying()) {
					runningUnitAllDeploying(jobVertexRunningUnit);
				} else {
					break;
				}
			}
			if (jobVertexRunningUnit != null) {
				scheduleRunningUnit(jobVertexRunningUnit);
			} else {
				if (scheduledRunningUnitSet.size() >= runningUnitMap.size() && !leftJobVertexRunningUnit.allTasksDeploying()) {
					scheduleRunningUnit(leftJobVertexRunningUnit);
				}
			}
		}
	}

	private synchronized void addToScheduleQueue(LogicalJobVertexRunningUnit jobVertexRunningUnit) {
		if (scheduledRunningUnitSet.add(jobVertexRunningUnit)) {
			scheduleQueue.add(jobVertexRunningUnit);
		}
	}

	private synchronized void scheduleRunningUnit(LogicalJobVertexRunningUnit jobVertexRunningUnit) {
		currentScheduledUnit = jobVertexRunningUnit;
		LOG.info("begin to schedule runningUnit: ");
		currentScheduledUnit.getJobVertexSet().forEach(x-> LOG.info(x.toString()));
		jobVertexRunningUnit.getToScheduleJobVertices().forEach(j -> {
			for (int i = 0; i < j.getParallelism(); i++) {
				scheduler.scheduleExecutionVertices(Collections.singletonList(new ExecutionVertexID(j.getJobVertexID(), i)));
			}
		});
	}

	@Override
	public synchronized void onExecutionVertexStateChanged(ExecutionVertexStateChangedEvent event) {
		if (event.getNewExecutionState() == ExecutionState.DEPLOYING) {
			LogicalJobVertex jobVertex = logicalJobVertices.get(event.getExecutionVertexID().getJobVertexID());
			jobVertex.deployingTask();
			if (jobVertex.allTasksDeploying()) {
				allTaskDeploying(jobVertex);
			}
			checkScheduleNewRunningUnit();
		}
	}

	private synchronized void allTaskDeploying(LogicalJobVertex jobVertex) {
		jobVertex.getRunningUnitSet().stream()
				.filter(LogicalJobVertexRunningUnit::allTasksDeploying)
				.forEach(this::runningUnitAllDeploying);
	}

	private synchronized void runningUnitAllDeploying(LogicalJobVertexRunningUnit jobVertexRunningUnit) {
		scheduledRunningUnitSet.add(jobVertexRunningUnit);
		for (LogicalJobVertexRunningUnit runningUnit : joinDependRunningUnitMap.get(jobVertexRunningUnit)) {
			runningUnit.joinDependDeploying(jobVertexRunningUnit);
			if (runningUnit.allDependReady()) {
				addToScheduleQueue(runningUnit);
			}
		}
	}

	// if loop depend, remove the stage.
	private void avoidDeadLockDepend(List<NodeRunningUnit> nodeRunningUnits) {
		for (NodeRunningUnit unit : nodeRunningUnits) {
			for (BatchExecNodeStage stage : unit.getAllNodeStages()) {
				List<BatchExecNodeStage> toRemoveStages = new LinkedList<>();
				for (BatchExecNodeStage dependStage : stage.getAllDependStageList()) {
					for (NodeRunningUnit dependUnit : dependStage.getRunningUnitList()) {
						Set<NodeRunningUnit> visitedRunningUnit = new HashSet<>();
						if (loopDepend(dependUnit, unit, visitedRunningUnit)) {
							toRemoveStages.add(dependStage);
						}
					}
				}
				for (BatchExecNodeStage toRemove : toRemoveStages) {
					stage.removeDependStage(toRemove);
				}
			}
		}
	}

	private boolean loopDepend(NodeRunningUnit dependUnit, NodeRunningUnit preUnit, Set<NodeRunningUnit> visitedRunningUnit) {
		if (dependUnit == preUnit) {
			return true;
		}
		if (visitedRunningUnit.contains(dependUnit)) {
			return false;
		} else {
			visitedRunningUnit.add(dependUnit);
		}
		for (BatchExecNodeStage containStage : dependUnit.getAllNodeStages()) {
			for (BatchExecNodeStage dependStage : containStage.getAllDependStageList()) {
				for (NodeRunningUnit unit : dependStage.getRunningUnitList()) {
					if (loopDepend(unit, preUnit, visitedRunningUnit)) {
						return true;
					}
				}
			}
		}
		return false;
	}

}
