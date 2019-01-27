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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCorrelate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExpand;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecNestedLoopJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecOverAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRank;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSort;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTemporalTableJoin;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Visit every batchExecNode to build runningUnits.
 */
public class RunningUnitGenerator extends BatchExecNodeVisitor {

	private final Map<BatchExecNode<?>, List<NodeStageExchangeInfo>> outputInfoMap = new LinkedHashMap<>();
	private final List<NodeRunningUnit> runningUnits = new LinkedList<>();
	private final Configuration tableConf;

	public RunningUnitGenerator(Configuration tableConf) {
		this.tableConf = tableConf;
	}

	public List<NodeRunningUnit> getRunningUnits() {
		return runningUnits;
	}

	private void addIntoInputRunningUnit(List<NodeStageExchangeInfo> inputInfoList, BatchExecNodeStage nodeStage) {
		for (NodeStageExchangeInfo inputInfo : inputInfoList) {
			if (inputInfo.exchangeMode == DataExchangeMode.BATCH) {
				nodeStage.addDependStage(inputInfo.outStage, BatchExecNodeStage.DependType.DATA_TRIGGER);
			} else {
				for (NodeRunningUnit inputRunningUnit : inputInfo.outStage.getRunningUnitList()) {
					inputRunningUnit.addNodeStage(nodeStage);
					nodeStage.addRunningUnit(inputRunningUnit);
				}
			}
			if (nodeStage.getRunningUnitList().isEmpty()) {
				newRunningUnitWithNodeStage(nodeStage);
			}
		}
	}

	private void newRunningUnitWithNodeStage(BatchExecNodeStage nodeStage) {
		NodeRunningUnit runningUnit = new NodeRunningUnit();
		runningUnits.add(runningUnit);
		runningUnit.addNodeStage(nodeStage);
		nodeStage.addRunningUnit(runningUnit);
	}

	@Override
	public void visit(BatchExecBoundedStreamScan boundedStreamScan) {
		visitSource(boundedStreamScan);
	}

	@Override
	public void visit(BatchExecTableSourceScan scanTableSource) {
		visitSource(scanTableSource);
	}

	@Override
	public void visit(BatchExecValues values) {
		visitSource(values);
	}

	private void visitSource(BatchExecNode<?> sourceNode) {
		List<NodeStageExchangeInfo> outputInfoList = outputInfoMap.get(sourceNode);
		if (outputInfoList == null) {
			BatchExecNodeStage nodeStage = new BatchExecNodeStage(sourceNode, 0);
			newRunningUnitWithNodeStage(nodeStage);
			outputInfoList = Collections.singletonList(new NodeStageExchangeInfo(nodeStage));
			outputInfoMap.put(sourceNode, outputInfoList);
		}
	}

	private List<NodeStageExchangeInfo> visitOneStageSingleNode(BatchExecNode<?> singleNode) {
		List<NodeStageExchangeInfo> outputInfoList = outputInfoMap.get(singleNode);
		if (outputInfoList == null) {
			BatchExecNode<?> input = (BatchExecNode<?>) singleNode.getInputNodes().get(0);
			input.accept(this);
			List<NodeStageExchangeInfo> inputInfoList = outputInfoMap.get(input);
			BatchExecNodeStage nodeStage = new BatchExecNodeStage(singleNode, 0);
			addIntoInputRunningUnit(inputInfoList, nodeStage);
			outputInfoList = Collections.singletonList(new NodeStageExchangeInfo(nodeStage));
			outputInfoMap.put(singleNode, outputInfoList);
		}
		return outputInfoList;
	}

	@Override
	public void visit(BatchExecCalc calc) {
		visitOneStageSingleNode(calc);
	}

	@Override
	public void visit(BatchExecCorrelate correlate) {
		visitOneStageSingleNode(correlate);
	}

	@Override
	public void visit(BatchExecExpand expand) {
		visitOneStageSingleNode(expand);
	}

	@Override
	public void visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		visitOneStageSingleNode(localSortAggregate);
	}

	@Override
	public void visit(BatchExecSortWindowAggregate sortAggregate) {
		visitOneStageSingleNode(sortAggregate);
	}

	@Override
	public void visit(BatchExecOverAggregate overWindowAgg) {
		visitOneStageSingleNode(overWindowAgg);
	}

	@Override
	public void visit(BatchExecLimit limit) {
		visitOneStageSingleNode(limit);
	}

	@Override
	public void visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		visitOneStageSingleNode(localHashAggregate);
	}

	@Override
	public void visit(BatchExecTemporalTableJoin joinTable) {
		visitOneStageSingleNode(joinTable);
	}

	@Override
	public void visit(BatchExecHashWindowAggregate hashAggregate) {
		visitTwoStageSingleNode(hashAggregate);
	}

	private List<NodeStageExchangeInfo> visitTwoStageSingleNode(BatchExecNode<?> singleNode) {
		List<NodeStageExchangeInfo> outputInfoList = outputInfoMap.get(singleNode);
		if (outputInfoList == null) {
			BatchExecNode<?> input = (BatchExecNode<?>) singleNode.getInputNodes().get(0);
			input.accept(this);
			List<NodeStageExchangeInfo> inputInfoList = outputInfoMap.get(input);
			BatchExecNodeStage inStage = new BatchExecNodeStage(singleNode, 0);
			BatchExecNodeStage outStage = new BatchExecNodeStage(singleNode, 1);
			outStage.addDependStage(inStage, BatchExecNodeStage.DependType.DATA_TRIGGER);
			addIntoInputRunningUnit(inputInfoList, inStage);
			newRunningUnitWithNodeStage(outStage);

			outputInfoList = Collections.singletonList(new NodeStageExchangeInfo(outStage));
			outputInfoMap.put(singleNode, outputInfoList);
		}
		return outputInfoList;
	}

	@Override
	public void visit(BatchExecLocalHashAggregate localHashAggregate) {
		if (localHashAggregate.getGrouping().length == 0) {
			visitTwoStageSingleNode(localHashAggregate);
		} else {
			visitOneStageSingleNode(localHashAggregate);
		}
	}

	@Override
	public void visit(BatchExecSortAggregate sortAggregate) {
		if (sortAggregate.getGrouping().length == 0) {
			visitTwoStageSingleNode(sortAggregate);
		} else {
			visitOneStageSingleNode(sortAggregate);
		}
	}

	@Override
	public void visit(BatchExecLocalSortAggregate localSortAggregate) {
		if (localSortAggregate.getGrouping().length == 0) {
			visitTwoStageSingleNode(localSortAggregate);
		} else {
			visitOneStageSingleNode(localSortAggregate);
		}
	}

	@Override
	public void visit(BatchExecSort sort) {
		visitTwoStageSingleNode(sort);
	}

	@Override
	public void visit(BatchExecSortLimit sortLimit) {
		visitTwoStageSingleNode(sortLimit);
	}

	@Override
	public void visit(BatchExecRank rank) {
		visitOneStageSingleNode(rank);
	}

	@Override
	public void visit(BatchExecHashAggregate hashAggregate) {
		visitTwoStageSingleNode(hashAggregate);
	}

	private List<NodeStageExchangeInfo> visitBuildProbeJoin(BatchExecJoinBase hashJoin,
			boolean leftIsBuild) {
		List<NodeStageExchangeInfo> outputInfoList = outputInfoMap.get(hashJoin);
		if (outputInfoList == null) {
			BatchExecNodeStage buildStage = new BatchExecNodeStage(hashJoin, 0);
			BatchExecNodeStage probeStage = new BatchExecNodeStage(hashJoin, 1);
			probeStage.addDependStage(buildStage, BatchExecNodeStage.DependType.PRIORITY);
			BatchExecNode<?>buildInput = (BatchExecNode<?>) (leftIsBuild ? hashJoin.getInputNodes().get(0) : hashJoin.getInputNodes().get(1));
			BatchExecNode<?> probeInput = (BatchExecNode<?>) (leftIsBuild ? hashJoin.getInputNodes().get(1) : hashJoin.getInputNodes().get(0));

			buildInput.accept(this);
			List<NodeStageExchangeInfo> buildInputInfoList = outputInfoMap.get(buildInput);

			probeInput.accept(this);
			List<NodeStageExchangeInfo> probeInputInfoList = outputInfoMap.get(probeInput);

			addIntoInputRunningUnit(buildInputInfoList, buildStage);
			addIntoInputRunningUnit(probeInputInfoList, probeStage);

			outputInfoList = Collections.singletonList(new NodeStageExchangeInfo(probeStage));
			outputInfoMap.put(hashJoin, outputInfoList);
		}
		return outputInfoList;
	}

	@Override
	public void visit(BatchExecExchange exchange) {
		List<NodeStageExchangeInfo> outputInfoList = outputInfoMap.get(exchange);
		if (outputInfoList == null) {
			BatchExecNode<?> input = (BatchExecNode<?>) exchange.getInput();
			input.accept(this);
			List<NodeStageExchangeInfo> inputInfoList = outputInfoMap.get(input);
			if (exchange.getDataExchangeModeForDeadlockBreakup(tableConf) == DataExchangeMode.BATCH) {
				outputInfoList = new ArrayList<>(inputInfoList.size());
				for (NodeStageExchangeInfo nodeStageExchangeInfo : inputInfoList) {
					outputInfoList.add(new NodeStageExchangeInfo(nodeStageExchangeInfo.outStage, DataExchangeMode.BATCH));
				}
			} else {
				outputInfoList = inputInfoList;
			}
			outputInfoMap.put(exchange, outputInfoList);
		}
	}

	@Override
	public void visit(BatchExecHashJoinBase hashJoin) {
		if (hashJoin.hashJoinType().buildLeftSemiOrAnti()) {
			List<NodeStageExchangeInfo> outputInfoList = outputInfoMap.get(hashJoin);
			if (outputInfoList == null) {
				BatchExecNodeStage buildStage = new BatchExecNodeStage(hashJoin, 0);
				BatchExecNodeStage probeStage = new BatchExecNodeStage(hashJoin, 1);
				BatchExecNodeStage outStage = new BatchExecNodeStage(hashJoin, 2);
				probeStage.addDependStage(buildStage, BatchExecNodeStage.DependType.PRIORITY);
				outStage.addDependStage(probeStage, BatchExecNodeStage.DependType.DATA_TRIGGER);

				BatchExecNode<?> buildInput = (BatchExecNode<?>) hashJoin.getInputNodes().get(0);
				BatchExecNode<?> probeInput = (BatchExecNode<?>) hashJoin.getInputNodes().get(1);

				buildInput.accept(this);
				List<NodeStageExchangeInfo> buildInputInfoList = outputInfoMap.get(buildInput);

				probeInput.accept(this);
				List<NodeStageExchangeInfo> probeInputInfoList = outputInfoMap.get(probeInput);

				addIntoInputRunningUnit(buildInputInfoList, buildStage);
				addIntoInputRunningUnit(probeInputInfoList, probeStage);
				newRunningUnitWithNodeStage(outStage);

				outputInfoList = Collections.singletonList(new NodeStageExchangeInfo(outStage));
				outputInfoMap.put(hashJoin, outputInfoList);
			}
		}
		visitBuildProbeJoin(hashJoin, hashJoin.leftIsBuild());
	}

	@Override
	public void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		List<NodeStageExchangeInfo> outputInfoList = outputInfoMap.get(sortMergeJoin);
		if (outputInfoList == null) {
			BatchExecNodeStage in0Stage = new BatchExecNodeStage(sortMergeJoin, 0);
			BatchExecNodeStage in1Stage = new BatchExecNodeStage(sortMergeJoin, 1);
			BatchExecNodeStage outStage = new BatchExecNodeStage(sortMergeJoin, 2);
			// in0Stage and in1Stage can be parallel
			outStage.addDependStage(in0Stage, BatchExecNodeStage.DependType.DATA_TRIGGER);
			outStage.addDependStage(in1Stage, BatchExecNodeStage.DependType.DATA_TRIGGER);

			BatchExecNode<?> leftInput = (BatchExecNode<?>) sortMergeJoin.getInputNodes().get(0);
			leftInput.accept(this);
			List<NodeStageExchangeInfo> in0InfoList = outputInfoMap.get(leftInput);

			BatchExecNode<?> rightInput = (BatchExecNode<?>) sortMergeJoin.getInputNodes().get(1);
			rightInput.accept(this);
			List<NodeStageExchangeInfo> in1InfoList = outputInfoMap.get(rightInput);

			addIntoInputRunningUnit(in0InfoList, in0Stage);
			addIntoInputRunningUnit(in1InfoList, in1Stage);
			newRunningUnitWithNodeStage(outStage);

			outputInfoList = Collections.singletonList(new NodeStageExchangeInfo(outStage));
			outputInfoMap.put(sortMergeJoin, outputInfoList);
		}
	}

	@Override
	public void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		visitBuildProbeJoin(nestedLoopJoin, nestedLoopJoin.leftIsBuild());
	}

	@Override
	public void visit(BatchExecUnion union) {
		List<NodeStageExchangeInfo> outputInfoList = outputInfoMap.get(union);
		if (outputInfoList == null) {
			outputInfoList = new LinkedList<>();
			for (ExecNode<?, ?> input : union.getInputNodes()) {
				((BatchExecNode<?>) input).accept(this);
				outputInfoList.addAll(outputInfoMap.get((BatchExecNode<?>) input));
			}
			outputInfoMap.put(union, outputInfoList);
		}
	}

	@Override
	public void visit(BatchExecSink<?> sink) {
		throw new TableException("could not reach sink here.");
	}

	/**
	 * NodeStage with exchange info.
	 */
	protected static class NodeStageExchangeInfo {

		private final BatchExecNodeStage outStage;
		private final DataExchangeMode exchangeMode;

		public NodeStageExchangeInfo(BatchExecNodeStage outStage) {
			this(outStage, null);
		}

		public NodeStageExchangeInfo(BatchExecNodeStage outStage, DataExchangeMode exchangeMode) {
			this.outStage = outStage;
			this.exchangeMode = exchangeMode;
		}
	}
}
