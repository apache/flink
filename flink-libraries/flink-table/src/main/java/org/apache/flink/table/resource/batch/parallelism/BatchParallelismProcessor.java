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

package org.apache.flink.table.resource.batch.parallelism;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfigOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink;
import org.apache.flink.table.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.plan.nodes.process.DAGProcessor;
import org.apache.flink.table.resource.batch.NodeRunningUnit;
import org.apache.flink.table.resource.batch.parallelism.autoconf.BatchParallelismAdjuster;
import org.apache.flink.table.util.NodeResourceUtil;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Processor for calculating parallelism for {@link BatchExecNode}.
 */
public class BatchParallelismProcessor implements DAGProcessor {

	private TableEnvironment tEnv;

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context) {
		sinkNodes.forEach(s -> Preconditions.checkArgument(s instanceof BatchExecNode));
		tEnv = context.getTableEnvironment();
		List<ExecNode<?, ?>> rootNodes = filterSinkNodes(sinkNodes);
		Map<ExecNode<?, ?>, Integer> nodeToFinalParallelismMap = BatchFinalParallelismSetter.calculate(tEnv, rootNodes);
		Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = ShuffleStageGenerator.generate(rootNodes, nodeToFinalParallelismMap);
		calculateOnShuffleStages(nodeShuffleStageMap, context);
		for (ExecNode<?, ?> node : nodeShuffleStageMap.keySet()) {
			node.getResource().setParallelism(nodeShuffleStageMap.get(node).getParallelism());
		}
		return sinkNodes;
	}

	protected void calculateOnShuffleStages(Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap, DAGProcessContext context) {
		Configuration tableConf = tEnv.getConfig().getConf();
		NodeResourceUtil.InferMode inferMode = NodeResourceUtil.getInferMode(tableConf);
		getShuffleStageParallelismCalculator(tableConf, inferMode).calculate(nodeShuffleStageMap.values());
		Double cpuLimit = tableConf.getDouble(TableConfigOptions.SQL_RESOURCE_RUNNING_UNIT_CPU_TOTAL);
		if (cpuLimit > 0) {
			Map<BatchExecNode<?>, Set<NodeRunningUnit>> nodeRunningUnitMap = context.getRunningUnitMap();
			BatchParallelismAdjuster.adjustParallelism(cpuLimit, nodeRunningUnitMap, nodeShuffleStageMap);
		}
	}

	private BatchShuffleStageParallelismCalculator getShuffleStageParallelismCalculator(
			Configuration tableConf,
			NodeResourceUtil.InferMode inferMode) {
		int envParallelism = tEnv.execEnv().getParallelism();
		if (inferMode.equals(NodeResourceUtil.InferMode.ALL)) {
			return new BatchParallelismCalculatorOnStatistics(tableConf, envParallelism);
		} else {
			return new BatchParallelismCalculatorOnConfig(tableConf, envParallelism);
		}
	}

	private List<ExecNode<?, ?>> filterSinkNodes(List<ExecNode<?, ?>> sinkNodes) {
		List<ExecNode<?, ?>> rootNodes = new ArrayList<>();
		sinkNodes.forEach(s -> {
			if (s instanceof BatchExecSink) {
				rootNodes.add(s.getInputNodes().get(0));
			} else {
				rootNodes.add(s);
			}
		});
		return rootNodes;
	}

}
