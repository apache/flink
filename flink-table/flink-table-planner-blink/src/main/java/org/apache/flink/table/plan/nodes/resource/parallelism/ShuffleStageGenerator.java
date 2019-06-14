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

package org.apache.flink.table.plan.nodes.resource.parallelism;

import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecUnion;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Exchange;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Build exec nodes to shuffleStages according to {@link BatchExecExchange}.
 * If there is data shuffle between two adjacent exec nodes,
 * they are belong to different shuffleStages.
 * If there is no data shuffle between two adjacent exec nodes, but
 * they have different final parallelism, they are also belong to different shuffleStages.
 */
public class ShuffleStageGenerator {

	private final Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = new LinkedHashMap<>();
	private final Map<ExecNode<?, ?>, Integer> nodeToFinalParallelismMap;

	private ShuffleStageGenerator(Map<ExecNode<?, ?>, Integer> nodeToFinalParallelismMap) {
		this.nodeToFinalParallelismMap = nodeToFinalParallelismMap;
	}

	public static Map<ExecNode<?, ?>, ShuffleStage> generate(List<ExecNode<?, ?>> sinkNodes, Map<ExecNode<?, ?>, Integer> finalParallelismNodeMap) {
		ShuffleStageGenerator generator = new ShuffleStageGenerator(finalParallelismNodeMap);
		sinkNodes.forEach(generator::buildShuffleStages);
		Map<ExecNode<?, ?>, ShuffleStage> result = generator.getNodeShuffleStageMap();
		result.values().forEach(s -> {
			List<ExecNode<?, ?>> virtualNodeList = s.getExecNodeSet().stream().filter(ShuffleStageGenerator::isVirtualNode).collect(toList());
			virtualNodeList.forEach(s::removeNode);
		});
		return generator.getNodeShuffleStageMap().entrySet().stream()
				.filter(x -> !isVirtualNode(x.getKey()))
				.collect(Collectors.toMap(Map.Entry::getKey,
						Map.Entry::getValue,
						(e1, e2) -> e1,
						LinkedHashMap::new));
	}

	private void buildShuffleStages(ExecNode<?, ?> execNode) {
		if (nodeShuffleStageMap.containsKey(execNode)) {
			return;
		}
		for (ExecNode<?, ?> input : execNode.getInputNodes()) {
			buildShuffleStages((input));
		}

		if (execNode.getInputNodes().isEmpty()) {
			// source node
			ShuffleStage shuffleStage = new ShuffleStage();
			shuffleStage.addNode(execNode);
			if (nodeToFinalParallelismMap.containsKey(execNode)) {
				shuffleStage.setParallelism(nodeToFinalParallelismMap.get(execNode), true);
			}
			nodeShuffleStageMap.put(execNode, shuffleStage);
		} else if (execNode instanceof Exchange && !isRangeExchange((Exchange) execNode)) {
				// do nothing.
		} else {
			Set<ShuffleStage> inputShuffleStages = getInputShuffleStages(execNode);
			Integer parallelism = nodeToFinalParallelismMap.get(execNode);
			ShuffleStage inputShuffleStage = mergeInputShuffleStages(inputShuffleStages, parallelism);
			inputShuffleStage.addNode(execNode);
			nodeShuffleStageMap.put(execNode, inputShuffleStage);
		}
	}

	private boolean isRangeExchange(Exchange exchange) {
		return exchange.getDistribution().getType() == RelDistribution.Type.RANGE_DISTRIBUTED;
	}

	private ShuffleStage mergeInputShuffleStages(Set<ShuffleStage> shuffleStageSet, Integer parallelism) {
		if (parallelism != null) {
			ShuffleStage resultShuffleStage = new ShuffleStage();
			resultShuffleStage.setParallelism(parallelism, true);
			for (ShuffleStage shuffleStage : shuffleStageSet) {
				//consider max parallelism.
				if ((shuffleStage.isFinalParallelism() && shuffleStage.getParallelism() == parallelism)
					|| (!shuffleStage.isFinalParallelism() && shuffleStage.getMaxParallelism() >= parallelism)) {
					mergeShuffleStage(resultShuffleStage, shuffleStage);
				}
			}
			return resultShuffleStage;
		} else {
			ShuffleStage resultShuffleStage = shuffleStageSet.stream()
					.filter(ShuffleStage::isFinalParallelism)
					.max(Comparator.comparing(ShuffleStage::getParallelism))
					.orElse(new ShuffleStage());
			for (ShuffleStage shuffleStage : shuffleStageSet) {
				//consider max parallelism.
				if ((shuffleStage.isFinalParallelism() && shuffleStage.getParallelism() == resultShuffleStage.getParallelism())
						|| (!shuffleStage.isFinalParallelism() && shuffleStage.getMaxParallelism() >= resultShuffleStage.getParallelism())) {
					mergeShuffleStage(resultShuffleStage, shuffleStage);
				}
			}
			return resultShuffleStage;
		}
	}

	private void mergeShuffleStage(ShuffleStage shuffleStage, ShuffleStage other) {
		Set<ExecNode<?, ?>> nodeSet = other.getExecNodeSet();
		shuffleStage.addNodeSet(nodeSet);
		for (ExecNode<?, ?> r : nodeSet) {
			nodeShuffleStageMap.put(r, shuffleStage);
		}
	}

	private Set<ShuffleStage> getInputShuffleStages(ExecNode<?, ?> node) {
		Set<ShuffleStage> shuffleStageList = new HashSet<>();
		for (ExecNode<?, ?> input : node.getInputNodes()) {
			ShuffleStage oneInputShuffleStage = nodeShuffleStageMap.get(input);
			if (oneInputShuffleStage != null) {
				shuffleStageList.add(oneInputShuffleStage);
			}
		}
		return shuffleStageList;
	}

	private static boolean isVirtualNode(ExecNode<?, ?> node) {
		return node instanceof BatchExecUnion || node instanceof StreamExecUnion;
	}

	private Map<ExecNode<?, ?>, ShuffleStage> getNodeShuffleStageMap() {
		return nodeShuffleStageMap;
	}
}

