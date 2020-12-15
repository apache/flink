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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.common.CommonIntermediateTableScan;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.RequiredShuffle;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * A generator that generates a {@link ExecNode} graph from a graph of {@link FlinkPhysicalRel}s.
 *
 * <p>This traverses the tree of {@link FlinkPhysicalRel} starting from the sinks. At each
 * rel we recursively transform the inputs, then create a {@link ExecNode}.
 * Each rel will be visited only once, that means a rel will only generate one ExecNode instance.
 *
 * <p>Exchange and Union will create a actual node in the {@link ExecNode} graph as the first step,
 * once all ExecNodes' implementation are separated from physical rel, we will use {@link ExecEdge}
 * to replace them.
 */
public class ExecGraphGenerator {

	private final TableConfig tableConfig;
	private final Map<FlinkPhysicalRel, ExecNode<?>> visitedRels;

	public ExecGraphGenerator(TableConfig tableConfig) {
		this.tableConfig = tableConfig;
		this.visitedRels = new IdentityHashMap<>();
	}

	public List<ExecNode<?>> generate(List<FlinkPhysicalRel> relNodes) {
		List<ExecNode<?>> execNodes = new ArrayList<>(relNodes.size());
		for (FlinkPhysicalRel relNode : relNodes) {
			execNodes.add(generate(relNode));
		}
		return execNodes;
	}

	private ExecNode<?> generate(FlinkPhysicalRel rel) {
		ExecNode<?> execNode = visitedRels.get(rel);
		if (execNode != null) {
			return execNode;
		}

		if (rel instanceof CommonIntermediateTableScan) {
			throw new TableException("Intermediate RelNode can't be converted to ExecNode.");
		}

		List<ExecNode<?>> inputNodes = new ArrayList<>();
		for (RelNode input : rel.getInputs()) {
			inputNodes.add(generate((FlinkPhysicalRel) input));
		}

		if (rel instanceof LegacyExecNodeBase) {
			LegacyExecNodeBase<?, ?> baseNode = (LegacyExecNodeBase<?, ?>) rel;
			baseNode.setInputNodes(inputNodes);
			execNode = baseNode;
		} else if (rel instanceof BatchPhysicalExchange) {
			execNode = translateBatchExchange((BatchPhysicalExchange) rel, inputNodes.get(0));
		} else if (rel instanceof StreamPhysicalExchange) {
			execNode = translateStreamExchange((StreamPhysicalExchange) rel, inputNodes.get(0));
		} else {
			throw new TableException(rel.getClass().getSimpleName() + " can't be converted to ExecNode." +
					"This is a bug and should not happen. Please file an issue.");
		}

		visitedRels.put(rel, execNode);
		return execNode;
	}

	private BatchExecExchange translateBatchExchange(BatchPhysicalExchange exchange, ExecNode<?> inputNode) {
		final RequiredShuffle requiredShuffle = getRequiredShuffle(exchange.getDistribution());
		final ExecEdge inputEdge;
		if (tableConfig.getConfiguration().getString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE)
				.equalsIgnoreCase(GlobalDataExchangeMode.ALL_EDGES_BLOCKING.toString())) {
			inputEdge = ExecEdge.builder()
					.requiredShuffle(requiredShuffle)
					.damBehavior(ExecEdge.DamBehavior.BLOCKING)
					.build();
		} else {
			inputEdge = ExecEdge.builder()
					.requiredShuffle(requiredShuffle)
					.damBehavior(ExecEdge.DamBehavior.PIPELINED)
					.build();
		}

		return new BatchExecExchange(
				inputNode,
				inputEdge,
				FlinkTypeFactory.toLogicalRowType(exchange.getRowType()));
	}

	private StreamExecExchange translateStreamExchange(StreamPhysicalExchange exchange, ExecNode<?> inputNode) {
		final RequiredShuffle requiredShuffle = getRequiredShuffle(exchange.getDistribution());
		return new StreamExecExchange(
				inputNode,
				ExecEdge.builder().requiredShuffle(requiredShuffle).build(),
				FlinkTypeFactory.toLogicalRowType(exchange.getRowType()),
				exchange.getRelDetailedDescription());
	}

	private RequiredShuffle getRequiredShuffle(RelDistribution relDistribution) {
		switch (relDistribution.getType()) {
			case ANY:
				return RequiredShuffle.any();
			case BROADCAST_DISTRIBUTED:
				return RequiredShuffle.broadcast();
			case SINGLETON:
				return RequiredShuffle.singleton();
			case HASH_DISTRIBUTED:
				int[] keys = relDistribution.getKeys().stream().mapToInt(v -> v).toArray();
				if (keys.length == 0) {
					return RequiredShuffle.singleton();
				} else {
					// Hash Shuffle requires not empty keys
					return RequiredShuffle.hash(keys);
				}
			default:
				throw new UnsupportedOperationException("Unsupported distribution type: " + relDistribution.getType());
		}
	}
}
