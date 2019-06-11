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

package org.apache.flink.table.plan.nodes.resource.batch.parallelism;

import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;

import org.apache.calcite.rel.RelDistribution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Set final parallelism if needed at the beginning time, if parallelism of a node is set to be final,
 * it will not be changed by other parallelism calculator.
 */
public class BatchFinalParallelismSetter {

	private final TableEnvironment tEnv;
	private Set<ExecNode<?, ?>> calculatedNodeSet = new HashSet<>();
	private Map<ExecNode<?, ?>, Integer> finalParallelismNodeMap = new HashMap<>();

	private BatchFinalParallelismSetter(TableEnvironment tEnv) {
		this.tEnv = tEnv;
	}

	/**
	 * Finding nodes that need to set final parallelism.
	 */
	public static Map<ExecNode<?, ?>, Integer> calculate(TableEnvironment tEnv, List<ExecNode<?, ?>> sinkNodes) {
		BatchFinalParallelismSetter setter = new BatchFinalParallelismSetter(tEnv);
		sinkNodes.forEach(setter::calculate);
		return setter.finalParallelismNodeMap;
	}

	private void calculate(ExecNode<?, ?> batchExecNode) {
		if (!calculatedNodeSet.add(batchExecNode)) {
			return;
		}
		if (batchExecNode instanceof BatchExecTableSourceScan) {
			calculateTableSource((BatchExecTableSourceScan) batchExecNode);
		} else if (batchExecNode instanceof BatchExecBoundedStreamScan) {
			calculateBoundedStreamScan((BatchExecBoundedStreamScan) batchExecNode);
		} else if (batchExecNode instanceof BatchExecValues) {
			calculateValues((BatchExecValues) batchExecNode);
		} else {
			calculateSingleton(batchExecNode);
		}
	}

	private void calculateTableSource(BatchExecTableSourceScan tableSourceScan) {
		StreamTransformation transformation = tableSourceScan.getSourceTransformation(tEnv.streamEnv());
		if (transformation.getMaxParallelism() > 0) {
			finalParallelismNodeMap.put(tableSourceScan, transformation.getMaxParallelism());
		}
	}

	private void calculateBoundedStreamScan(BatchExecBoundedStreamScan boundedStreamScan) {
		StreamTransformation transformation = boundedStreamScan.getSourceTransformation();
		int parallelism = transformation.getParallelism();
		if (parallelism <= 0) {
			parallelism = tEnv.streamEnv().getParallelism();
		}
		finalParallelismNodeMap.put(boundedStreamScan, parallelism);
	}

	private void calculateSingleton(ExecNode<?, ?> execNode) {
		calculateInputs(execNode);
		for (ExecNode<?, ?> inputNode : execNode.getInputNodes()) {
			if (inputNode instanceof BatchExecExchange) {
				// set parallelism as 1 to GlobalAggregate and other global node.
				if (((BatchExecExchange) inputNode).getDistribution().getType() == RelDistribution.Type.SINGLETON) {
					finalParallelismNodeMap.put(execNode, 1);
					return;
				}
			}
		}
	}

	private void calculateValues(BatchExecValues valuesBatchExec) {
		finalParallelismNodeMap.put(valuesBatchExec, 1);
	}

	private void calculateInputs(ExecNode<?, ?> node) {
		node.getInputNodes().forEach(this::calculate);
	}
}

