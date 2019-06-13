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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecDataStreamScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecTableSourceScan;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Set final parallelism if needed at the beginning time, if parallelism of a node is set to be final,
 * it will not be changed by other parallelism calculator.
 */
public class FinalParallelismSetter {

	private final StreamExecutionEnvironment env;
	private Set<ExecNode<?, ?>> calculatedNodeSet = new HashSet<>();
	private Map<ExecNode<?, ?>, Integer> finalParallelismNodeMap = new HashMap<>();

	private FinalParallelismSetter(StreamExecutionEnvironment env) {
		this.env = env;
	}

	/**
	 * Finding nodes that need to set final parallelism.
	 */
	public static Map<ExecNode<?, ?>, Integer> calculate(StreamExecutionEnvironment env, List<ExecNode<?, ?>> sinkNodes) {
		FinalParallelismSetter setter = new FinalParallelismSetter(env);
		sinkNodes.forEach(setter::calculate);
		return setter.finalParallelismNodeMap;
	}

	private void calculate(ExecNode<?, ?> execNode) {
		if (!calculatedNodeSet.add(execNode)) {
			return;
		}
		if (execNode instanceof BatchExecTableSourceScan) {
			calculateTableSource((BatchExecTableSourceScan) execNode);
		} else if (execNode instanceof StreamExecTableSourceScan) {
			calculateTableSource((StreamExecTableSourceScan) execNode);
		} else if (execNode instanceof BatchExecBoundedStreamScan) {
			calculateBoundedStreamScan((BatchExecBoundedStreamScan) execNode);
		} else if (execNode instanceof StreamExecDataStreamScan) {
			calculateDataStreamScan((StreamExecDataStreamScan) execNode);
		} else if (execNode instanceof Values) {
			calculateValues(execNode);
		} else {
			calculateIfSingleton(execNode);
		}
	}

	private void calculateTableSource(BatchExecTableSourceScan tableSourceScan) {
		StreamTransformation transformation = tableSourceScan.getSourceTransformation(env);
		if (transformation.getMaxParallelism() > 0) {
			tableSourceScan.getResource().setMaxParallelism(transformation.getMaxParallelism());
		}
	}

	private void calculateTableSource(StreamExecTableSourceScan tableSourceScan) {
		StreamTransformation transformation = tableSourceScan.getSourceTransformation(env);
		if (transformation.getMaxParallelism() > 0) {
			tableSourceScan.getResource().setMaxParallelism(transformation.getMaxParallelism());
		}
	}

	private void calculateBoundedStreamScan(BatchExecBoundedStreamScan boundedStreamScan) {
		StreamTransformation transformation = boundedStreamScan.getSourceTransformation();
		int parallelism = transformation.getParallelism();
		if (parallelism <= 0) {
			parallelism = env.getParallelism();
		}
		finalParallelismNodeMap.put(boundedStreamScan, parallelism);
	}

	private void calculateDataStreamScan(StreamExecDataStreamScan dataStreamScan) {
		StreamTransformation transformation = dataStreamScan.getSourceTransformation();
		int parallelism = transformation.getParallelism();
		if (parallelism <= 0) {
			parallelism = env.getParallelism();
		}
		finalParallelismNodeMap.put(dataStreamScan, parallelism);
	}

	private void calculateIfSingleton(ExecNode<?, ?> execNode) {
		calculateInputs(execNode);
		for (ExecNode<?, ?> inputNode : execNode.getInputNodes()) {
			if (inputNode instanceof Exchange &&
					((Exchange) inputNode).getDistribution().getType() == RelDistribution.Type.SINGLETON) {
				// set parallelism as 1 to GlobalAggregate and other global node.
				finalParallelismNodeMap.put(execNode, 1);
				execNode.getResource().setMaxParallelism(1);
				return;
			}
		}
	}

	private void calculateValues(ExecNode<?, ?> values) {
		finalParallelismNodeMap.put(values, 1);
		values.getResource().setMaxParallelism(1);
	}

	private void calculateInputs(ExecNode<?, ?> node) {
		node.getInputNodes().forEach(this::calculate);
	}
}

