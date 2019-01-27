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

package org.apache.flink.table.resource.stream;

import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.exec.StreamExecNode;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecDataStreamScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecExchange;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecValues;
import org.apache.flink.table.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.plan.nodes.process.DAGProcessor;
import org.apache.flink.table.util.NodeResourceUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelDistribution;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *  Calculating parallelism for for {@link org.apache.flink.table.plan.nodes.exec.StreamExecNode}.
 */
public class StreamParallelismProcessor implements DAGProcessor {

	private final Set<ExecNode> calculatedNodeSet = new HashSet<>();
	private TableEnvironment tEnv;

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context) {
		sinkNodes.forEach(s -> Preconditions.checkArgument(s instanceof StreamExecNode));
		tEnv = context.getTableEnvironment();
		for (ExecNode sinkNode : sinkNodes) {
			calculate(sinkNode);
		}
		return sinkNodes;
	}

	private void calculate(ExecNode<?, ?> execNode) {
		if (!calculatedNodeSet.add(execNode)) {
			return;
		}
		calculateInputs(execNode);
		if (execNode instanceof StreamExecDataStreamScan) {
			calculateStreamScan((StreamExecDataStreamScan) execNode);
		} else if (execNode instanceof StreamExecTableSourceScan) {
			calculateTableSourceScan((StreamExecTableSourceScan) execNode);
		} else if (execNode instanceof StreamExecExchange) {
			calculateExchange((StreamExecExchange) execNode);
		} else if (execNode instanceof StreamExecValues) {
			calculateValues((StreamExecValues) execNode);
		} else {
			calculateDefault(execNode);
		}
	}

	private void calculateStreamScan(StreamExecDataStreamScan streamScan) {
		StreamTransformation transformation = streamScan.getSourceTransformation(tEnv.execEnv());
		int parallelism = transformation.getParallelism();
		if (parallelism <= 0) {
			parallelism = tEnv.execEnv().getParallelism();
		}
		streamScan.getResource().setParallelism(parallelism);
	}

	private void calculateTableSourceScan(StreamExecTableSourceScan tableSourceScan) {
		StreamTransformation transformation = tableSourceScan.getSourceTransformation(tEnv.execEnv());
		if (transformation.getMaxParallelism() > 0) {
			tableSourceScan.getResource().setParallelism(transformation.getMaxParallelism());
			return;
		}
		int configParallelism = NodeResourceUtil.getSourceParallelism(
				tEnv.getConfig().getConf(), tEnv.execEnv().getParallelism());
		tableSourceScan.getResource().setParallelism(configParallelism);
	}

	private void calculateExchange(StreamExecExchange execExchange) {
		if (execExchange.getDistribution().getType() == RelDistribution.Type.SINGLETON) {
			execExchange.getResource().setParallelism(1);
		} else {
			calculateDefault(execExchange);
		}
	}

	private void calculateValues(StreamExecValues values) {
		values.getResource().setParallelism(1);
	}

	private void calculateDefault(ExecNode<?, ?> execNode) {
		execNode.getResource().setParallelism(NodeResourceUtil.getOperatorDefaultParallelism(
					tEnv.getConfig().getConf(), tEnv.execEnv().getParallelism()));
	}

	private void calculateInputs(ExecNode<?, ?> node) {
		node.getInputNodes().forEach(this::calculate);
	}
}
