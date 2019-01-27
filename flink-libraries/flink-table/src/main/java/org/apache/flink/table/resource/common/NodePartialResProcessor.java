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

package org.apache.flink.table.resource.common;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.common.CommonScan;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.exec.NodeResource;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecDataStreamScan;
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecTableSourceScan;
import org.apache.flink.table.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.plan.nodes.process.DAGProcessor;
import org.apache.flink.table.util.NodeResourceUtil;

import org.apache.calcite.rel.RelDistribution;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *  Calculating partial resource for for {@link ExecNode}. Here Cpu, heap and direct memory
 *  will be calculated, parallelism, managed memory and other resource need to be calculated by
 *  other {@link DAGProcessor}s.
 */
public class NodePartialResProcessor implements DAGProcessor {

	private final Set<ExecNode> calculatedNodeSet = new HashSet<>();
	private TableEnvironment tEnv;

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context) {
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
		if (execNode instanceof BatchExecBoundedStreamScan || execNode instanceof StreamExecDataStreamScan) {
			calculateStreamScan((CommonScan) execNode);
		} else if (execNode instanceof BatchExecTableSourceScan || execNode instanceof StreamExecTableSourceScan) {
			calculateTableSourceScan((CommonScan) execNode);
		} else if (execNode instanceof BatchExecExchange) {
			calculateExchange((BatchExecExchange) execNode);
		} else if (!(execNode instanceof BatchExecSink || execNode instanceof BatchExecUnion)) {
			calculateDefaultNode(execNode);
		}
	}

	private void calculateStreamScan(CommonScan streamScan) {
		StreamTransformation transformation = streamScan.getSourceTransformation(tEnv.execEnv());
		ResourceSpec sourceRes = transformation.getMinResources();
		if (sourceRes == null) {
			sourceRes = ResourceSpec.DEFAULT;
		}
		calculateCommonScan(streamScan, sourceRes);
	}

	private void calculateTableSourceScan(CommonScan tableSourceScan) {
		// user may have set resource for source transformation.
		StreamTransformation transformation = tableSourceScan.getSourceTransformation(tEnv.execEnv());
		ResourceSpec sourceRes = transformation.getMinResources();
		if (sourceRes == ResourceSpec.DEFAULT || sourceRes == null) {
			int heap = NodeResourceUtil.getSourceMem(tEnv.getConfig().getConf());
			int direct = NodeResourceUtil.getSourceDirectMem(tEnv.getConfig().getConf());
			sourceRes = NodeResourceUtil.getResourceSpec(tEnv.getConfig().getConf(), heap, direct);
		}
		calculateCommonScan(tableSourceScan, sourceRes);
	}

	private void calculateCommonScan(CommonScan commonScan, ResourceSpec sourceRes) {
		ResourceSpec conversionRes = ResourceSpec.DEFAULT;
		if (commonScan.needInternalConversion()) {
			conversionRes = NodeResourceUtil.getDefaultResourceSpec(tEnv.getConfig().getConf());
		}
		commonScan.setResForSourceAndConversion(sourceRes, conversionRes);
	}

	private void calculateDefaultNode(ExecNode node) {
		setDefaultRes(node.getResource());
	}

	// set resource for rangePartition exchange
	private void calculateExchange(BatchExecExchange execExchange) {
		if (execExchange.getDistribution().getType() == RelDistribution.Type.RANGE_DISTRIBUTED) {
			setDefaultRes(execExchange.getResource());
		}
	}

	private void setDefaultRes(NodeResource resource) {
		double cpu = NodeResourceUtil.getDefaultCpu(tEnv.getConfig().getConf());
		int heap = NodeResourceUtil.getDefaultHeapMem(tEnv.getConfig().getConf());
		int direct = NodeResourceUtil.getDefaultDirectMem(tEnv.getConfig().getConf());
		resource.setCpu(cpu);
		resource.setHeapMem(heap);
		resource.setDirectMem(direct);
	}

	private void calculateInputs(ExecNode<?, ?> node) {
		node.getInputNodes().forEach(this::calculate);
	}
}
