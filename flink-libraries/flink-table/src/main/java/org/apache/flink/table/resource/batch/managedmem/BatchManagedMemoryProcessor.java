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

package org.apache.flink.table.resource.batch.managedmem;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor;
import org.apache.flink.table.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.plan.nodes.process.DAGProcessor;
import org.apache.flink.table.util.NodeResourceUtil;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * Processor for calculating managed memory for {@link BatchExecNode}.
 */
public class BatchManagedMemoryProcessor implements DAGProcessor {
	private TableEnvironment tEnv;

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context) {
		sinkNodes.forEach(s -> Preconditions.checkArgument(s instanceof BatchExecNode));
		tEnv = context.getTableEnvironment();
		NodeResourceUtil.InferMode inferMode = NodeResourceUtil.getInferMode(tEnv.getConfig().getConf());
		BatchExecNodeVisitor managedVisitor;
		if (inferMode.equals(NodeResourceUtil.InferMode.ALL)) {
			managedVisitor = new BatchManagedMemCalculatorOnStatistics(tEnv.getConfig().getConf());
		} else {
			managedVisitor = new BatchManagedMemCalculatorOnConfig(tEnv.getConfig().getConf());
		}
		sinkNodes.forEach(s -> ((BatchExecNode) s).accept(managedVisitor));
		return sinkNodes;
	}
}
