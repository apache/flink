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

package org.apache.flink.table.planner.plan.processors;

import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.process.DAGProcessContext;
import org.apache.flink.table.planner.plan.nodes.process.DAGProcessor;
import org.apache.flink.table.planner.plan.processors.utils.InputPriorityConflictResolver;

import java.util.List;

/**
 * A {@link DAGProcessor} that finds out all deadlocks in the DAG and resolves them.
 *
 * <p>NOTE: This processor can be only applied on {@link BatchExecNode} DAG.
 */
public class DeadlockBreakupProcessor implements DAGProcessor {

	@Override
	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> rootNodes, DAGProcessContext context) {
		if (!rootNodes.stream().allMatch(r -> r instanceof BatchExecNode)) {
			throw new TableException("Only BatchExecNode DAG is supported now");
		}

		InputPriorityConflictResolver resolver = new InputPriorityConflictResolver(
			rootNodes,
			ExecEdge.DamBehavior.END_INPUT,
			ShuffleMode.BATCH);
		resolver.detectAndResolve();
		return rootNodes;
	}
}
