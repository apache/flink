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

package org.apache.flink.optimizer.traversals;

import org.apache.flink.optimizer.dag.IterationNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.util.Visitor;

/**
 * This traversal of the optimizer DAG computes the information needed to track
 * branches and joins in the data flow. This is important to support plans
 * that are not a minimally connected DAG (Such plans are not trees, but at least one node feeds its
 * output into more than one other node).
 */
public final class BranchesVisitor implements Visitor<OptimizerNode> {

	@Override
	public boolean preVisit(OptimizerNode node) {
		return node.getOpenBranches() == null;
	}

	@Override
	public void postVisit(OptimizerNode node) {
		if (node instanceof IterationNode) {
			((IterationNode) node).acceptForStepFunction(this);
		}

		node.computeUnclosedBranchStack();
	}
}
