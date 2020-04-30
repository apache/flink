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

import org.apache.flink.optimizer.dag.BinaryUnionNode;
import org.apache.flink.optimizer.dag.DagConnection;
import org.apache.flink.optimizer.dag.IterationNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Visitor;

/**
 * Enforces that all union nodes have the same parallelism as their successor (there must be only one!)
 * and that the union node and its successor are connected by a forward ship strategy.
 */
public class UnionParallelismAndForwardEnforcer implements Visitor<OptimizerNode> {

	@Override
	public boolean preVisit(OptimizerNode node) {

		// if the current node is a union
		if (node instanceof BinaryUnionNode) {
			int parallelism = -1;
			// set ship strategy of all outgoing connections to FORWARD.
			for (DagConnection conn : node.getOutgoingConnections()) {
				parallelism = conn.getTarget().getParallelism();
				conn.setShipStrategy(ShipStrategyType.FORWARD);
			}
			// adjust parallelism to be same as successor
			node.setParallelism(parallelism);
		}

		// traverse the whole plan
		return true;
	}

	@Override
	public void postVisit(OptimizerNode node) {
		// if required, recurse into the step function
		if (node instanceof IterationNode) {
			((IterationNode) node).acceptForStepFunction(this);
		}
	}
}
