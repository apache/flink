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

import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.util.Visitor;

/**
 * Visitor that computes the interesting properties for each node in the optimizer DAG. On its recursive
 * depth-first descend, it propagates all interesting properties top-down.
 */
public class InterestingPropertyVisitor implements Visitor<OptimizerNode> {

	private CostEstimator estimator; // the cost estimator for maximal costs of an interesting property

	/**
	 * Creates a new visitor that computes the interesting properties for all nodes in the plan.
	 * It uses the given cost estimator used to compute the maximal costs for an interesting property.
	 *
	 * @param estimator
	 *        The cost estimator to estimate the maximal costs for interesting properties.
	 */
	public InterestingPropertyVisitor(CostEstimator estimator) {
		this.estimator = estimator;
	}

	@Override
	public boolean preVisit(OptimizerNode node) {
		// The interesting properties must be computed on the descend. In case a node has multiple outputs,
		// that computation must happen during the last descend.

		if (node.getInterestingProperties() == null && node.haveAllOutputConnectionInterestingProperties()) {
			node.computeUnionOfInterestingPropertiesFromSuccessors();
			node.computeInterestingPropertiesForInputs(this.estimator);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void postVisit(OptimizerNode visitable) {}
}
