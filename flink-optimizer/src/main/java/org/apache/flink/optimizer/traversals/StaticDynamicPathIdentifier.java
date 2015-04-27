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

import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.IterationNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.util.Visitor;

import java.util.HashSet;
import java.util.Set;

/**
 * A traversal that goes over the program data flow of an iteration and makes the nodes
 * that depend on the partial solution (the data set recomputed in each iteration) as "dynamic"
 * and the other nodes as "static".
 */
public class StaticDynamicPathIdentifier implements Visitor<OptimizerNode> {

	private final Set<OptimizerNode> seenBefore = new HashSet<OptimizerNode>();

	private final int costWeight;

	public StaticDynamicPathIdentifier(int costWeight) {
		this.costWeight = costWeight;
	}

	@Override
	public boolean preVisit(OptimizerNode visitable) {
		return this.seenBefore.add(visitable);
	}

	@Override
	public void postVisit(OptimizerNode visitable) {
		visitable.identifyDynamicPath(this.costWeight);

		// check that there is no nested iteration on the dynamic path
		if (visitable.isOnDynamicPath() && visitable instanceof IterationNode) {
			throw new CompilerException("Nested iterations are currently not supported.");
		}
	}
}
