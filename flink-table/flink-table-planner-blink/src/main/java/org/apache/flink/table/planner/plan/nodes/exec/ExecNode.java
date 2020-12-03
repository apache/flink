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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;

import java.util.List;

/**
 * The representation of execution information for a {@link FlinkPhysicalRel}.
 *
 * @param <T> The type of the elements that result from this node.
 */
public interface ExecNode<T> {

	/**
	 * Returns a list of this node's input nodes.
	 * If there are no inputs, returns an empty list, not null.
	 *
	 * @return List of this node's input nodes
	 */
	List<ExecNode<?>> getInputNodes();

	/**
	 * Returns a list of this node's input edges.
	 * If there are no inputs, returns an empty list, not null.
	 *
	 * @return List of this node's input edges
	 */
	List<ExecEdge> getInputEdges();

	/**
	 * Replaces the <code>ordinalInParent</code><sup>th</sup> input.
	 * You must override this method if you override {@link #getInputNodes}.
	 *
	 * @param ordinalInParent Position of the child input, 0 is the first
	 * @param newInputNode New node that should be put at position ordinalInParent
	 */
	void replaceInputNode(int ordinalInParent, ExecNode<?> newInputNode);

	/**
	 * Translates this node into a Flink operator.
	 *
	 * <p>NOTE: This method should return same translate result if called multiple times.
	 *
	 * @param planner The {@link Planner} of the translated Table.
	 */
	Transformation<T> translateToPlan(Planner planner);

	/**
	 * Accepts a visit from a {@link ExecNodeVisitor}.
	 *
	 * @param visitor ExecNodeVisitor
	 */
	void accept(ExecNodeVisitor visitor);
}
