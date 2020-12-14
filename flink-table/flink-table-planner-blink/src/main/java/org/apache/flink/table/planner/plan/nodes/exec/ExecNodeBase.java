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
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for {@link ExecNode}.
 *
 * @param <P> The {@link Planner} that could translate the node into {@link Transformation}.
 * @param <T> The type of the elements that result from this node.
 */
public abstract class ExecNodeBase<P extends Planner, T> implements ExecNode<T> {

	private final String description;
	private final List<ExecNode<?>> inputNodes;
	private final List<ExecEdge> inputEdges;
	private final RowType outputType;

	private transient Transformation<T> transformation;

	protected ExecNodeBase(
			List<ExecNode<?>> inputNodes,
			List<ExecEdge> inputEdges,
			RowType outputType,
			String description) {
		checkArgument(checkNotNull(inputNodes).size() == checkNotNull(inputEdges).size());
		this.inputNodes = new ArrayList<>(inputNodes);
		this.inputEdges = new ArrayList<>(inputEdges);
		this.outputType = checkNotNull(outputType);
		this.description = checkNotNull(description);
	}

	@Override
	public String getDesc() {
		return description;
	}

	@Override
	public RowType getOutputType() {
		return outputType;
	}

	@Override
	public List<ExecNode<?>> getInputNodes() {
		return inputNodes;
	}

	@Override
	public List<ExecEdge> getInputEdges() {
		return inputEdges;
	}

	@Override
	public void replaceInputNode(int ordinalInParent, ExecNode<?> newInputNode) {
		checkArgument(ordinalInParent >= 0 && ordinalInParent < inputNodes.size());
		inputNodes.set(ordinalInParent, newInputNode);
	}

	@Override
	public void replaceInputEdge(int ordinalInParent, ExecEdge newInputEdge) {
		checkArgument(ordinalInParent >= 0 && ordinalInParent < inputEdges.size());
		inputEdges.set(ordinalInParent, newInputEdge);
	}

	@SuppressWarnings("unchecked")
	public Transformation<T> translateToPlan(Planner planner) {
		if (transformation == null) {
			transformation = translateToPlanInternal((P) planner);
		}
		return transformation;
	}

	/**
	 * Internal method, translates this node into a Flink operator.
	 *
	 * @param planner The {@link Planner} that could translate the node into {@link Transformation}.
	 */
	protected abstract Transformation<T> translateToPlanInternal(P planner);

	@Override
	public void accept(ExecNodeVisitor visitor) {
		visitor.visit(this);
	}
}
