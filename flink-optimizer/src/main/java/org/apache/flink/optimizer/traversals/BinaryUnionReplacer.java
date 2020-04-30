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
import org.apache.flink.optimizer.plan.BinaryUnionPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.IterationPlanNode;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A traversal that collects cascading binary unions into a single n-ary
 * union operator. The exception is, when on of the union inputs is materialized, such as in the
 * static-code-path-cache in iterations.
 */
public class BinaryUnionReplacer implements Visitor<PlanNode> {

	private final Set<PlanNode> seenBefore = new HashSet<PlanNode>();

	@Override
	public boolean preVisit(PlanNode visitable) {
		if (this.seenBefore.add(visitable)) {
			if (visitable instanceof IterationPlanNode) {
				((IterationPlanNode) visitable).acceptForStepFunction(this);
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void postVisit(PlanNode visitable) {

		if (visitable instanceof BinaryUnionPlanNode) {

			final BinaryUnionPlanNode unionNode = (BinaryUnionPlanNode) visitable;
			final Channel in1 = unionNode.getInput1();
			final Channel in2 = unionNode.getInput2();

			if (!unionNode.unionsStaticAndDynamicPath()) {

				// both on static path, or both on dynamic path. we can collapse them
				NAryUnionPlanNode newUnionNode;

				List<Channel> inputs = new ArrayList<Channel>();
				collect(in1, inputs);
				collect(in2, inputs);

				newUnionNode = new NAryUnionPlanNode(unionNode.getOptimizerNode(), inputs,
						unionNode.getGlobalProperties(), unionNode.getCumulativeCosts());

				newUnionNode.setParallelism(unionNode.getParallelism());

				for (Channel c : inputs) {
					c.setTarget(newUnionNode);
				}

				for (Channel channel : unionNode.getOutgoingChannels()) {
					channel.swapUnionNodes(newUnionNode);
					newUnionNode.addOutgoingChannel(channel);
				}
			}
			else {
				// union between the static and the dynamic path. we need to handle this for now
				// through a special union operator

				// make sure that the first input is the cached (static) and the second input is the dynamic
				if (in1.isOnDynamicPath()) {
					BinaryUnionPlanNode newUnionNode = new BinaryUnionPlanNode(unionNode);

					in1.setTarget(newUnionNode);
					in2.setTarget(newUnionNode);

					for (Channel channel : unionNode.getOutgoingChannels()) {
						channel.swapUnionNodes(newUnionNode);
						newUnionNode.addOutgoingChannel(channel);
					}
				}
			}
		}
	}

	public void collect(Channel in, List<Channel> inputs) {
		if (in.getSource() instanceof NAryUnionPlanNode) {
			// sanity check
			if (in.getShipStrategy() != ShipStrategyType.FORWARD) {
				throw new CompilerException("Bug: Plan generation for Unions picked a ship strategy between binary plan operators.");
			}
			if (!(in.getLocalStrategy() == null || in.getLocalStrategy() == LocalStrategy.NONE)) {
				throw new CompilerException("Bug: Plan generation for Unions picked a local strategy between binary plan operators.");
			}

			inputs.addAll(((NAryUnionPlanNode) in.getSource()).getListOfInputs());
		} else {
			// is not a collapsed union node, so we take the channel directly
			inputs.add(in);
		}
	}
}
