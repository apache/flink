/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.compiler.plan.candidate;

import java.util.List;

import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.costs.Costs;
import eu.stratosphere.pact.compiler.plan.SinkJoiner;
import eu.stratosphere.pact.runtime.task.DriverStrategy;


/**
 *
 */
public class SinkJoinerPlanNode extends DualInputPlanNode {
	
	public SinkJoinerPlanNode(SinkJoiner template, Channel input1, Channel input2) {
		super(template, input1, input2, DriverStrategy.NONE);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void setCosts(Costs nodeCosts) {
		// do not account for any cost, regardless of what the estimator
		// calculates for our shipping strategies
		super.setCosts(new Costs());
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void getDataSinks(List<SinkPlanNode> sinks) {
		final PlanNode in1 = this.input1.getSource();
		final PlanNode in2 = this.input2.getSource();
		
		if (in1 instanceof SinkPlanNode) {
			sinks.add((SinkPlanNode) in1);
		} else if (in1 instanceof SinkJoinerPlanNode) {
			((SinkJoinerPlanNode) in1).getDataSinks(sinks);
		} else {
			throw new CompilerException("Illegal child node for a sink joiner utility node: Neither Sink nor Sink Joiner");
		}
		
		if (in2 instanceof SinkPlanNode) {
			sinks.add((SinkPlanNode) in2);
		} else if (in2 instanceof SinkJoinerPlanNode) {
			((SinkJoinerPlanNode) in2).getDataSinks(sinks);
		} else {
			throw new CompilerException("Illegal child node for a sink joiner utility node: Neither Sink nor Sink Joiner");
		}
	}
}
