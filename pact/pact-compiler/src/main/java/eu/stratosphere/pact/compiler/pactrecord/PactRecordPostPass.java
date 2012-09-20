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

package eu.stratosphere.pact.compiler.pactrecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.compiler.OptimizerPostPass;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializerFactory;


/**
 * @author Stephan Ewen
 */
public class PactRecordPostPass implements OptimizerPostPass
{
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.OptimizerPostPass#postPass(eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan)
	 */
	@Override
	public void postPass(OptimizedPlan plan) {
		for (SinkPlanNode sink : plan.getDataSinks()) {
			traverse(sink);
		}
	}
	
	protected void traverse(PlanNode node)
	{
		final List<Channel> outChannels = node.getOutgoingChannels();
		final KeySchema schema = (KeySchema) node.postPassHelper;
		
		if (outChannels.size() == 0 || schema.getNumConnectionsThatContributed() == outChannels.size()) {
			// done, we can create all the info now
		}
		
		
		
		// serializer factory is trivial, it is always the same parameterless one
		node.setSerializer(PactRecordSerializerFactory.get());
	}
	
	// --------------------------------------------------------------------------------------------

	private static final class KeySchema
	{
		private final Map<Integer, Class<? extends Key>> schema;
		
		private int numConnectionsThatContributed;
		
		KeySchema() {
			this.schema = new HashMap<Integer, Class<? extends Key>>();
		}
		
		int getNumConnectionsThatContributed() {
			return this.numConnectionsThatContributed;
		}
		
		void increaseNumConnectionsThatContributed() {
			this.numConnectionsThatContributed++;
		}
	}
}
