/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.example.record.incremental.pagerank;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactDouble;
import eu.stratosphere.types.PactLong;

public class PRDependenciesComputationMatchDelta extends JoinFunction {

	private final PactRecord result = new PactRecord();
	private final PactDouble partRank = new PactDouble();
	
	/*
	 * 
	 * (srcId, trgId, weight) x (vId, rank) => (trgId, rank / weight)
	 * 
	 */
	@Override
	public void match(PactRecord vertexWithRank, PactRecord edgeWithWeight, Collector<PactRecord> out) throws Exception {
		
		result.setField(0, edgeWithWeight.getField(1, PactLong.class));
		final long outLinks = edgeWithWeight.getField(2, PactLong.class).getValue();
		final double rank = vertexWithRank.getField(1, PactDouble.class).getValue();
		partRank.setValue(rank / (double) outLinks);
		result.setField(1, partRank);
		
		out.collect(result);
	}
}
