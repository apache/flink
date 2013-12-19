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
import eu.stratosphere.types.Record;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.LongValue;

public class PRDependenciesComputationMatchDelta extends JoinFunction {

	private final Record result = new Record();
	private final DoubleValue partRank = new DoubleValue();
	
	/*
	 * 
	 * (srcId, trgId, weight) x (vId, rank) => (trgId, rank / weight)
	 * 
	 */
	@Override
	public void match(Record vertexWithRank, Record edgeWithWeight, Collector<Record> out) throws Exception {
		
		result.setField(0, edgeWithWeight.getField(1, LongValue.class));
		final long outLinks = edgeWithWeight.getField(2, LongValue.class).getValue();
		final double rank = vertexWithRank.getField(1, DoubleValue.class).getValue();
		partRank.setValue(rank / (double) outLinks);
		result.setField(1, partRank);
		
		out.collect(result);
	}
}
