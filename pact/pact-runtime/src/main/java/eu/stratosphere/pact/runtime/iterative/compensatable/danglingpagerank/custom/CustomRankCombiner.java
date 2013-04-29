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
package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.generic.stub.AbstractStub;
import eu.stratosphere.pact.generic.stub.GenericReducer;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types.VertexWithRank;


/**
 *
 */
public class CustomRankCombiner extends AbstractStub implements GenericReducer<VertexWithRank, VertexWithRank> {

	private final VertexWithRank accumulator = new VertexWithRank();
	
	@Override
	public void reduce(Iterator<VertexWithRank> records, Collector<VertexWithRank> out) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void combine(Iterator<VertexWithRank> records, Collector<VertexWithRank> out) throws Exception {
		VertexWithRank next = records.next();
		this.accumulator.setVertexID(next.getVertexID());
		double rank = next.getRank();
		
		while (records.hasNext()) {
			rank += records.next().getRank();
		}
		
		this.accumulator.setRank(rank);
		out.collect(this.accumulator);
	}
}
