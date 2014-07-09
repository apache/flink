/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Apache Flink project (http://flink.incubator.apache.org)
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
package eu.stratosphere.test.iterative.nephele.customdanglingpagerank;

import java.util.Iterator;

import org.apache.flink.api.common.functions.AbstractFunction;
import org.apache.flink.api.common.functions.GenericCombine;
import org.apache.flink.api.common.functions.GenericGroupReduce;
import org.apache.flink.util.Collector;

import eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types.VertexWithRank;


public class CustomRankCombiner extends AbstractFunction implements GenericGroupReduce<VertexWithRank, VertexWithRank>,
		GenericCombine<VertexWithRank>
{
	private static final long serialVersionUID = 1L;
	
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
