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

package org.apache.flink.test.iterative.nephele.customdanglingpagerank;

import java.util.Iterator;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.test.iterative.nephele.customdanglingpagerank.types.VertexWithRank;
import org.apache.flink.util.Collector;


public class CustomRankCombiner extends AbstractRichFunction implements GroupReduceFunction<VertexWithRank, VertexWithRank>,
		GroupCombineFunction<VertexWithRank, VertexWithRank>
{
	private static final long serialVersionUID = 1L;
	
	private final VertexWithRank accumulator = new VertexWithRank();
	
	@Override
	public void reduce(Iterable<VertexWithRank> records, Collector<VertexWithRank> out) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void combine(Iterable<VertexWithRank> recordsIterable, Collector<VertexWithRank> out) throws Exception {
		final Iterator<VertexWithRank> records = recordsIterable.iterator();
		
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
