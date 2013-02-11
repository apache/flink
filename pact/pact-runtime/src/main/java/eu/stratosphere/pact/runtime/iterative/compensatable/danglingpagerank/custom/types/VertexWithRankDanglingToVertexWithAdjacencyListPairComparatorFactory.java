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
package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.types;

import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypePairComparator;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;


/**
 *
 */
public class VertexWithRankDanglingToVertexWithAdjacencyListPairComparatorFactory
	implements TypePairComparatorFactory<VertexWithRankAndDangling, VertexWithAdjacencyList>
{
	
	@Override
	public VertexWithRankDanglingToVertexWithAdjacencyListPairComparator createComparator12(
			TypeComparator<VertexWithRankAndDangling> comparator1, TypeComparator<VertexWithAdjacencyList> comparator2)
	{
		return new VertexWithRankDanglingToVertexWithAdjacencyListPairComparator();
	}

	@Override
	public VertexWithAdjacencyListToVertexWithRankDanglingPairComparator createComparator21(
			TypeComparator<VertexWithRankAndDangling> comparator1, TypeComparator<VertexWithAdjacencyList> comparator2)
	{
		return new VertexWithAdjacencyListToVertexWithRankDanglingPairComparator();
	}
	

	public static final class VertexWithRankDanglingToVertexWithAdjacencyListPairComparator
		implements TypePairComparator<VertexWithRankAndDangling, VertexWithAdjacencyList>
	{
		private long reference;
		
		@Override
		public void setReference(VertexWithRankAndDangling reference) {
			this.reference = reference.getVertexID();
		}
		
		@Override
		public boolean equalToReference(VertexWithAdjacencyList candidate) {
			return this.reference == candidate.getVertexID();
		}
	
		@Override
		public int compareToReference(VertexWithAdjacencyList candidate) {
			long diff = candidate.getVertexID() - this.reference;
			return diff < 0 ? -1 : diff > 0 ? 1 : 0;
		}
	}
	
	public static final class VertexWithAdjacencyListToVertexWithRankDanglingPairComparator
		implements TypePairComparator<VertexWithAdjacencyList, VertexWithRankAndDangling>
	{
		private long reference;
		
		@Override
		public void setReference(VertexWithAdjacencyList reference) {
			this.reference = reference.getVertexID();
		}
		
		@Override
		public boolean equalToReference(VertexWithRankAndDangling candidate) {
			return this.reference == candidate.getVertexID();
		}
	
		@Override
		public int compareToReference(VertexWithRankAndDangling candidate) {
			long diff = candidate.getVertexID() - this.reference;
			return diff < 0 ? -1 : diff > 0 ? 1 : 0;
		}
	}
}
