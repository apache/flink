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
package eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types;

import eu.stratosphere.api.typeutils.TypeComparator;
import eu.stratosphere.api.typeutils.TypePairComparator;
import eu.stratosphere.api.typeutils.TypePairComparatorFactory;


/**
 *
 */
public class VertexWithRankDanglingToVertexWithRankPairComparatorFactory
	implements TypePairComparatorFactory<VertexWithRankAndDangling, VertexWithRank>
{
	
	@Override
	public VertexWithRankDanglingToVertexWithRankComparator createComparator12(
			TypeComparator<VertexWithRankAndDangling> comparator1, TypeComparator<VertexWithRank> comparator2)
	{
		return new VertexWithRankDanglingToVertexWithRankComparator();
	}

	@Override
	public VertexWithRankToVertexWithRankDanglingPairComparator createComparator21(
			TypeComparator<VertexWithRankAndDangling> comparator1, TypeComparator<VertexWithRank> comparator2)
	{
		return new VertexWithRankToVertexWithRankDanglingPairComparator();
	}
	

	public static final class VertexWithRankDanglingToVertexWithRankComparator
		extends TypePairComparator<VertexWithRankAndDangling, VertexWithRank>
	{
		private long reference;
		
		@Override
		public void setReference(VertexWithRankAndDangling reference) {
			this.reference = reference.getVertexID();
		}
		
		@Override
		public boolean equalToReference(VertexWithRank candidate) {
			return this.reference == candidate.getVertexID();
		}
	
		@Override
		public int compareToReference(VertexWithRank candidate) {
			long diff = candidate.getVertexID() - this.reference;
			return diff < 0 ? -1 : diff > 0 ? 1 : 0;
		}
	}
	
	public static final class VertexWithRankToVertexWithRankDanglingPairComparator
		extends TypePairComparator<VertexWithRank, VertexWithRankAndDangling>
	{
		private long reference;
		
		@Override
		public void setReference(VertexWithRank reference) {
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
