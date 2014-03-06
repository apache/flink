/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils.runtime;

import java.io.Serializable;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.util.Reference;


public class ReferenceWrappedPairComparator<T1, T2> extends TypePairComparator<Reference<T1>, Reference<T2>> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private final TypePairComparator<T1, T2> comparator;
	
	public ReferenceWrappedPairComparator(TypePairComparator<T1, T2> comparator) {
		this.comparator = comparator;
	}
	
	@Override
	public void setReference(Reference<T1> toCompare) {
		comparator.setReference(toCompare.ref);
	}

	@Override
	public boolean equalToReference(Reference<T2> candidate) {
		return comparator.equalToReference(candidate.ref);
	}

	@Override
	public int compareToReference(Reference<T2> candidate) {
		return comparator.compareToReference(candidate.ref);
	}
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	
	public static final class ReferenceWrappedPairComparatorFactory<T1, T2> implements TypePairComparatorFactory<Reference<T1>, Reference<T2>>, java.io.Serializable {

		private static final long serialVersionUID = 1L;

		public ReferenceWrappedPairComparatorFactory() {}
		
		@SuppressWarnings("unchecked")
		@Override
		public TypePairComparator<Reference<T1>, Reference<T2>> createComparator12(
				TypeComparator<Reference<T1>> comparator1,
				TypeComparator<Reference<T2>> comparator2) {

			if (!(comparator1 instanceof ReferenceWrappedComparator) ||
					!(comparator2 instanceof ReferenceWrappedComparator)) {
				throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
			}
			
			TypeComparator<T1> unwrappedComp1 = ((ReferenceWrappedComparator<T1>)comparator1).getWrappedComparator();
			TypeComparator<T2> unwrappedComp2 = ((ReferenceWrappedComparator<T2>)comparator2).getWrappedComparator();
			
			if((unwrappedComp1 instanceof TupleComparator) && (unwrappedComp2 instanceof TupleComparator)) {
							
				TupleComparator<?> tupleComp1 = ((TupleComparator<?>)unwrappedComp1);
				TupleComparator<?> tupleComp2 = ((TupleComparator<?>)unwrappedComp2);
				
				TypePairComparator<T1, T2> tuplePairComp = (TypePairComparator<T1, T2>) new TuplePairComparator<Tuple, Tuple>(
						tupleComp1.getKeyPositions(), tupleComp2.getKeyPositions(),
						tupleComp1.getComparators(), tupleComp2.getComparators());

				return new ReferenceWrappedPairComparator<T1, T2>(tuplePairComp);
			
			}

			if((unwrappedComp1 instanceof TupleSingleFieldComparator) && (unwrappedComp2 instanceof TupleSingleFieldComparator)) {
				
				TupleSingleFieldComparator<?,? extends Object> tupleComp1 = ((TupleSingleFieldComparator<?,? extends Object>)unwrappedComp1);
				TupleSingleFieldComparator<?,? extends Object> tupleComp2 = ((TupleSingleFieldComparator<?,? extends Object>)unwrappedComp2);

				TypePairComparator<T1, T2> tuplePairComp = (TypePairComparator<T1, T2>) new TuplePairSingleFieldComparator<Tuple, Tuple>(
						tupleComp1.getKeyPosition(), tupleComp2.getKeyPosition(), 
						(TypeComparator<Object>)tupleComp1.getComparator(), (TypeComparator<Object>)tupleComp2.getComparator());
				
				return new ReferenceWrappedPairComparator<T1, T2>(tuplePairComp);
				
			} else {
				throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
			}
			
		}

		@SuppressWarnings("unchecked")
		@Override
		public TypePairComparator<Reference<T2>, Reference<T1>> createComparator21(
				TypeComparator<Reference<T1>> comparator1,
				TypeComparator<Reference<T2>> comparator2) {

			if (!(comparator1 instanceof ReferenceWrappedComparator) ||
					!(comparator2 instanceof ReferenceWrappedComparator)) {
				throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
			}
			
			TypeComparator<T1> unwrappedComp1 = ((ReferenceWrappedComparator<T1>)comparator1).getWrappedComparator();
			TypeComparator<T2> unwrappedComp2 = ((ReferenceWrappedComparator<T2>)comparator2).getWrappedComparator();
			
			if((unwrappedComp1 instanceof TupleComparator) && (unwrappedComp2 instanceof TupleComparator)) {
							
				TupleComparator<?> tupleComp1 = ((TupleComparator<?>)unwrappedComp1);
				TupleComparator<?> tupleComp2 = ((TupleComparator<?>)unwrappedComp2);
				
				TypePairComparator<T2, T1> tuplePairComp = (TypePairComparator<T2, T1>) new TuplePairComparator<Tuple, Tuple>(
						tupleComp2.getKeyPositions(), tupleComp1.getKeyPositions(),
						tupleComp2.getComparators(), tupleComp1.getComparators());
				
				return new ReferenceWrappedPairComparator<T2, T1>(tuplePairComp);
			
			}
			if((unwrappedComp1 instanceof TupleSingleFieldComparator) && (unwrappedComp2 instanceof TupleSingleFieldComparator)) {
				
				TupleSingleFieldComparator<?,? extends Object> tupleComp1 = ((TupleSingleFieldComparator<?,? extends Object>)unwrappedComp1);
				TupleSingleFieldComparator<?,? extends Object> tupleComp2 = ((TupleSingleFieldComparator<?,? extends Object>)unwrappedComp2);

				TypePairComparator<T2, T1> tuplePairComp = (TypePairComparator<T2, T1>) new TuplePairSingleFieldComparator<Tuple, Tuple>(
						tupleComp2.getKeyPosition(), tupleComp1.getKeyPosition(), 
						(TypeComparator<Object>)tupleComp2.getComparator(), (TypeComparator<Object>)tupleComp1.getComparator());
				
				return new ReferenceWrappedPairComparator<T2, T1>(tuplePairComp);
				
			} else {
				throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
			}
			
		}
		
	}
	
}
