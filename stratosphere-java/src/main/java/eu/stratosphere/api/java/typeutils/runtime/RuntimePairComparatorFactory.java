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

package eu.stratosphere.api.java.typeutils.runtime;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.java.tuple.Tuple;


public final class RuntimePairComparatorFactory<T1 extends Tuple, T2 extends Tuple> implements TypePairComparatorFactory<T1, T2>, java.io.Serializable {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public TypePairComparator<T1, T2> createComparator12(TypeComparator<T1> comparator1, TypeComparator<T2> comparator2) {

		if ((comparator1 instanceof TupleLeadingFieldComparator) && (comparator2 instanceof TupleLeadingFieldComparator)) {

			TypeComparator<?> comp1 = ((TupleLeadingFieldComparator<?,?>) comparator1).getFieldComparator();
			TypeComparator<?> comp2 = ((TupleLeadingFieldComparator<?,?>) comparator2).getFieldComparator();

			return createLeadingFieldPairComp(comp1, comp2);
		}
		else if ((comparator1 instanceof TupleComparator) && (comparator2 instanceof TupleComparator)) {

			TupleComparator<?> tupleComp1 = (TupleComparator<?>) comparator1;
			TupleComparator<?> tupleComp2 = (TupleComparator<?>) comparator2;

			return (TypePairComparator<T1, T2>) new TuplePairComparator<Tuple, Tuple>(
					tupleComp1.getKeyPositions(), tupleComp2.getKeyPositions(),
					tupleComp1.getComparators(), tupleComp2.getComparators());
		}
		else {
			throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypePairComparator<T2, T1> createComparator21(TypeComparator<T1> comparator1, TypeComparator<T2> comparator2) {
		
		if ((comparator1 instanceof TupleLeadingFieldComparator) && (comparator2 instanceof TupleLeadingFieldComparator)) {

			TypeComparator<?> comp1 = ((TupleLeadingFieldComparator<?,?>) comparator1).getFieldComparator();
			TypeComparator<?> comp2 = ((TupleLeadingFieldComparator<?,?>) comparator2).getFieldComparator();

			return createLeadingFieldPairComp(comp2, comp1);
		}
		else if ((comparator1 instanceof TupleComparator) && (comparator2 instanceof TupleComparator)) {

			TupleComparator<?> tupleComp1 = ((TupleComparator<?>)comparator1);
			TupleComparator<?> tupleComp2 = ((TupleComparator<?>)comparator2);

			return (TypePairComparator<T2, T1>) new TuplePairComparator<Tuple, Tuple>(
					tupleComp2.getKeyPositions(), tupleComp1.getKeyPositions(),
					tupleComp2.getComparators(), tupleComp1.getComparators());
		}
		else {
			throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
		}
	}
	
	private static <K, T1 extends Tuple, T2 extends Tuple> TupleLeadingFieldPairComparator<K, T1, T2> createLeadingFieldPairComp(
			TypeComparator<?> comp1, TypeComparator<?> comp2)
	{
		@SuppressWarnings("unchecked")
		TypeComparator<K> c1 = (TypeComparator<K>) comp1;
		@SuppressWarnings("unchecked")
		TypeComparator<K> c2 = (TypeComparator<K>) comp2;
		
		return new TupleLeadingFieldPairComparator<K, T1, T2>(c1, c2);
	}
}
