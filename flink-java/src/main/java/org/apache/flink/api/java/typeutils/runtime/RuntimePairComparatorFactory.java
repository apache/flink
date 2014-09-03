/**
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


package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.java.tuple.Tuple;


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
		else {
			int[] keyPos1;
			int[] keyPos2;
			TypeComparator<Object>[] comps1;
			TypeComparator<Object>[] comps2;
			
			// get info from first comparator
			if (comparator1 instanceof TupleComparator) {
				TupleComparator<?> tupleComp1 = (TupleComparator<?>) comparator1;
				keyPos1 = tupleComp1.getKeyPositions();
				comps1 = tupleComp1.getComparators();
			}
			else if (comparator1 instanceof TupleLeadingFieldComparator) {
				TupleLeadingFieldComparator<?, ?> tupleComp1 = (TupleLeadingFieldComparator<?, ?>) comparator1;
				keyPos1 = new int[] {0};
				comps1 = new TypeComparator[] { tupleComp1.getFieldComparator() };
			}
			else {
				throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparator: " + comparator1);
			}
			
			// get info from second comparator
			if (comparator2 instanceof TupleComparator) {
				TupleComparator<?> tupleComp2 = (TupleComparator<?>) comparator2;
				keyPos2 = tupleComp2.getKeyPositions();
				comps2 = tupleComp2.getComparators();
			}
			else if (comparator2 instanceof TupleLeadingFieldComparator) {
				TupleLeadingFieldComparator<?, ?> tupleComp2 = (TupleLeadingFieldComparator<?, ?>) comparator2;
				keyPos2 = new int[] {0};
				comps2 = new TypeComparator[] { tupleComp2.getFieldComparator() };
			}
			else {
				throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparator: " + comparator1);
			}

			return (TypePairComparator<T1, T2>) new TuplePairComparator<Tuple, Tuple>(keyPos1, keyPos2, comps1, comps2);
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
		else {
			int[] keyPos1;
			int[] keyPos2;
			TypeComparator<Object>[] comps1;
			TypeComparator<Object>[] comps2;
			
			// get info from first comparator
			if (comparator1 instanceof TupleComparator) {
				TupleComparator<?> tupleComp1 = (TupleComparator<?>) comparator1;
				keyPos1 = tupleComp1.getKeyPositions();
				comps1 = tupleComp1.getComparators();
			}
			else if (comparator1 instanceof TupleLeadingFieldComparator) {
				TupleLeadingFieldComparator<?, ?> tupleComp1 = (TupleLeadingFieldComparator<?, ?>) comparator1;
				keyPos1 = new int[] {0};
				comps1 = new TypeComparator[] { tupleComp1.getFieldComparator() };
			}
			else {
				throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparator: " + comparator1);
			}
			
			// get info from second comparator
			if (comparator2 instanceof TupleComparator) {
				TupleComparator<?> tupleComp2 = (TupleComparator<?>) comparator2;
				keyPos2 = tupleComp2.getKeyPositions();
				comps2 = tupleComp2.getComparators();
			}
			else if (comparator2 instanceof TupleLeadingFieldComparator) {
				TupleLeadingFieldComparator<?, ?> tupleComp2 = (TupleLeadingFieldComparator<?, ?>) comparator2;
				keyPos2 = new int[] {0};
				comps2 = new TypeComparator[] { tupleComp2.getFieldComparator() };
			}
			else {
				throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparator: " + comparator1);
			}

			return (TypePairComparator<T2, T1>) new TuplePairComparator<Tuple, Tuple>(keyPos2, keyPos1, comps2, comps1);
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
