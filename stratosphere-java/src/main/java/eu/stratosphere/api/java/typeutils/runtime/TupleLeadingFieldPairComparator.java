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
import eu.stratosphere.api.java.tuple.Tuple;


public class TupleLeadingFieldPairComparator<K, T1 extends Tuple, T2 extends Tuple> extends TypePairComparator<T1, T2> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private final TypeComparator<K> comparator1;
	private final TypeComparator<K> comparator2;
	
	public TupleLeadingFieldPairComparator(TypeComparator<K> comparator1, TypeComparator<K> comparator2) {
		this.comparator1 = comparator1;
		this.comparator2 = comparator2;
	}
	
	@Override
	public void setReference(T1 reference) {
		this.comparator1.setReference(reference.<K>getField(0));
	}

	@Override
	public boolean equalToReference(T2 candidate) {
		return this.comparator1.equalToReference(candidate.<K>getField(0));
	}

	@Override
	public int compareToReference(T2 candidate) {
		this.comparator2.setReference(candidate.<K>getField(0));
		return this.comparator1.compareToReference(this.comparator2);
	}
}
