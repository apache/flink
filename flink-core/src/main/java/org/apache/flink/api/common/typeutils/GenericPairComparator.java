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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

@Internal
public class GenericPairComparator<T1, T2> extends TypePairComparator<T1, T2>
		implements Serializable {

	private static final long serialVersionUID = 1L;

	private final TypeComparator<T1> comparator1;
	private final TypeComparator<T2> comparator2;

	private final TypeComparator<Object>[] comparators1;
	private final TypeComparator<Object>[] comparators2;

	private final Object[] referenceKeyFields;
	
	private final Object[] candidateKeyFields;

	@SuppressWarnings("unchecked")
	public GenericPairComparator(TypeComparator<T1> comparator1, TypeComparator<T2> comparator2) {
		this.comparator1 = comparator1;
		this.comparator2 = comparator2;
		this.comparators1 = comparator1.getFlatComparators();
		this.comparators2 = comparator2.getFlatComparators();

		if(comparators1.length != comparators2.length) {
			throw new IllegalArgumentException("Number of key fields and comparators differ.");
		}

		int numKeys = comparators1.length;
		
		for(int i = 0; i < numKeys; i++) {
			this.comparators1[i] = comparators1[i].duplicate();
			this.comparators2[i] = comparators2[i].duplicate();
		}

		this.referenceKeyFields = new Object[numKeys];
		this.candidateKeyFields = new Object[numKeys];
	}
	
	@Override
	public void setReference(T1 reference) {
		comparator1.extractKeys(reference, referenceKeyFields, 0);
	}

	@Override
	public boolean equalToReference(T2 candidate) {
		comparator2.extractKeys(candidate, candidateKeyFields, 0);
		for (int i = 0; i < this.comparators1.length; i++) {
			if (this.comparators1[i].compare(referenceKeyFields[i], candidateKeyFields[i]) != 0) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int compareToReference(T2 candidate) {
		comparator2.extractKeys(candidate, candidateKeyFields, 0);
		for (int i = 0; i < this.comparators1.length; i++) {
			// We reverse ordering here because our "compareToReference" does work in a mirrored
			// way compared to Comparable.compareTo
			int res = this.comparators1[i].compare(candidateKeyFields[i], referenceKeyFields[i]);
			if(res != 0) {
				return res;
			}
		}
		return 0;
	}
}
