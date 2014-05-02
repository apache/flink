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


public class TuplePairComparator<T1 extends Tuple, T2 extends Tuple> extends TypePairComparator<T1, T2> implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private final int[] keyFields1, keyFields2;
	private final TypeComparator<Object>[] comparators1;
	private final TypeComparator<Object>[] comparators2;
	
	@SuppressWarnings("unchecked")
	public TuplePairComparator(int[] keyFields1, int[] keyFields2, TypeComparator<Object>[] comparators1, TypeComparator<Object>[] comparators2) {
		
		if(keyFields1.length != keyFields2.length 
			|| keyFields1.length != comparators1.length
			|| keyFields2.length != comparators2.length) {
			
			throw new IllegalArgumentException("Number of key fields and comparators differ.");
		}
		
		int numKeys = keyFields1.length;
		
		this.keyFields1 = keyFields1;
		this.keyFields2 = keyFields2;
		this.comparators1 = new TypeComparator[numKeys];
		this.comparators2 = new TypeComparator[numKeys];
		
		for(int i = 0; i < numKeys; i++) {
			this.comparators1[i] = comparators1[i].duplicate();
			this.comparators2[i] = comparators2[i].duplicate();
		}
	}
	
	@Override
	public void setReference(T1 reference) {
		for(int i=0; i < this.comparators1.length; i++) {
			this.comparators1[i].setReference(reference.getField(keyFields1[i]));
		}
	}

	@Override
	public boolean equalToReference(T2 candidate) {
		for(int i=0; i < this.comparators1.length; i++) {
			if(!this.comparators1[i].equalToReference(candidate.getField(keyFields2[i]))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int compareToReference(T2 candidate) {
		for(int i=0; i < this.comparators1.length; i++) {
			this.comparators2[i].setReference(candidate.getField(keyFields2[i]));
			int res = this.comparators1[i].compareToReference(this.comparators2[i]);
			if(res != 0) {
				return res;
			}
		}
		return 0;
	}

	
	
}
