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

import java.io.Serializable;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.NullFieldException;
import org.apache.flink.types.NullKeyFieldException;


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
		for (int i = 0; i < this.comparators1.length; i++) {
			try {
				this.comparators1[i].setReference(reference
						.getFieldNotNull(keyFields1[i]));
			} catch (NullFieldException nfex) {
				throw new NullKeyFieldException(nfex);
			}
		}
	}

	@Override
	public boolean equalToReference(T2 candidate) {
		for (int i = 0; i < this.comparators1.length; i++) {
			try {
				if (!this.comparators1[i].equalToReference(candidate
						.getFieldNotNull(keyFields2[i]))) {
					return false;
				}
			} catch (NullFieldException nfex) {
				throw new NullKeyFieldException(nfex);
			}
		}
		return true;
	}

	@Override
	public int compareToReference(T2 candidate) {
		for (int i = 0; i < this.comparators1.length; i++) {
			try {
				this.comparators2[i].setReference(candidate
						.getFieldNotNull(keyFields2[i]));
				int res = this.comparators1[i]
						.compareToReference(this.comparators2[i]);
				if (res != 0) {
					return res;
				}
			} catch (NullFieldException nfex) {
				throw new NullKeyFieldException(nfex);
			}
		}
		return 0;
	}
}
