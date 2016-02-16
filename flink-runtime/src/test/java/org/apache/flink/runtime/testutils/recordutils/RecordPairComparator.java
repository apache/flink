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


package org.apache.flink.runtime.testutils.recordutils;

import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;


/**
 * Implementation of the {@link TypePairComparator} interface for Pact Records. The equality is established on a set of
 * key fields. The indices of the key fields may be different on the reference and candidate side.
 */
public class RecordPairComparator extends TypePairComparator<Record, Record>  {
	
	private final int[] keyFields1, keyFields2;			// arrays with the positions of the keys in the records
	
	@SuppressWarnings("rawtypes")
	private final Value[] keyHolders1, keyHolders2;		// arrays with mutable objects for the key types
	
	
	public RecordPairComparator(int[] keyFieldsReference, int[] keyFieldsCandidate, Class<? extends Value>[] keyTypes) {
		if (keyFieldsReference.length != keyFieldsCandidate.length || keyFieldsCandidate.length != keyTypes.length) {
			throw new IllegalArgumentException(
				"The arrays describing the key positions and types must be of the same length.");
		}
		this.keyFields1 = keyFieldsReference;
		this.keyFields2 = keyFieldsCandidate;
		
		// instantiate fields to extract keys into
		this.keyHolders1 = new Value[keyTypes.length];
		this.keyHolders2 = new Value[keyTypes.length];
		
		for (int i = 0; i < keyTypes.length; i++) {
			if (keyTypes[i] == null) {
				throw new NullPointerException("Key type " + i + " is null.");
			}
			this.keyHolders1[i] = InstantiationUtil.instantiate(keyTypes[i], Value.class);
			this.keyHolders2[i] = InstantiationUtil.instantiate(keyTypes[i], Value.class);
		}
	}
	
	// --------------------------------------------------------------------------------------------


	@Override
	public void setReference(Record reference) {
		for (int i = 0; i < this.keyFields1.length; i++) {
			if (!reference.getFieldInto(this.keyFields1[i], this.keyHolders1[i])) {
				throw new NullKeyFieldException(this.keyFields1[i]);
			}
		}
	}


	@SuppressWarnings("rawtypes")
	@Override
	public boolean equalToReference(Record candidate) {
		for (int i = 0; i < this.keyFields2.length; i++) {
			final Value k = candidate.getField(this.keyFields2[i], this.keyHolders2[i]);
			if (k == null) {
				throw new NullKeyFieldException(this.keyFields2[i]);
			} else if (!k.equals(this.keyHolders1[i])) {
				return false;
			}
		}
		return true;
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public int compareToReference(Record candidate) {
		for (int i = 0; i < this.keyFields2.length; i++) {
			final Comparable k = (Comparable) candidate.getField(this.keyFields2[i], this.keyHolders2[i]);
			if (k == null) {
				throw new NullKeyFieldException(this.keyFields2[i]);
			} else {
				final int comp = k.compareTo(this.keyHolders1[i]);
				if (comp != 0) {
					return comp;
				}
			}
		}
		return 0;
	}
}
