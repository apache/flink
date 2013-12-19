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

package eu.stratosphere.pact.runtime.plugable.arrayrecord;

import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.types.CopyableValue;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.NullKeyFieldException;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.InstantiationUtil;


/**
 * Implementation of the {@link TypePairComparator} interface for Pact Records. The equality is established on a set of
 * key fields. The indices of the key fields may be different on the reference and candidate side.
 */
public class ArrayRecordPairComparator extends TypePairComparator<Value[], Value[]>
{
	private final int[] keyFields1, keyFields2;			// arrays with the positions of the keys in the records
	
	private final Key[] keyHolders1;					// arrays with mutable objects for the key types
	
	
	public ArrayRecordPairComparator(int[] keyFieldsReference, int[] keyFieldsCandidate,
			Class<? extends Key>[] keyTypes)
	{
		if (keyFieldsReference.length != keyFieldsCandidate.length || keyFieldsCandidate.length != keyTypes.length) {
			throw new IllegalArgumentException(
				"The arrays describing the key positions and types must be of the same length.");
		}
		this.keyFields1 = keyFieldsReference;
		this.keyFields2 = keyFieldsCandidate;
		
		// instantiate fields to extract keys into
		this.keyHolders1 = new Key[keyTypes.length];
		
		for (int i = 0; i < keyTypes.length; i++) {
			if (keyTypes[i] == null) {
				throw new NullPointerException("Key type " + i + " is null.");
			}
			this.keyHolders1[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
		}
	}
	
	// --------------------------------------------------------------------------------------------


	@SuppressWarnings("unchecked")
	@Override
	public void setReference(Value[] reference) {
		for (int i = 0; i < this.keyFields1.length; i++) {
			((CopyableValue<Value>) reference[this.keyFields1[i]]).copyTo(this.keyHolders1[i]);
		}
	}


	@Override
	public boolean equalToReference(Value[] candidate) {
		for (int i = 0; i < this.keyFields2.length; i++) {
			final Value k = candidate[this.keyFields2[i]];
			if (k == null)
				throw new NullKeyFieldException(this.keyFields2[i]);
			else if (!k.equals(this.keyHolders1[i]))
				return false;
		}
		return true;
	}


	@Override
	public int compareToReference(Value[] candidate) {
		for (int i = 0; i < this.keyFields2.length; i++) {
			final Key k = (Key) candidate[this.keyFields2[i]];
			if (k == null)
				throw new NullKeyFieldException(this.keyFields2[i]);
			else {
				final int comp = k.compareTo(this.keyHolders1[i]);
				if (comp != 0) {
					return comp;
				}
			}
		}
		return 0;
	}
}
