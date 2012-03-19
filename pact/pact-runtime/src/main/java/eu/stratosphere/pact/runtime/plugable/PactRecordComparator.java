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

package eu.stratosphere.pact.runtime.plugable;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;


/**
 * Implementation of the {@link TypeComparator} interface for Pact Records. The equality is established on a set of
 * key fields.
 *
 * @author Stephan Ewen
 */
public class PactRecordComparator implements TypeComparator<PactRecord, PactRecord>
{
	private final int[] keyFields1, keyFields2;			// arrays with the positions of the keys in the records
	
	private final Key[] keyHolders1, keyHolders2;		// arrays with mutable objects for the key types
	
	
	public PactRecordComparator(int[] keyFieldsReference, int[] keyFieldsCandidate, Class<? extends Key>[] keyTypes)
	{
		if (keyFieldsReference.length != keyFieldsCandidate.length || keyFieldsCandidate.length != keyTypes.length) {
			throw new IllegalArgumentException(
				"The arrays describing the key positions and types must be of the same length.");
		}
		this.keyFields1 = keyFieldsReference;
		this.keyFields2 = keyFieldsCandidate;
		
		// instantiate fields to extract keys into
		this.keyHolders1 = new Key[keyTypes.length];
		this.keyHolders2 = new Key[keyTypes.length];
		
		for (int i = 0; i < keyTypes.length; i++) {
			if (keyTypes[i] == null) {
				throw new NullPointerException("Key type " + i + " is null.");
			}
			this.keyHolders1[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
			this.keyHolders2[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
		}
	}
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#setReference(java.lang.Object)
	 */
	@Override
	public void setReference(PactRecord reference)
	{
		for (int i = 0; i < this.keyFields1.length; i++) {
			if (!reference.getFieldInto(this.keyFields1[i], this.keyHolders1[i])) {
				throw new NullKeyFieldException(this.keyFields1[i]);
			}
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#equalToReference(java.lang.Object)
	 */
	@Override
	public boolean equalToReference(PactRecord candidate)
	{
		for (int i = 0; i < this.keyFields2.length; i++) {
			final Key k = candidate.getField(this.keyFields2[i], this.keyHolders2[i]);
			if (k == null)
				throw new NullKeyFieldException(this.keyFields2[i]);
			else if (!k.equals(this.keyHolders1[i]))
				return false;
		}
		return true;
	}
}
