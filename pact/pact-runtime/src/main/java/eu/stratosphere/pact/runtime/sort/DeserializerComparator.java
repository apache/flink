/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.sort;

import java.util.Comparator;

import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;

/**
 * A special raw comparator that deserializes the data into Pact Records, obtains their relevant fields, and
 * applies comparators to them. 
 *
 * @author Stephan Ewen
 */
public final class DeserializerComparator implements RawComparator
{
	private final PactRecord deserializer1;
	
	private final PactRecord deserializer2;
	
	private final int[] keyPositions;
	
	private final Key[] keyHolders1;
	
	private final Key[] keyHolders2;
	
	private final Comparator<Key>[] comparators;

	public DeserializerComparator(int[] keyPositions, Class<? extends Key>[] keyTypes, Comparator<Key>[] comparators)
	{
		this.deserializer1 = new PactRecord();
		this.deserializer2 = new PactRecord();
		
		this.keyPositions = keyPositions;	
		this.keyHolders1 = new Key[keyTypes.length];
		this.keyHolders2 = new Key[keyTypes.length];
		
		for (int i = 0; i < keyTypes.length; i++) {
			if (keyTypes[i] == null) {
				throw new NullPointerException("Key type " + i + " is null.");
			}
			this.keyHolders1[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
			this.keyHolders2[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
		}
		
		this.comparators = comparators;
	}

	public int compare(byte[] keyBytes1, byte[] keyBytes2, int startKey1, int startKey2)
	{
		if (!(this.deserializer1.readBinary(this.keyPositions, this.keyHolders1, keyBytes1, startKey1) &
		      this.deserializer2.readBinary(this.keyPositions, this.keyHolders2, keyBytes2, startKey2) ))
		{
			throw new NullKeyFieldException();
		}
		
		for (int i = 0; i < this.comparators.length; i++) {
			int val = this.comparators[i].compare(this.keyHolders1[i], this.keyHolders2[i]);
			if (val != 0) {
				return val;
			}
		}
		return 0;
	}
}
