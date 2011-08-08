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

package eu.stratosphere.pact.runtime.plugable;

import java.util.Comparator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class PactRecordAccessors implements TypeAccessors<PactRecord>
{
	private final int[] keyFields;
	
	@SuppressWarnings("unused")
	private final Class<? extends Key>[] keyTypes;
	
	private final Key[] keyHolders1;
	
	private final Key[] keyHolders2;
	
	/**
	 * @param keyFields
	 */
	public PactRecordAccessors(int[] keyFields, Class<? extends Key>[] keyTypes) {
		this.keyFields = keyFields;
		this.keyTypes = keyTypes;
		
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

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#createInstance()
	 */
	@Override
	public PactRecord createInstance() {
		return new PactRecord(); 
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#createCopy(java.lang.Object)
	 */
	@Override
	public PactRecord createCopy(PactRecord from) {
		return from.createCopy();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(PactRecord from, PactRecord to) {
		from.copyTo(to);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#hash(java.lang.Object)
	 */
	@Override
	public int hash(PactRecord object) {
		object.getFieldsInto(this.keyFields, this.keyHolders1);
		int code = 0;
		for (int i = 0; i < this.keyHolders1.length; i++) {
			code ^= this.keyHolders1[i].hashCode();
		}
		return code;
	}


	@Override
	public int compare(PactRecord first, PactRecord second, Comparator<Key> comparator) {
		first.getFieldsInto(this.keyFields, this.keyHolders1);
		second.getFieldsInto(this.keyFields, this.keyHolders2);
		
		for (int i = 0; i < this.keyHolders1.length; i++) {
			int c = comparator.compare(this.keyHolders1[i], this.keyHolders2[i]);
			if (c != 0)
				return c;
		}
		return 0;
	}

}
