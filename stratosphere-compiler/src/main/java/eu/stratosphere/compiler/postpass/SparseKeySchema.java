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

package eu.stratosphere.compiler.postpass;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.types.Key;

/**
 * Class encapsulating a schema map (int column position -> column type) and a reference counter.
 */
public class SparseKeySchema extends AbstractSchema<Class<? extends Key>> {
	
	private final Map<Integer, Class<? extends Key>> schema;
	
	
	public SparseKeySchema() {
		this.schema = new HashMap<Integer, Class<? extends Key>>();
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void addType(int key, Class<? extends Key> type) throws ConflictingFieldTypeInfoException  {
		Class<? extends Key> previous = this.schema.put(key, type);
		if (previous != null && previous != type) {
			throw new ConflictingFieldTypeInfoException(key, previous, type);
		}
	}
	
	@Override
	public Class<? extends Key> getType(int field) {
		return this.schema.get(field);
	}
	
	@Override
	public Iterator<Entry<Integer, Class<? extends Key>>> iterator() {
		return this.schema.entrySet().iterator();
	}
	
	public int getNumTypes() {
		return this.schema.size();
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return this.schema.hashCode() ^ getNumConnectionsThatContributed();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SparseKeySchema) {
			SparseKeySchema other = (SparseKeySchema) obj;
			return this.schema.equals(other.schema) && 
					this.getNumConnectionsThatContributed() == other.getNumConnectionsThatContributed();
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return "<" + getNumConnectionsThatContributed() + "> : " + this.schema.toString();
	}
}