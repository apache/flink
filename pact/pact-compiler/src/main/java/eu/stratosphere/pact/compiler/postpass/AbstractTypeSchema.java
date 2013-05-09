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

package eu.stratosphere.pact.compiler.postpass;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.type.Value;

/**
 * Class encapsulating a schema map (int column position -> column type) and a reference counter.
 */
abstract class AbstractTypeSchema<T extends Value> implements Iterable<Map.Entry<Integer, Class<? extends T>>> {
	
	private final Map<Integer, Class<? extends T>> schema;
	
	private int numConnectionsThatContributed;
	
	
	public AbstractTypeSchema() {
		this.schema = new HashMap<Integer, Class<? extends T>>();
	}
	
	
	public void addType(Integer key, Class<? extends T> type) throws ConflictingFieldTypeInfoException 
	{
		Class<? extends T> previous = this.schema.put(key, type);
		if (previous != null && previous != type) {
			throw new ConflictingFieldTypeInfoException(key, previous, type);
		}
	}
	
	public Class<? extends T> getType(Integer field) {
		return this.schema.get(field);
	}
	
	public Iterator<Entry<Integer, Class<? extends T>>> iterator() {
		return this.schema.entrySet().iterator();
	}
	
	public int getNumConnectionsThatContributed() {
		return this.numConnectionsThatContributed;
	}
	
	public void increaseNumConnectionsThatContributed() {
		this.numConnectionsThatContributed++;
	}


	@Override
	public int hashCode() {
		return this.schema.hashCode() ^ this.numConnectionsThatContributed;
	}


	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AbstractTypeSchema) {
			AbstractTypeSchema<?> other = (AbstractTypeSchema<?>) obj;
			return this.schema.equals(other.schema) && 
					this.numConnectionsThatContributed == other.numConnectionsThatContributed;
		} else {
			return false;
		}
	}


	@Override
	public String toString() {
		return "<" + this.numConnectionsThatContributed + "> : " + this.schema.toString();
	}
}