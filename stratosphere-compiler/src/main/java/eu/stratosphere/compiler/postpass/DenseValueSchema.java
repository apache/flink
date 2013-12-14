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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.types.Value;

/**
 * Class encapsulating a schema map (int column position -> column type) and a reference counter.
 */
public class DenseValueSchema extends AbstractSchema<Class<? extends Value>> {
	
	private final List<Class<? extends Value>> schema;
	
	private int numFields = -1;
	
	
	public DenseValueSchema() {
		this.schema = new ArrayList<Class<? extends Value>>();
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void addType(int pos, Class<? extends Value> type) throws ConflictingFieldTypeInfoException {
		if (pos == schema.size()) {
			// right at the end, most common case
			schema.add(type);
		} else if (pos < schema.size()) {
			// in the middle, check for existing conflicts
			Class<? extends Value> previous = schema.get(pos);
			if (previous == null) {
				schema.set(pos, type);
			} else if (previous != type) {
				throw new ConflictingFieldTypeInfoException(pos, previous, type);
			}
		} else {
			// grow to the end
			for (int i = schema.size(); i <= pos; i++) {
				schema.add(null);
			}
			schema.set(pos, type);
		}
	}
	
	@Override
	public Class<? extends Value> getType(int field) {
		return schema.get(field);
	}
	
	@Override
	public Iterator<Map.Entry<Integer, Class<? extends Value>>> iterator() {
		final Iterator<Class<? extends Value>> iter = schema.iterator();
		return new Iterator<Map.Entry<Integer,Class<? extends Value>>>() {

			private int pos = 0;
			
			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public java.util.Map.Entry<Integer, Class<? extends Value>> next() {
				return new Entry(pos++, iter.next());
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	public int getNumFields() {
		return numFields;
	}

	
	public void setNumFields(int numFields) {
		this.numFields = numFields;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return schema.hashCode() ^ getNumConnectionsThatContributed();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DenseValueSchema) {
			DenseValueSchema other = (DenseValueSchema) obj;
			return schema.equals(other.schema) && 
					getNumConnectionsThatContributed() == other.getNumConnectionsThatContributed();
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return "<" + getNumConnectionsThatContributed() + "> : " + schema.toString();
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class Entry implements Map.Entry<Integer, Class<? extends Value>> {
		
		private Integer key;
		private Class<? extends Value> value;
		
		
		public Entry(Integer key, Class<? extends Value> value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public Integer getKey() {
			return this.key;
		}

		@Override
		public Class<? extends Value> getValue() {
			return this.value;
		}

		@Override
		public Class<? extends Value> setValue(Class<? extends Value> value) {
			throw new UnsupportedOperationException();
		}
	}
}