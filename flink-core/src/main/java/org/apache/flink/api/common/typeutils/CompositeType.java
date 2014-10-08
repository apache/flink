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

package org.apache.flink.api.common.typeutils;

import java.util.List;

import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;


/**
 * Type Information for Tuple and Pojo types
 * 
 * The class is taking care of serialization and comparators for Tuples as well.
 * See @see {@link Keys} class for fields setup.
 */
public abstract class CompositeType<T> extends TypeInformation<T> {
	
	protected final Class<T> typeClass;
	
	public CompositeType(Class<T> typeClass) {
		this.typeClass = typeClass;
	}
	
	/**
	 * Returns the keyPosition for the given fieldPosition, offsetted by the given offset
	 */
	public abstract void getKey(String fieldExpression, int offset, List<FlatFieldDescriptor> result);
	
	public abstract <X> TypeInformation<X> getTypeAt(int pos);
	
	/**
	 * Initializes the internal state inside a Composite type to create a new comparator 
	 * (such as the lists / arrays for the fields and field comparators)
	 * @param localKeyCount 
	 */
	protected abstract void initializeNewComparator(int localKeyCount);
	
	/**
	 * Add a field for comparison in this type.
	 */
	protected abstract void addCompareField(int fieldId, TypeComparator<?> comparator);
	
	/**
	 * Get the actual comparator we've initialized.
	 */
	protected abstract TypeComparator<T> getNewComparator();
	
	
	/**
	 * Generic implementation of the comparator creation. Composite types are supplying the infrastructure
	 * to create the actual comparators
	 * @return
	 */
	public TypeComparator<T> createComparator(int[] logicalKeyFields, boolean[] orders, int logicalFieldOffset) {
		initializeNewComparator(logicalKeyFields.length);
		
		for(int logicalKeyFieldIndex = 0; logicalKeyFieldIndex < logicalKeyFields.length; logicalKeyFieldIndex++) {
			int logicalKeyField = logicalKeyFields[logicalKeyFieldIndex];
			int logicalField = logicalFieldOffset; // this is the global/logical field number
			for(int localFieldId = 0; localFieldId < this.getArity(); localFieldId++) {
				TypeInformation<?> localFieldType = this.getTypeAt(localFieldId);
				
				if(localFieldType instanceof AtomicType && logicalField == logicalKeyField) {
					// we found an atomic key --> create comparator
					addCompareField(localFieldId, ((AtomicType<?>) localFieldType).createComparator(orders[logicalKeyFieldIndex]) );
				} else if(localFieldType instanceof CompositeType  && // must be a composite type
						( logicalField <= logicalKeyField //check if keyField can be at or behind the current logicalField
						&& logicalKeyField <= logicalField + (localFieldType.getTotalFields() - 1) ) // check if logical field + lookahead could contain our key
						) {
					// we found a compositeType that is containing the logicalKeyField we are looking for --> create comparator
					addCompareField(localFieldId, ((CompositeType<?>) localFieldType).createComparator(new int[] {logicalKeyField}, new boolean[] {orders[logicalKeyFieldIndex]}, logicalField));
				}
				
				// maintain logicalField
				if(localFieldType instanceof CompositeType) {
					// we need to subtract 1 because we are not accounting for the local field (not accessible for the user)
					logicalField += localFieldType.getTotalFields() - 1;
				}
				logicalField++;
			}
		}
		return getNewComparator();
	}
 	

	
	public static class FlatFieldDescriptor {
		private int keyPosition;
		private TypeInformation<?> type;
		
		public FlatFieldDescriptor(int keyPosition, TypeInformation<?> type) {
			if( !(type instanceof AtomicType)) {
				throw new IllegalArgumentException("A flattened field can only be an atomic type");
			}
			this.keyPosition = keyPosition;
			this.type = type;
		}


		public int getPosition() {
			return keyPosition;
		}

		public TypeInformation<?> getType() {
			return type;
		}
		
		@Override
		public String toString() {
			return "FlatFieldDescriptor [position="+keyPosition+" typeInfo="+type+"]";
		}
	}
}
