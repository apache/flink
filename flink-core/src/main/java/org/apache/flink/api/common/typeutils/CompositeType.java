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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;


/**
 * Type Information for Tuple and Pojo types
 * 
 * The class is taking care of serialization and comparators for Tuples as well.
 */
public abstract class CompositeType<T> extends TypeInformation<T> {
	
	private static final long serialVersionUID = 1L;
	
	protected final Class<T> typeClass;
	
	public CompositeType(Class<T> typeClass) {
		this.typeClass = typeClass;
	}
	
	/**
	 * Returns the flat field descriptors for the given field expression.
	 *
	 * @param fieldExpression The field expression for which the flat field descriptors are computed.
	 * @return The list of descriptors for the flat fields which are specified by the field expression.
	 */
	public List<FlatFieldDescriptor> getFlatFields(String fieldExpression) {
		List<FlatFieldDescriptor> result = new ArrayList<FlatFieldDescriptor>();
		this.getFlatFields(fieldExpression, 0, result);
		return result;
	}

	/**
	 * Computes the flat field descriptors for the given field expression with the given offset.
	 *
	 * @param fieldExpression The field expression for which the FlatFieldDescriptors are computed.
	 * @param offset The offset to use when computing the positions of the flat fields.
	 * @param result The list into which all flat field descriptors are inserted.
	 */
	public abstract void getFlatFields(String fieldExpression, int offset, List<FlatFieldDescriptor> result);

	/**
	 * Returns the type of the (nested) field at the given field expression position.
	 * Wildcards are not allowed.
	 *
	 * @param fieldExpression The field expression for which the field of which the type is returned.
	 * @return The type of the field at the given field expression.
	 */
	public abstract <X> TypeInformation<X> getTypeAt(String fieldExpression);

	/**
	 * Returns the type of the (unnested) field at the given field position.
	 *
	 * @param pos The position of the (unnested) field in this composite type.
	 * @return The type of the field at the given position.
	 */
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
	protected abstract TypeComparator<T> getNewComparator(ExecutionConfig config);
	
	
	/**
	 * Generic implementation of the comparator creation. Composite types are supplying the infrastructure
	 * to create the actual comparators
	 * @return The comparator
	 */
	public TypeComparator<T> createComparator(int[] logicalKeyFields, boolean[] orders, int logicalFieldOffset, ExecutionConfig config) {
		initializeNewComparator(logicalKeyFields.length);
		
		for(int logicalKeyFieldIndex = 0; logicalKeyFieldIndex < logicalKeyFields.length; logicalKeyFieldIndex++) {
			int logicalKeyField = logicalKeyFields[logicalKeyFieldIndex];
			int logicalField = logicalFieldOffset; // this is the global/logical field number
			for(int localFieldId = 0; localFieldId < this.getArity(); localFieldId++) {
				TypeInformation<?> localFieldType = this.getTypeAt(localFieldId);
				
				if(localFieldType instanceof AtomicType && logicalField == logicalKeyField) {
					// we found an atomic key --> create comparator
					addCompareField(localFieldId, ((AtomicType<?>) localFieldType).createComparator(orders[logicalKeyFieldIndex], config) );
				} else if(localFieldType instanceof CompositeType  && // must be a composite type
						( logicalField <= logicalKeyField //check if keyField can be at or behind the current logicalField
						&& logicalKeyField <= logicalField + (localFieldType.getTotalFields() - 1) ) // check if logical field + lookahead could contain our key
						) {
					// we found a compositeType that is containing the logicalKeyField we are looking for --> create comparator
					addCompareField(localFieldId, ((CompositeType<?>) localFieldType).createComparator(new int[] {logicalKeyField}, new boolean[] {orders[logicalKeyFieldIndex]}, logicalField, config));
				}
				
				// maintain logicalField
				if(localFieldType instanceof CompositeType) {
					// we need to subtract 1 because we are not accounting for the local field (not accessible for the user)
					logicalField += localFieldType.getTotalFields() - 1;
				}
				logicalField++;
			}
		}
		return getNewComparator(config);
	}

	// --------------------------------------------------------------------------------------------

	public static class FlatFieldDescriptor {
		private int keyPosition;
		private TypeInformation<?> type;
		
		public FlatFieldDescriptor(int keyPosition, TypeInformation<?> type) {
			if(type instanceof CompositeType) {
				throw new IllegalArgumentException("A flattened field can not be a composite type");
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

	/**
	 * Returns true when this type has a composite field with the given name.
	 */
	public boolean hasField(String fieldName) {
		return getFieldIndex(fieldName) >= 0;
	}

	@Override
	public boolean isKeyType() {
		for(int i=0;i<this.getArity();i++) {
			if (!this.getTypeAt(i).isKeyType()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isSortKeyType() {
		for(int i=0;i<this.getArity();i++) {
			if (!this.getTypeAt(i).isSortKeyType()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Returns the names of the composite fields of this type. The order of the returned array must
	 * be consistent with the internal field index ordering.
	 */
	public abstract String[] getFieldNames();

	/**
	 * True if this type has an inherent ordering of the fields, such that a user can
	 * always be sure in which order the fields will be in. This is true for Tuples and
	 * Case Classes. It is not true for Regular Java Objects, since there, the ordering of
	 * the fields can be arbitrary.
	 *
	 * This is used when translating a DataSet or DataStream to an Expression Table, when
	 * initially renaming the fields of the underlying type.
	 */
	public boolean hasDeterministicFieldOrder() {
		return false;
	}
	/**
	 * Returns the field index of the composite field of the given name.
	 *
	 * @return The field index or -1 if this type does not have a field of the given name.
	 */
	public abstract int getFieldIndex(String fieldName);

	public static class InvalidFieldReferenceException extends IllegalArgumentException {

		private static final long serialVersionUID = 1L;

		public InvalidFieldReferenceException(String s) {
			super(s);
		}
	}
}
