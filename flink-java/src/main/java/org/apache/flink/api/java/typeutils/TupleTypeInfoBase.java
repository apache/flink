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

package org.apache.flink.api.java.typeutils;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.operators.Keys.ExpressionKeys;

import com.google.common.base.Preconditions;

public abstract class TupleTypeInfoBase<T> extends CompositeType<T> {
	
	protected final TypeInformation<?>[] types;
	
	protected final Class<T> tupleType;

	private int totalFields;
	
	public TupleTypeInfoBase(Class<T> tupleType, TypeInformation<?>... types) {
		super(tupleType);
		this.tupleType = tupleType;
		this.types = types;
		for(TypeInformation<?> type : types) {
			totalFields += type.getTotalFields();
		}
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return true;
	}

	@Override
	public int getArity() {
		return types.length;
	}
	
	@Override
	public int getTotalFields() {
		return totalFields;
	}

	@Override
	public Class<T> getTypeClass() {
		return tupleType;
	}

	
	/**
	 * Recursively add all fields in this tuple type. We need this in particular to get all
	 * the types.
	 * @param keyId
	 * @param keyFields
	 */
	public void addAllFields(int startKeyId, List<FlatFieldDescriptor> keyFields) {
		for(int i = 0; i < this.getArity(); i++) {
			TypeInformation<?> type = this.types[i];
			if(type instanceof AtomicType) {
				keyFields.add(new FlatFieldDescriptor(startKeyId, type));
			} else if(type instanceof TupleTypeInfoBase<?>) {
				TupleTypeInfoBase<?> ttb = (TupleTypeInfoBase<?>) type;
				ttb.addAllFields(startKeyId, keyFields);
			}
			startKeyId += type.getTotalFields();
		}
	}
	

	@Override
	public void getKey(String fieldExpression, int offset, List<FlatFieldDescriptor> result) {
		// handle 'select all'
		if(fieldExpression.equals(ExpressionKeys.SELECT_ALL_CHAR) || fieldExpression.equals(ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
			int keyPosition = 0;
			for(TypeInformation<?> type : types) {
				if(type instanceof AtomicType) {
					result.add(new FlatFieldDescriptor(offset + keyPosition, type));
				} else if(type instanceof CompositeType) {
					CompositeType<?> cType = (CompositeType<?>)type;
					cType.getKey(String.valueOf(ExpressionKeys.SELECT_ALL_CHAR), offset + keyPosition, result);
					keyPosition += cType.getTotalFields()-1;
				} else {
					throw new RuntimeException("Unexpected key type: "+type);
				}
				keyPosition++;
			}
			return;
		}
		// check input
		if(fieldExpression.length() < 2) {
			throw new IllegalArgumentException("The field expression '"+fieldExpression+"' is incorrect. The length must be at least 2");
		}
		if(fieldExpression.charAt(0) != 'f') {
			throw new IllegalArgumentException("The field expression '"+fieldExpression+"' is incorrect for a Tuple type. It has to start with an 'f'");
		}
		// get first component of nested expression
		int dotPos = fieldExpression.indexOf('.');
		String nestedSplitFirst = fieldExpression;
		if(dotPos != -1 ) {
			Preconditions.checkArgument(dotPos != fieldExpression.length()-1, "The field expression can never end with a dot.");
			nestedSplitFirst = fieldExpression.substring(0, dotPos);
		}
		String fieldNumStr = nestedSplitFirst.substring(1, nestedSplitFirst.length());
		if(!StringUtils.isNumeric(fieldNumStr)) {
			throw new IllegalArgumentException("The field expression '"+fieldExpression+"' is incorrect. Field number '"+fieldNumStr+" is not numeric");
		}
		int pos = -1;
		try {
			pos = Integer.valueOf(fieldNumStr);
		} catch(NumberFormatException nfe) {
			throw new IllegalArgumentException("The field expression '"+fieldExpression+"' is incorrect. Field number '"+fieldNumStr+" is not numeric", nfe);
		}
		if(pos < 0) {
			throw new IllegalArgumentException("Negative position is not possible");
		}
		// pass down the remainder (after the dot) of the fieldExpression to the type at that position.
		if(dotPos != -1) { // we need to go deeper
			String rem = fieldExpression.substring(dotPos+1);
			if( !(types[pos] instanceof CompositeType<?>) ) {
				throw new RuntimeException("Element at position "+pos+" is not a composite type. There are no nested types to select");
			}
			CompositeType<?> cType = (CompositeType<?>) types[pos];
			cType.getKey(rem, offset + pos, result);
			return;
		}
		
		if(pos >= types.length) {
			throw new IllegalArgumentException("The specified tuple position does not exist");
		}
		
		// count nested fields before "pos".
		for(int i = 0; i < pos; i++) {
			offset += types[i].getTotalFields() - 1; // this adds only something to offset if its a composite type.
		}
		if(types[pos] instanceof CompositeType) {
			throw new IllegalArgumentException("The specified field '"+fieldExpression+"' is refering to a composite type.\n"
					+ "Either select all elements in this type with the '"+ExpressionKeys.SELECT_ALL_CHAR+"' operator or specify a field in the sub-type");
		}
		result.add(new FlatFieldDescriptor(offset + pos, types[pos]));
	}
	
	public <X> TypeInformation<X> getTypeAt(int pos) {
		if (pos < 0 || pos >= this.types.length) {
			throw new IndexOutOfBoundsException();
		}

		@SuppressWarnings("unchecked")
		TypeInformation<X> typed = (TypeInformation<X>) this.types[pos];
		return typed;
	}
	
	@Override
	public boolean isKeyType() {
		return isValidKeyType(this);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TupleTypeInfoBase) {
			@SuppressWarnings("unchecked")
			TupleTypeInfoBase<T> other = (TupleTypeInfoBase<T>) obj;
			return ((this.tupleType == null && other.tupleType == null) || this.tupleType.equals(other.tupleType)) &&
					Arrays.deepEquals(this.types, other.types);
			
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return this.types.hashCode() ^ Arrays.deepHashCode(this.types);
	}

	private boolean isValidKeyType(TypeInformation<?> typeInfo) {
		if(typeInfo instanceof TupleTypeInfoBase) {
			TupleTypeInfoBase<?> tupleType = ((TupleTypeInfoBase<?>)typeInfo);
			for(int i=0;i<tupleType.getArity();i++) {
				if (!isValidKeyType(tupleType.getTypeAt(i))) {
					return false;
				}
			}
			return true;
		} else  {
			return typeInfo.isKeyType();
		}
	}

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder("Tuple");
		bld.append(types.length).append('<');
		bld.append(types[0]);
		
		for (int i = 1; i < types.length; i++) {
			bld.append(", ").append(types[i]);
		}
		
		bld.append('>');
		return bld.toString();
	}
	
}
