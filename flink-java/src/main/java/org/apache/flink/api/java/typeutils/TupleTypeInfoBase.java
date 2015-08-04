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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.operators.Keys.ExpressionKeys;

public abstract class TupleTypeInfoBase<T> extends CompositeType<T> {

	private static final long serialVersionUID = 1L;
	
	private final static String REGEX_FIELD = "(f?)([0-9]+)";
	private final static String REGEX_NESTED_FIELDS = "("+REGEX_FIELD+")(\\.(.+))?";
	private final static String REGEX_NESTED_FIELDS_WILDCARD = REGEX_NESTED_FIELDS
			+"|\\"+ExpressionKeys.SELECT_ALL_CHAR
			+"|\\"+ExpressionKeys.SELECT_ALL_CHAR_SCALA;

	private static final Pattern PATTERN_FIELD = Pattern.compile(REGEX_FIELD);
	private static final Pattern PATTERN_NESTED_FIELDS = Pattern.compile(REGEX_NESTED_FIELDS);
	private static final Pattern PATTERN_NESTED_FIELDS_WILDCARD = Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD);

	// --------------------------------------------------------------------------------------------
	
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

	public boolean isCaseClass() {
		return false;
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
	 * @param startKeyId
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
	public void getFlatFields(String fieldExpression, int offset, List<FlatFieldDescriptor> result) {

		Matcher matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression);
		if (!matcher.matches()) {
			throw new InvalidFieldReferenceException("Invalid tuple field reference \""+fieldExpression+"\".");
		}

		String field = matcher.group(0);
		if (field.equals(ExpressionKeys.SELECT_ALL_CHAR) || field.equals(ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
			// handle select all
			int keyPosition = 0;
			for (TypeInformation<?> type : types) {
				if (type instanceof CompositeType) {
					CompositeType<?> cType = (CompositeType<?>) type;
					cType.getFlatFields(String.valueOf(ExpressionKeys.SELECT_ALL_CHAR), offset + keyPosition, result);
					keyPosition += cType.getTotalFields() - 1;
				} else {
					result.add(new FlatFieldDescriptor(offset + keyPosition, type));
				}
				keyPosition++;
			}
			return;
		}

		String fieldStr = matcher.group(1);
		Matcher fieldMatcher = PATTERN_FIELD.matcher(fieldStr);
		if (!fieldMatcher.matches()) {
			throw new RuntimeException("Invalid matcher pattern");
		}
		field = fieldMatcher.group(2);
		int fieldPos = Integer.valueOf(field);

		if (fieldPos >= this.getArity()) {
			throw new InvalidFieldReferenceException("Tuple field expression \"" + fieldStr + "\" out of bounds of " + this.toString() + ".");
		}
		TypeInformation<?> fieldType = this.getTypeAt(fieldPos);
		String tail = matcher.group(5);
		if(tail == null) {
			if(fieldType instanceof CompositeType) {
				// forward offsets
				for(int i=0; i<fieldPos; i++) {
					offset += this.getTypeAt(i).getTotalFields();
				}
				// add all fields of composite type
				((CompositeType<?>) fieldType).getFlatFields("*", offset, result);
				return;
			} else {
				// we found the field to add
				// compute flat field position by adding skipped fields
				int flatFieldPos = offset;
				for(int i=0; i<fieldPos; i++) {
					flatFieldPos += this.getTypeAt(i).getTotalFields();
				}
				result.add(new FlatFieldDescriptor(flatFieldPos, fieldType));
				// nothing left to do
				return;
			}
		} else {
			if(fieldType instanceof CompositeType<?>) {
				// forward offset
				for(int i=0; i<fieldPos; i++) {
					offset += this.getTypeAt(i).getTotalFields();
				}
				((CompositeType<?>) fieldType).getFlatFields(tail, offset, result);
				// nothing left to do
				return;
			} else {
				throw new InvalidFieldReferenceException("Nested field expression \""+tail+"\" not possible on atomic type "+fieldType+".");
			}
		}
	}

	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {

		Matcher matcher = PATTERN_NESTED_FIELDS.matcher(fieldExpression);
		if(!matcher.matches()) {
			if (fieldExpression.equals(ExpressionKeys.SELECT_ALL_CHAR) || fieldExpression.equals(ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
				throw new InvalidFieldReferenceException("Wildcard expressions are not allowed here.");
			} else {
				throw new InvalidFieldReferenceException("Invalid format of tuple field expression \""+fieldExpression+"\".");
			}
		}

		String fieldStr = matcher.group(1);
		Matcher fieldMatcher = PATTERN_FIELD.matcher(fieldStr);
		if(!fieldMatcher.matches()) {
			throw new RuntimeException("Invalid matcher pattern");
		}
		String field = fieldMatcher.group(2);
		int fieldPos = Integer.valueOf(field);

		if(fieldPos >= this.getArity()) {
			throw new InvalidFieldReferenceException("Tuple field expression \""+fieldStr+"\" out of bounds of "+this.toString()+".");
		}
		TypeInformation<X> fieldType = this.getTypeAt(fieldPos);
		String tail = matcher.group(5);
		if(tail == null) {
			// we found the type
			return fieldType;
		} else {
			if(fieldType instanceof CompositeType<?>) {
				return ((CompositeType<?>) fieldType).getTypeAt(tail);
			} else {
				throw new InvalidFieldReferenceException("Nested field expression \""+tail+"\" not possible on atomic type "+fieldType+".");
			}
		}
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

	@Override
	public boolean hasDeterministicFieldOrder() {
		return true;
	}
}
