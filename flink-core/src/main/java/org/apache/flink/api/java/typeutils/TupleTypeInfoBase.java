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

import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class TupleTypeInfoBase<T> extends CompositeType<T> {

	private static final long serialVersionUID = 1L;
	
	private final static String REGEX_FIELD = "(f?)([0-9]+)";
	private final static String REGEX_NESTED_FIELDS = "("+REGEX_FIELD+")(\\.(.+))?";
	private final static String REGEX_NESTED_FIELDS_WILDCARD = REGEX_NESTED_FIELDS
			+"|\\"+ ExpressionKeys.SELECT_ALL_CHAR
			+"|\\"+ExpressionKeys.SELECT_ALL_CHAR_SCALA;

	private static final Pattern PATTERN_FIELD = Pattern.compile(REGEX_FIELD);
	private static final Pattern PATTERN_NESTED_FIELDS = Pattern.compile(REGEX_NESTED_FIELDS);
	private static final Pattern PATTERN_NESTED_FIELDS_WILDCARD = Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD);

	// --------------------------------------------------------------------------------------------
	
	protected final TypeInformation<?>[] types;
	
	private final int totalFields;

	public TupleTypeInfoBase(Class<T> tupleType, TypeInformation<?>... types) {
		super(tupleType);

		this.types = checkNotNull(types);

		int fieldCounter = 0;

		for(TypeInformation<?> type : types) {
			fieldCounter += type.getTotalFields();
		}

		totalFields = fieldCounter;
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
		} else {
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
			if (tail == null) {
				if (fieldType instanceof CompositeType) {
					// forward offsets
					for (int i = 0; i < fieldPos; i++) {
						offset += this.getTypeAt(i).getTotalFields();
					}
					// add all fields of composite type
					((CompositeType<?>) fieldType).getFlatFields("*", offset, result);
				} else {
					// we found the field to add
					// compute flat field position by adding skipped fields
					int flatFieldPos = offset;
					for (int i = 0; i < fieldPos; i++) {
						flatFieldPos += this.getTypeAt(i).getTotalFields();
					}
					result.add(new FlatFieldDescriptor(flatFieldPos, fieldType));
				}
			} else {
				if (fieldType instanceof CompositeType<?>) {
					// forward offset
					for (int i = 0; i < fieldPos; i++) {
						offset += this.getTypeAt(i).getTotalFields();
					}
					((CompositeType<?>) fieldType).getFlatFields(tail, offset, result);
				} else {
					throw new InvalidFieldReferenceException("Nested field expression \"" + tail + "\" not possible on atomic type " + fieldType + ".");
				}
			}
		}
	}

	@Override
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
	
	@Override
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

			return other.canEqual(this) &&
				super.equals(other) &&
				Arrays.equals(types, other.types) &&
				totalFields == other.totalFields;
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TupleTypeInfoBase;
	}
	
	@Override
	public int hashCode() {
		return 31 * (31 * super.hashCode() + Arrays.hashCode(types)) + totalFields;
	}

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder("Tuple");
		bld.append(types.length);
		if (types.length > 0) {
			bld.append('<').append(types[0]);

			for (int i = 1; i < types.length; i++) {
				bld.append(", ").append(types[i]);
			}

			bld.append('>');
		}
		return bld.toString();
	}

	@Override
	public boolean hasDeterministicFieldOrder() {
		return true;
	}
}
