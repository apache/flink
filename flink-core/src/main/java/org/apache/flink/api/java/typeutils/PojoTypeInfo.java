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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoComparator;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * TypeInformation for "Java Beans"-style types. Flink refers to them as POJOs,
 * since the conditions are slightly different from Java Beans.
 * A type is considered a FLink POJO type, if it fulfills the conditions below.
 * <ul>
 *   <li>It is a public class, and standalone (not a non-static inner class)</li>
 *   <li>It has a public no-argument constructor.</li>
 *   <li>All fields are either public, or have public getters and setters.</li>
 * </ul>
 * 
 * @param <T> The type represented by this type information.
 */
@Public
public class PojoTypeInfo<T> extends CompositeType<T> {
	
	private static final long serialVersionUID = 1L;

	private final static String REGEX_FIELD = "[\\p{L}_\\$][\\p{L}\\p{Digit}_\\$]*";
	private final static String REGEX_NESTED_FIELDS = "("+REGEX_FIELD+")(\\.(.+))?";
	private final static String REGEX_NESTED_FIELDS_WILDCARD = REGEX_NESTED_FIELDS
					+"|\\"+ExpressionKeys.SELECT_ALL_CHAR
					+"|\\"+ExpressionKeys.SELECT_ALL_CHAR_SCALA;

	private static final Pattern PATTERN_NESTED_FIELDS = Pattern.compile(REGEX_NESTED_FIELDS);
	private static final Pattern PATTERN_NESTED_FIELDS_WILDCARD = Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD);

	private final PojoField[] fields;
	
	private final int totalFields;

	@PublicEvolving
	public PojoTypeInfo(Class<T> typeClass, List<PojoField> fields) {
		super(typeClass);

		checkArgument(Modifier.isPublic(typeClass.getModifiers()),
				"POJO %s is not public", typeClass);

		this.fields = fields.toArray(new PojoField[fields.size()]);

		Arrays.sort(this.fields, new Comparator<PojoField>() {
			@Override
			public int compare(PojoField o1, PojoField o2) {
				return o1.getField().getName().compareTo(o2.getField().getName());
			}
		});

		int counterFields = 0;

		for(PojoField field : fields) {
			counterFields += field.getTypeInformation().getTotalFields();
		}

		totalFields = counterFields;
	}

	@Override
	@PublicEvolving
	public boolean isBasicType() {
		return false;
	}


	@Override
	@PublicEvolving
	public boolean isTupleType() {
		return false;
	}

	@Override
	@PublicEvolving
	public int getArity() {
		return fields.length;
	}
	
	@Override
	@PublicEvolving
	public int getTotalFields() {
		return totalFields;
	}

	@Override
	@PublicEvolving
	public boolean isSortKeyType() {
		// Support for sorting POJOs that implement Comparable is not implemented yet.
		// Since the order of fields in a POJO type is not well defined, sorting on fields
		//   gives only some undefined order.
		return false;
	}
	

	@Override
	@PublicEvolving
	public void getFlatFields(String fieldExpression, int offset, List<FlatFieldDescriptor> result) {

		Matcher matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression);
		if(!matcher.matches()) {
			throw new InvalidFieldReferenceException("Invalid POJO field reference \""+fieldExpression+"\".");
		}

		String field = matcher.group(0);
		if(field.equals(ExpressionKeys.SELECT_ALL_CHAR) || field.equals(ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
			// handle select all
			int keyPosition = 0;
			for(PojoField pField : fields) {
				if(pField.getTypeInformation() instanceof CompositeType) {
					CompositeType<?> cType = (CompositeType<?>)pField.getTypeInformation();
					cType.getFlatFields(String.valueOf(ExpressionKeys.SELECT_ALL_CHAR), offset + keyPosition, result);
					keyPosition += cType.getTotalFields()-1;
				} else {
					result.add(
						new NamedFlatFieldDescriptor(
							pField.getField().getName(),
							offset + keyPosition,
							pField.getTypeInformation()));
				}
				keyPosition++;
			}
			return;
		} else {
			field = matcher.group(1);
		}

		// get field
		int fieldPos = -1;
		TypeInformation<?> fieldType = null;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getField().getName().equals(field)) {
				fieldPos = i;
				fieldType = fields[i].getTypeInformation();
				break;
			}
		}
		if (fieldPos == -1) {
			throw new InvalidFieldReferenceException("Unable to find field \""+field+"\" in type "+this+".");
		}
		String tail = matcher.group(3);
		if(tail == null) {
			if(fieldType instanceof CompositeType) {
				// forward offset
				for(int i=0; i<fieldPos; i++) {
					offset += this.getTypeAt(i).getTotalFields();
				}
				// add all fields of composite type
				((CompositeType<?>) fieldType).getFlatFields("*", offset, result);
			} else {
				// we found the field to add
				// compute flat field position by adding skipped fields
				int flatFieldPos = offset;
				for(int i=0; i<fieldPos; i++) {
					flatFieldPos += this.getTypeAt(i).getTotalFields();
				}
				result.add(new FlatFieldDescriptor(flatFieldPos, fieldType));
			}
		} else {
			if(fieldType instanceof CompositeType<?>) {
				// forward offset
				for(int i=0; i<fieldPos; i++) {
					offset += this.getTypeAt(i).getTotalFields();
				}
				((CompositeType<?>) fieldType).getFlatFields(tail, offset, result);
			} else {
				throw new InvalidFieldReferenceException("Nested field expression \""+tail+"\" not possible on atomic type "+fieldType+".");
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	@PublicEvolving
	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {

		Matcher matcher = PATTERN_NESTED_FIELDS.matcher(fieldExpression);
		if(!matcher.matches()) {
			if (fieldExpression.startsWith(ExpressionKeys.SELECT_ALL_CHAR) || fieldExpression.startsWith(ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
				throw new InvalidFieldReferenceException("Wildcard expressions are not allowed here.");
			} else {
				throw new InvalidFieldReferenceException("Invalid format of POJO field expression \""+fieldExpression+"\".");
			}
		}

		String field = matcher.group(1);
		// get field
		int fieldPos = -1;
		TypeInformation<?> fieldType = null;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getField().getName().equals(field)) {
				fieldPos = i;
				fieldType = fields[i].getTypeInformation();
				break;
			}
		}
		if (fieldPos == -1) {
			throw new InvalidFieldReferenceException("Unable to find field \""+field+"\" in type "+this+".");
		}

		String tail = matcher.group(3);
		if(tail == null) {
			// we found the type
			return (TypeInformation<X>) fieldType;
		} else {
			if(fieldType instanceof CompositeType<?>) {
				return ((CompositeType<?>) fieldType).getTypeAt(tail);
			} else {
				throw new InvalidFieldReferenceException("Nested field expression \""+tail+"\" not possible on atomic type "+fieldType+".");
			}
		}
	}

	@Override
	@PublicEvolving
	public <X> TypeInformation<X> getTypeAt(int pos) {
		if (pos < 0 || pos >= this.fields.length) {
			throw new IndexOutOfBoundsException();
		}
		@SuppressWarnings("unchecked")
		TypeInformation<X> typed = (TypeInformation<X>) fields[pos].getTypeInformation();
		return typed;
	}

	@Override
	@PublicEvolving
	protected TypeComparatorBuilder<T> createTypeComparatorBuilder() {
		return new PojoTypeComparatorBuilder();
	}

	@PublicEvolving
	public PojoField getPojoFieldAt(int pos) {
		if (pos < 0 || pos >= this.fields.length) {
			throw new IndexOutOfBoundsException();
		}
		return this.fields[pos];
	}

	@PublicEvolving
	public String[] getFieldNames() {
		String[] result = new String[fields.length];
		for (int i = 0; i < fields.length; i++) {
			result[i] = fields[i].getField().getName();
		}
		return result;
	}

	@Override
	@PublicEvolving
	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getField().getName().equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	@PublicEvolving
	@SuppressWarnings("unchecked")
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		if (config.isForceKryoEnabled()) {
			return new KryoSerializer<>(getTypeClass(), config);
		}

		if (config.isForceAvroEnabled()) {
			return AvroUtils.getAvroUtils().createAvroSerializer(getTypeClass());
		}

		return createPojoSerializer(config);
	}

	public PojoSerializer<T> createPojoSerializer(ExecutionConfig config) {
		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[fields.length];
		Field[] reflectiveFields = new Field[fields.length];

		for (int i = 0; i < fields.length; i++) {
			fieldSerializers[i] = fields[i].getTypeInformation().createSerializer(config);
			reflectiveFields[i] = fields[i].getField();
		}

		return new PojoSerializer<T>(getTypeClass(), fieldSerializers, reflectiveFields, config);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PojoTypeInfo) {
			@SuppressWarnings("unchecked")
			PojoTypeInfo<T> pojoTypeInfo = (PojoTypeInfo<T>)obj;

			return pojoTypeInfo.canEqual(this) &&
				super.equals(pojoTypeInfo) &&
				Arrays.equals(fields, pojoTypeInfo.fields) &&
				totalFields == pojoTypeInfo.totalFields;
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return 31 * (31 * Arrays.hashCode(fields) + totalFields) + super.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof PojoTypeInfo;
	}
	
	@Override
	public String toString() {
		List<String> fieldStrings = new ArrayList<String>();
		for (PojoField field : fields) {
			fieldStrings.add(field.getField().getName() + ": " + field.getTypeInformation().toString());
		}
		return "PojoType<" + getTypeClass().getName()
				+ ", fields = [" + StringUtils.join(fieldStrings, ", ") + "]"
				+ ">";
	}

	// --------------------------------------------------------------------------------------------

	private class PojoTypeComparatorBuilder implements TypeComparatorBuilder<T> {

		private ArrayList<TypeComparator> fieldComparators;
		private ArrayList<Field> keyFields;

		public PojoTypeComparatorBuilder() {
			fieldComparators = new ArrayList<TypeComparator>();
			keyFields = new ArrayList<Field>();
		}


		@Override
		public void initializeTypeComparatorBuilder(int size) {
			fieldComparators.ensureCapacity(size);
			keyFields.ensureCapacity(size);
		}

		@Override
		public void addComparatorField(int fieldId, TypeComparator<?> comparator) {
			fieldComparators.add(comparator);
			keyFields.add(fields[fieldId].getField());
		}

		@Override
		public TypeComparator<T> createTypeComparator(ExecutionConfig config) {
			checkState(
				keyFields.size() > 0,
				"No keys were defined for the PojoTypeComparatorBuilder.");

			checkState(
				fieldComparators.size() > 0,
				"No type comparators were defined for the PojoTypeComparatorBuilder.");

			checkState(
				keyFields.size() == fieldComparators.size(),
				"Number of key fields and field comparators is not equal.");

			return new PojoComparator<T>(
				keyFields.toArray(new Field[keyFields.size()]),
				fieldComparators.toArray(new TypeComparator[fieldComparators.size()]),
				createSerializer(config),
				getTypeClass());
		}
	}

	public static class NamedFlatFieldDescriptor extends FlatFieldDescriptor {

		private String fieldName;

		public NamedFlatFieldDescriptor(String name, int keyPosition, TypeInformation<?> type) {
			super(keyPosition, type);
			this.fieldName = name;
		}

		public String getFieldName() {
			return fieldName;
		}

		@Override
		public String toString() {
			return "NamedFlatFieldDescriptor [name="+fieldName+" position="+getPosition()+" typeInfo="+getType()+"]";
		}
	}
}
