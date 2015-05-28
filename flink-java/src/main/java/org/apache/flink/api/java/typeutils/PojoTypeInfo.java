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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.AvroSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoComparator;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.operators.Keys.ExpressionKeys;

import com.google.common.base.Joiner;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class PojoTypeInfo<T> extends CompositeType<T> {
	
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(PojoTypeInfo.class);

	private final static String REGEX_FIELD = "[\\p{L}_\\$][\\p{L}\\p{Digit}_\\$]*";
	private final static String REGEX_NESTED_FIELDS = "("+REGEX_FIELD+")(\\.(.+))?";
	private final static String REGEX_NESTED_FIELDS_WILDCARD = REGEX_NESTED_FIELDS
			+"|\\"+ExpressionKeys.SELECT_ALL_CHAR
			+"|\\"+ExpressionKeys.SELECT_ALL_CHAR_SCALA;

	private static final Pattern PATTERN_NESTED_FIELDS = Pattern.compile(REGEX_NESTED_FIELDS);
	private static final Pattern PATTERN_NESTED_FIELDS_WILDCARD = Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD);

	private final Class<T> typeClass;

	private PojoField[] fields;
	
	private int totalFields;

	public PojoTypeInfo(Class<T> typeClass, List<PojoField> fields) {
		super(typeClass);
		this.typeClass = typeClass;
		List<PojoField> tempFields = new ArrayList<PojoField>(fields);
		Collections.sort(tempFields, new Comparator<PojoField>() {
			@Override
			public int compare(PojoField o1, PojoField o2) {
				return o1.field.getName().compareTo(o2.field.getName());
			}
		});
		this.fields = tempFields.toArray(new PojoField[tempFields.size()]);
		
		// check if POJO is public
		if(!Modifier.isPublic(typeClass.getModifiers())) {
			throw new RuntimeException("POJO "+typeClass+" is not public");
		}
		for(PojoField field : fields) {
			totalFields += field.type.getTotalFields();
		}
	}

	@Override
	public boolean isBasicType() {
		return false;
	}


	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return fields.length;
	}
	
	@Override
	public int getTotalFields() {
		return totalFields;
	}

	@Override
	public Class<T> getTypeClass() {
		return typeClass;
	}

	@Override
	public boolean isSortKeyType() {
		// Support for sorting POJOs that implement Comparable is not implemented yet.
		// Since the order of fields in a POJO type is not well defined, sorting on fields
		//   gives only some undefined order.
		return false;
	}
	

	@Override
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
				if(pField.type instanceof CompositeType) {
					CompositeType<?> cType = (CompositeType<?>)pField.type;
					cType.getFlatFields(String.valueOf(ExpressionKeys.SELECT_ALL_CHAR), offset + keyPosition, result);
					keyPosition += cType.getTotalFields()-1;
				} else {
					result.add(new NamedFlatFieldDescriptor(pField.field.getName(), offset + keyPosition, pField.type));
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
			if (fields[i].field.getName().equals(field)) {
				fieldPos = i;
				fieldType = fields[i].type;
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

	@SuppressWarnings("unchecked")
	@Override
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
			if (fields[i].field.getName().equals(field)) {
				fieldPos = i;
				fieldType = fields[i].type;
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
	public <X> TypeInformation<X> getTypeAt(int pos) {
		if (pos < 0 || pos >= this.fields.length) {
			throw new IndexOutOfBoundsException();
		}
		@SuppressWarnings("unchecked")
		TypeInformation<X> typed = (TypeInformation<X>) fields[pos].type;
		return typed;
	}

	// used for testing. Maybe use mockito here
	public PojoField getPojoFieldAt(int pos) {
		if (pos < 0 || pos >= this.fields.length) {
			throw new IndexOutOfBoundsException();
		}
		return this.fields[pos];
	}

	/**
	 * Comparator creation
	 */
	private TypeComparator<?>[] fieldComparators;
	private Field[] keyFields;
	private int comparatorHelperIndex = 0;
	@Override
	protected void initializeNewComparator(int keyCount) {
		fieldComparators = new TypeComparator<?>[keyCount];
		keyFields = new Field[keyCount];
		comparatorHelperIndex = 0;
	}

	@Override
	protected void addCompareField(int fieldId, TypeComparator<?> comparator) {
		fieldComparators[comparatorHelperIndex] = comparator;
		keyFields[comparatorHelperIndex] = fields[fieldId].field;
		comparatorHelperIndex++;
	}

	@Override
	protected TypeComparator<T> getNewComparator(ExecutionConfig config) {
		// first remove the null array fields
		final Field[] finalKeyFields = Arrays.copyOf(keyFields, comparatorHelperIndex);
		@SuppressWarnings("rawtypes")
		final TypeComparator[] finalFieldComparators = Arrays.copyOf(fieldComparators, comparatorHelperIndex);
		if(finalFieldComparators.length == 0 || finalKeyFields.length == 0 ||  finalFieldComparators.length != finalKeyFields.length) {
			throw new IllegalArgumentException("Pojo comparator creation has a bug");
		}
		return new PojoComparator<T>(finalKeyFields, finalFieldComparators, createSerializer(config), typeClass);
	}

	public String[] getFieldNames() {
		String[] result = new String[fields.length];
		for (int i = 0; i < fields.length; i++) {
			result[i] = fields[i].field.getName();
		}
		return result;
	}

	@Override
	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].field.getName().equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		if(config.isForceKryoEnabled()) {
			return new KryoSerializer<T>(this.typeClass, config);
		}
		if(config.isForceAvroEnabled()) {
			return new AvroSerializer<T>(this.typeClass);
		}

		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[fields.length ];
		Field[] reflectiveFields = new Field[fields.length];

		for (int i = 0; i < fields.length; i++) {
			fieldSerializers[i] = fields[i].type.createSerializer(config);
			reflectiveFields[i] = fields[i].field;
		}

		return new PojoSerializer<T>(this.typeClass, fieldSerializers, reflectiveFields, config);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object obj) {
		return (obj instanceof PojoTypeInfo) && ((PojoTypeInfo<?>) obj).typeClass == this.typeClass;
	}
	
	@Override
	public int hashCode() {
		return typeClass.hashCode() + 1387562934;
	}
	
	@Override
	public String toString() {
		List<String> fieldStrings = new ArrayList<String>();
		for (PojoField field : fields) {
			fieldStrings.add(field.field.getName() + ": " + field.type.toString());
		}
		return "PojoType<" + typeClass.getName()
				+ ", fields = [" + Joiner.on(", ").join(fieldStrings) + "]"
				+ ">";
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
