/**
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

import com.google.common.base.Joiner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.Validate;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.GenericTypeComparator;
import org.apache.flink.api.java.typeutils.runtime.PojoComparator;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.types.TypeInformation;


/**
 *
 */
public class PojoTypeInfo<T> extends TypeInformation<T> implements CompositeType<T>, AtomicType<T> {

	private final Class<T> typeClass;

	private PojoField[] fields;

	private PojoFieldAccessor[] flattenedFields;

	public PojoTypeInfo(Class<T> typeClass, List<PojoField> fields) {
		this.typeClass = typeClass;
		List<PojoField> tempFields = new ArrayList<PojoField>(fields);
		Collections.sort(tempFields, new Comparator<PojoField>() {
			@Override
			public int compare(PojoField o1, PojoField o2) {
				return o1.field.getName().compareTo(o2.field.getName());
			}
		});
		this.fields = tempFields.toArray(new PojoField[tempFields.size()]);
		List<PojoFieldAccessor> flatFieldsList = getFlattenedFields(new LinkedList<Field>());

		flattenedFields = flatFieldsList.toArray(new PojoFieldAccessor[flatFieldsList.size()]);
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
	public Class<T> getTypeClass() {
		return typeClass;
	}

	@Override
	public boolean isKeyType() {
		return Comparable.class.isAssignableFrom(typeClass);
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending) {
		if (isKeyType()) {
			@SuppressWarnings("rawtypes")
			GenericTypeComparator comparator = new GenericTypeComparator(sortOrderAscending, createSerializer(), this.typeClass);
			return (TypeComparator<T>) comparator;
		}

		throw new UnsupportedOperationException("Types that do not implement java.lang.Comparable cannot be used as keys.");
	}

	@Override
	public TypeSerializer<T> createSerializer() {
		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[fields.length];
		Field[] reflectiveFields = new Field[fields.length];

		for (int i = 0; i < fields.length; i++) {
			fieldSerializers[i] = fields[i].type.createSerializer();
			reflectiveFields[i] = fields[i].field;
		}

		return new PojoSerializer<T>(this.typeClass, fieldSerializers, reflectiveFields);
	}

	@Override
	public String toString() {
		List<String> fieldStrings = new ArrayList<String>();
		for (PojoField field : fields) {
			fieldStrings.add(field.field.getName() + ": " + field.type.toString());
		}
		return "PojoType<" + typeClass.getCanonicalName()
				+ ", fields = [" + Joiner.on(", ").join(fieldStrings) + "]"
				+ ">";
	}

	// get the field from this pojo of a nested pojo the field expression contains dot(s)
	private PojoField getField(String fieldExpression) {
		Validate.notEmpty(fieldExpression, "Field expression must not be empty.");

		// if there is a dot try getting the field from that sub field
		int firstDot = fieldExpression.indexOf('.');
		if (firstDot == -1) {
			// this is the last field (or only field) in the field expression
			for (int i = 0; i < fields.length; i++) {
				if (fields[i].field.getName().equals(fieldExpression)) {
					return fields[i];
				}
			}
		} else {
			// split and go deeper
			String firstField = fieldExpression.substring(0, firstDot);
			String rest = fieldExpression.substring(firstDot + 1);
			for (int i = 0; i < fields.length; i++) {
				if (fields[i].field.getName().equals(firstField)) {
					if (!(fields[i].type instanceof  PojoTypeInfo)) {
						return null;
					}
					PojoTypeInfo pojoField = (PojoTypeInfo) fields[i].type;
					return pojoField.getField(rest);
				}
			}
		}
		return null;
	}

	// The logical position can identify a field since we rely on the list
	// of flattened fields which is deterministic. We need the logical positions since this
	// is the only way to pass field positions to a comparator.
	// We return -1 in case we don't find the field. Keys.ExpressionKeys will handle this.
	private int getLogicalPosition(String fieldExpression) {
		PojoField expressionField = getField(fieldExpression);
		if (expressionField == null) {
			return -1;
		}

		for (int i = 0; i < flattenedFields.length; i++) {
			if (flattenedFields[i].pojoField.equals(expressionField)) {
				return i;
			}
		}

		return -1;
	}

	@Override
	public int[] getLogicalPositions(String[] fieldExpression) {
		int[] result = new int[fieldExpression.length];
		for (int i = 0; i < fieldExpression.length; i++) {
			result[i] = getLogicalPosition(fieldExpression[i]);
		}
		return result;
	}

	private TypeInformation<?> getType(String fieldExpression) {
		PojoField expressionField = getField(fieldExpression);
		if (expressionField == null) {
			return null;
		}

		for (int i = 0; i < flattenedFields.length; i++) {
			if (flattenedFields[i].pojoField.equals(expressionField)) {
				return flattenedFields[i].pojoField.type;
			}
		}
		return null;
	}

	@Override
	public TypeInformation<?>[] getTypes(String[] fieldExpression) {
		TypeInformation<?>[] result = new TypeInformation<?>[fieldExpression.length];
		for (int i = 0; i < fieldExpression.length; i++) {
			result[i] = getType(fieldExpression[i]);
		}
		return result;
	}

	// Flatten fields of inner pojo classes into one list with deterministic order so that
	// we can use it to derive logical key positions/
	private final List<PojoFieldAccessor> getFlattenedFields(List<Field> accessorChain) {
		List<PojoFieldAccessor> result = new ArrayList<PojoFieldAccessor>();

		for (PojoField field : fields) {
			if (field.type instanceof PojoTypeInfo) {
				PojoTypeInfo<?> pojoField = (PojoTypeInfo<?>)field.type;
				List<Field> newAccessorChain = new ArrayList<Field>();
				newAccessorChain.addAll(accessorChain);
				newAccessorChain.add(field.field);
				result.addAll(pojoField.getFlattenedFields(newAccessorChain));
			} else {
				result.add(new PojoFieldAccessor(accessorChain, field.field, field));
			}
		}

		return result;
	}

	@Override
	public TypeComparator<T> createComparator(int[] logicalKeyFields, boolean[] orders) {
		// sanity checks
		if (logicalKeyFields == null || orders == null || logicalKeyFields.length != orders.length ||
				logicalKeyFields.length > fields.length)
		{
			throw new IllegalArgumentException();
		}

		// create the comparators for the individual fields
		TypeComparator<?>[] fieldComparators = new TypeComparator<?>[logicalKeyFields.length];
		List<Field>[] keyFields = new List[logicalKeyFields.length];
		for (int i = 0; i < logicalKeyFields.length; i++) {
			int field = logicalKeyFields[i];

			if (field < 0 || field >= flattenedFields.length) {
				throw new IllegalArgumentException("The field position " + field + " is out of range [0," + flattenedFields.length + ")");
			}
			if (flattenedFields[field].pojoField.type.isKeyType() && flattenedFields[field].pojoField.type instanceof AtomicType) {
				fieldComparators[i] = ((AtomicType<?>) flattenedFields[field].pojoField.type).createComparator(orders[i]);
				keyFields[i] = flattenedFields[field].accessorChain;
				for (Field accessedField : keyFields[i]) {
					accessedField.setAccessible(true);
				}
			} else {
				throw new IllegalArgumentException("The field at position " + field + " (" + flattenedFields[field].pojoField.type + ") is no atomic key type.");
			}
		}

		return new PojoComparator<T>(keyFields, fieldComparators, createSerializer(), typeClass);
	}
}
