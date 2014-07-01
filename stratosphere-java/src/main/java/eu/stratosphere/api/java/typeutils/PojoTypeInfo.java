/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java.typeutils;

import com.google.common.base.Joiner;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.typeutils.runtime.PojoComparator;
import eu.stratosphere.api.java.typeutils.runtime.PojoSerializer;
import eu.stratosphere.types.TypeInformation;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 *
 */
public class PojoTypeInfo<T> extends TypeInformation<T> implements CompositeType<T> {

	private final Class<T> typeClass;

	private PojoField[] fields;

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

	public int getLogicalPosition(String fieldExpression) {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].field.getName().equals(fieldExpression)) {
				return i;
			}
		}
		return -1;
	}

	public int[] getLogicalPositions(String[] fieldExpression) {
		int[] result = new int[fieldExpression.length];
		for (int i = 0; i < fieldExpression.length; i++) {
			result[i] = getLogicalPosition(fieldExpression[i]);
		}
		return result;
	}

	public TypeInformation<?> getType(String fieldExpression) {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].field.getName().equals(fieldExpression)) {
				return fields[i].type;
			}
		}
		return null;
	}

	public TypeInformation<?>[] getTypes(String[] fieldExpression) {
		TypeInformation<?>[] result = new TypeInformation<?>[fieldExpression.length];
		for (int i = 0; i < fieldExpression.length; i++) {
			result[i] = getType(fieldExpression[i]);
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

//		if (logicalKeyFields.length == 1) {
//			return createSinglefieldComparator(logicalKeyFields[0], orders[0], types[logicalKeyFields[0]]);
//		}

		// create the comparators for the individual fields
		TypeComparator<?>[] fieldComparators = new TypeComparator<?>[logicalKeyFields.length];
		Field[] keyFields = new Field[logicalKeyFields.length];

		for (int i = 0; i < logicalKeyFields.length; i++) {
			int field = logicalKeyFields[i];

			if (field < 0 || field >= fields.length) {
				throw new IllegalArgumentException("The field position " + field + " is out of range [0," + fields.length + ")");
			}
			if (fields[field].type.isKeyType() && fields[field].type instanceof AtomicType) {
				fieldComparators[i] = ((AtomicType<?>) fields[field].type).createComparator(orders[i]);
				keyFields[i] = fields[field].field;
				keyFields[i].setAccessible(true);
			} else {
				throw new IllegalArgumentException("The field at position " + field + " (" + fields[field].type + ") is no atomic key type.");
			}
		}

		return new PojoComparator<T>(keyFields, fieldComparators, createSerializer(), typeClass);
	}

	public PojoField[] getFields() {
		return fields;
	}
}
