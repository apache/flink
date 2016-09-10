/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.schema;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.lang.reflect.Field;

public class StreamSerializer<T> implements Serializable {
	private final StreamSchema<T> schema;

	public StreamSerializer(StreamSchema<T> schema) {
		this.schema = schema;
	}

	public Object[] getRow(T input) {
		Preconditions.checkArgument(input.getClass() == schema.getTypeInfo().getTypeClass()
			, "Invalid input type: " + input + ", expected: " + schema.getTypeInfo());

		Object[] data;
		if (schema.isAtomicType()) {
			data = new Object[]{input};
		} else if (schema.isTupleType()) {
			Tuple tuple = (Tuple) input;
			data = new Object[schema.getFieldIndexes().length];
			for (int i = 0; i < schema.getFieldIndexes().length; i++) {
				data[i] = tuple.getField(schema.getFieldIndexes()[i]);
			}
		} else if (schema.isPojoType() || schema.isCaseClassType()) {
			data = new Object[schema.getFieldIndexes().length];
			for (int i = 0; i < schema.getFieldNames().length; i++) {
				data[i] = getFieldValue(schema.getFieldNames()[i], input);
			}
		} else {
			throw new IllegalArgumentException("Failed to get field values from " + schema.getTypeInfo());
		}
		return data;
	}

	private Object getFieldValue(String fieldName, T input) {
		// TODO: Cache Field Accessor
		Field field = TypeExtractor.getDeclaredField(schema.getTypeInfo().getTypeClass(), fieldName);
		if (field == null) {
			throw new IllegalArgumentException(fieldName + " is not found in " + schema.getTypeInfo());
		}
		if (!field.isAccessible()) {
			field.setAccessible(true);
		}
		try {
			return field.get(input);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}
}
