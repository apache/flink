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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * A subclass of {@link ParquetInputFormat} to read from Parquets files and convert to POJO type.
 * It is mainly used to read simple data type without nested fields.
 */
public class ParquetPojoInputFormat<E> extends ParquetInputFormat<E> {

	private static final Logger LOG = LoggerFactory.getLogger(ParquetPojoInputFormat.class);
	private final Class<E> pojoTypeClass;
	private final TypeSerializer<E> typeSerializer;
	private transient Field[] pojoFields;

	public ParquetPojoInputFormat(Path filePath, PojoTypeInfo<E> pojoTypeInfo) {
		this(filePath, pojoTypeInfo, pojoTypeInfo.getFieldNames());
	}

	public ParquetPojoInputFormat(Path filePath, PojoTypeInfo<E> pojoTypeInfo, String[] fieldNames) {
		super(filePath, extractTypeInfos(pojoTypeInfo, fieldNames), fieldNames);
		this.pojoTypeClass = pojoTypeInfo.getTypeClass();
		this.typeSerializer = pojoTypeInfo.createSerializer(new ExecutionConfig());
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		pojoFields = new Field[fieldNames.length];

		final Map<String, Field> fieldMap = new HashMap<>();
		findAllFields(pojoTypeClass, fieldMap);

		for (int i = 0; i < fieldNames.length; ++i) {
			String fieldName = fieldNames[i];
			pojoFields[i] = fieldMap.get(fieldName);

			if (pojoFields[i] != null) {
				pojoFields[i].setAccessible(true);
			} else {
				throw new RuntimeException(
					String.format("There is no field called %s in %s", fieldName, pojoTypeClass.getName()));
			}
		}
	}

	private void findAllFields(Class<?> clazz, Map<String, Field> fieldMap) {

		for (Field field : clazz.getDeclaredFields()) {
			fieldMap.put(field.getName(), field);
		}

		if (clazz.getSuperclass() != null) {
			findAllFields(clazz.getSuperclass(), fieldMap);
		}
	}

	@Override
	protected E convert(Row row) {
		E result = typeSerializer.createInstance();
		for (int i = 0; i < row.getArity(); ++i) {
			try {
				pojoFields[i].set(result, row.getField(i));
			} catch (IllegalAccessException e) {
				throw new RuntimeException(
					String.format("Parsed value could not be set in POJO field %s", fieldNames[i]));
			}
		}

		return result;
	}

	/**
	 * Extracts the {@link TypeInformation}s  from {@link PojoTypeInfo} according to the given field name.
	 */
	private static <E> TypeInformation<?>[] extractTypeInfos(PojoTypeInfo<E> pojoTypeInfo, String[] fieldNames) {
		Preconditions.checkNotNull(pojoTypeInfo);
		Preconditions.checkNotNull(fieldNames);
		Preconditions.checkArgument(pojoTypeInfo.getArity() >= fieldNames.length);
		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[fieldNames.length];
		for (int i = 0; i < fieldNames.length; ++i) {
			String fieldName = fieldNames[i];
			Preconditions.checkNotNull(fieldName, "The field can't be null");
			int fieldPos = pojoTypeInfo.getFieldIndex(fieldName);
			Preconditions.checkArgument(fieldPos >= 0,
				String.format("Field %s is not a member of POJO type %s",
					fieldName, pojoTypeInfo.getTypeClass().getName()));
			fieldTypes[i] = pojoTypeInfo.getTypeAt(fieldPos);
		}

		return fieldTypes;
	}
}
