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

package org.apache.flink.api.io.parquet;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * A subclass of {@link ParquetInputFormat} to read from Parquet files and convert to POJO type.
 */
public class PojoParquetInputFormat<OUT> extends ParquetInputFormat<OUT> {
	private static final long serialVersionUID = -7396017155381358924L;

	private final Class<OUT> pojoTypeClass;
	private final TypeSerializer<OUT> pojoSerializer;
	private transient Field[] pojoFields;

	public PojoParquetInputFormat(Path filePath, PojoTypeInfo<OUT> pojoTypeInfo) {
		this(filePath, pojoTypeInfo, pojoTypeInfo.getFieldNames());
	}

	public PojoParquetInputFormat(Path filePath, PojoTypeInfo<OUT> pojoTypeInfo, String[] fieldNames) {
		super(filePath, extractTypeInfos(pojoTypeInfo, fieldNames), fieldNames);

		this.pojoSerializer = pojoTypeInfo.createSerializer(new ExecutionConfig());
		this.pojoTypeClass = pojoTypeInfo.getTypeClass();
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		pojoFields = new Field[fieldNames.length];

		final Map<String, Field> allFields = new HashMap<>();
		findAllFields(pojoTypeClass, allFields);

		for (int i = 0; i < fieldNames.length; i++) {
			String fieldName = fieldNames[i];
			pojoFields[i] = allFields.get(fieldName);

			if (pojoFields[i] != null) {
				pojoFields[i].setAccessible(true);
			} else {
				throw new RuntimeException(
						"There is no field called \"" + fieldName + "\" in " + pojoTypeClass.getName());
			}
		}
	}

	@Override
	public OUT convert(Row current, OUT reuse) {
		if (reuse == null) {
			reuse = pojoSerializer.createInstance();
		}
		for (int i = 0; i < current.getArity(); ++i) {
			try {
				pojoFields[i].set(reuse, current.getField(i));
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Parsed value could not be set in POJO field \"" + fieldNames[i] + "\"", e);
			}
		}
		return reuse;
	}

	/**
	 * Finds all declared fields in a class and all its super classes.
	 *
	 * @param clazz Class for which all declared fields are found
	 * @param allFields Map containing all found fields so far
	 */
	private void findAllFields(Class<?> clazz, Map<String, Field> allFields) {
		for (Field field : clazz.getDeclaredFields()) {
			allFields.put(field.getName(), field);
		}

		if (clazz.getSuperclass() != null) {
			findAllFields(clazz.getSuperclass(), allFields);
		}
	}

	/**
	 * Extracts the {@link TypeInformation}s from {@link PojoTypeInfo} corresponding to the given fieldNames.
	 */
	private static <OUT> TypeInformation<?>[] extractTypeInfos(PojoTypeInfo<OUT> pojoTypeInfo, String[] fieldNames) {
		Preconditions.checkNotNull(pojoTypeInfo);
		Preconditions.checkNotNull(fieldNames);
		Preconditions.checkArgument(pojoTypeInfo.getArity() >= fieldNames.length);

		TypeInformation<?>[] fieldTypes = new TypeInformation<?>[fieldNames.length];
		for (int i = 0; i < fieldNames.length; ++i) {
			String fieldName = fieldNames[i];
			Preconditions.checkNotNull(fieldName, "The field name cannot be null.");
			int fieldPos = pojoTypeInfo.getFieldIndex(fieldName);
			Preconditions.checkArgument(fieldPos >= 0, "Field \"" + fieldName + "\" is not a member of POJO class "
					+ pojoTypeInfo.getTypeClass().getName());
			fieldTypes[i] = pojoTypeInfo.getTypeAt(fieldPos);
		}
		return fieldTypes;
	}

}
