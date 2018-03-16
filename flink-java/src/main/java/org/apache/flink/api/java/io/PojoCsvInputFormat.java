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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Input format that reads csv into POJOs.
 * @param <OUT> resulting POJO type
 */
@Internal
public class PojoCsvInputFormat<OUT> extends CsvInputFormat<OUT> {

	private static final long serialVersionUID = 1L;

	private Class<OUT> pojoTypeClass;

	private String[] pojoFieldNames;

	private transient PojoTypeInfo<OUT> pojoTypeInfo;
	private transient Field[] pojoFields;

	public PojoCsvInputFormat(Path filePath, PojoTypeInfo<OUT> pojoTypeInfo) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, pojoTypeInfo);
	}

	public PojoCsvInputFormat(Path filePath, PojoTypeInfo<OUT> pojoTypeInfo, String[] fieldNames) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, pojoTypeInfo, fieldNames, createDefaultMask(pojoTypeInfo.getArity()));
	}

	public PojoCsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, PojoTypeInfo<OUT> pojoTypeInfo) {
		this(filePath, lineDelimiter, fieldDelimiter, pojoTypeInfo, pojoTypeInfo.getFieldNames(), createDefaultMask(pojoTypeInfo.getArity()));
	}

	public PojoCsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, PojoTypeInfo<OUT> pojoTypeInfo, String[] fieldNames) {
		this(filePath, lineDelimiter, fieldDelimiter, pojoTypeInfo, fieldNames, createDefaultMask(fieldNames.length));
	}

	public PojoCsvInputFormat(Path filePath, PojoTypeInfo<OUT> pojoTypeInfo, int[] includedFieldsMask) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, pojoTypeInfo, pojoTypeInfo.getFieldNames(), toBooleanMask(includedFieldsMask));
	}

	public PojoCsvInputFormat(Path filePath, PojoTypeInfo<OUT> pojoTypeInfo, String[] fieldNames, int[] includedFieldsMask) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, pojoTypeInfo, fieldNames, includedFieldsMask);
	}

	public PojoCsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, PojoTypeInfo<OUT> pojoTypeInfo, int[] includedFieldsMask) {
		this(filePath, lineDelimiter, fieldDelimiter, pojoTypeInfo, pojoTypeInfo.getFieldNames(), includedFieldsMask);
	}

	public PojoCsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, PojoTypeInfo<OUT> pojoTypeInfo, String[] fieldNames, int[] includedFieldsMask) {
		super(filePath);
		boolean[] mask = (includedFieldsMask == null)
				? createDefaultMask(fieldNames.length)
				: toBooleanMask(includedFieldsMask);
		configure(lineDelimiter, fieldDelimiter, pojoTypeInfo, fieldNames, mask);
	}

	public PojoCsvInputFormat(Path filePath, PojoTypeInfo<OUT> pojoTypeInfo, boolean[] includedFieldsMask) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, pojoTypeInfo, pojoTypeInfo.getFieldNames(), includedFieldsMask);
	}

	public PojoCsvInputFormat(Path filePath, PojoTypeInfo<OUT> pojoTypeInfo, String[] fieldNames, boolean[] includedFieldsMask) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, pojoTypeInfo, fieldNames, includedFieldsMask);
	}

	public PojoCsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, PojoTypeInfo<OUT> pojoTypeInfo, boolean[] includedFieldsMask) {
		this(filePath, lineDelimiter, fieldDelimiter, pojoTypeInfo, pojoTypeInfo.getFieldNames(), includedFieldsMask);
	}

	public PojoCsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, PojoTypeInfo<OUT> pojoTypeInfo, String[] fieldNames, boolean[] includedFieldsMask) {
		super(filePath);
		configure(lineDelimiter, fieldDelimiter, pojoTypeInfo, fieldNames, includedFieldsMask);
	}

	private void configure(String lineDelimiter, String fieldDelimiter, PojoTypeInfo<OUT> pojoTypeInfo, String[] fieldNames, boolean[] includedFieldsMask) {

		if (includedFieldsMask == null) {
			includedFieldsMask = createDefaultMask(fieldNames.length);
		}

		for (String name : fieldNames) {
			if (name == null) {
				throw new NullPointerException("Field name must not be null.");
			}
			if (pojoTypeInfo.getFieldIndex(name) < 0) {
				throw new IllegalArgumentException("Field \"" + name + "\" not part of POJO type " + pojoTypeInfo.getTypeClass().getCanonicalName());
			}
		}

		setDelimiter(lineDelimiter);
		setFieldDelimiter(fieldDelimiter);

		Class<?>[] classes = new Class<?>[fieldNames.length];

		for (int i = 0; i < fieldNames.length; i++) {
			try {
				classes[i] = pojoTypeInfo.getTypeAt(pojoTypeInfo.getFieldIndex(fieldNames[i])).getTypeClass();
			} catch (IndexOutOfBoundsException e) {
				throw new IllegalArgumentException("Invalid field name: " + fieldNames[i]);
			}
		}

		this.pojoTypeClass = pojoTypeInfo.getTypeClass();
		this.pojoTypeInfo = pojoTypeInfo;
		setFieldsGeneric(includedFieldsMask, classes);
		setOrderOfPOJOFields(fieldNames);
	}

	private void setOrderOfPOJOFields(String[] fieldNames) {
		Preconditions.checkNotNull(fieldNames);

		int includedCount = 0;
		for (boolean isIncluded : fieldIncluded) {
			if (isIncluded) {
				includedCount++;
			}
		}

		Preconditions.checkArgument(includedCount == fieldNames.length, includedCount
				+ " CSV fields and " + fieldNames.length + " POJO fields selected. The number of selected CSV and POJO fields must be equal.");

		for (String field : fieldNames) {
			Preconditions.checkNotNull(field, "The field name cannot be null.");
			Preconditions.checkArgument(pojoTypeInfo.getFieldIndex(field) != -1,
					"Field \"" + field + "\" is not a member of POJO class " + pojoTypeClass.getName());
		}

		pojoFieldNames = Arrays.copyOfRange(fieldNames, 0, fieldNames.length);
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		pojoFields = new Field[pojoFieldNames.length];

		Map<String, Field> allFields = new HashMap<String, Field>();

		findAllFields(pojoTypeClass, allFields);

		for (int i = 0; i < pojoFieldNames.length; i++) {
			pojoFields[i] = allFields.get(pojoFieldNames[i]);

			if (pojoFields[i] != null) {
				pojoFields[i].setAccessible(true);
			} else {
				throw new RuntimeException("There is no field called \"" + pojoFieldNames[i] + "\" in " + pojoTypeClass.getName());
			}
		}
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

	@Override
	public OUT fillRecord(OUT reuse, Object[] parsedValues) {
		for (int i = 0; i < parsedValues.length; i++) {
			try {
				pojoFields[i].set(reuse, parsedValues[i]);
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Parsed value could not be set in POJO field \"" + pojoFieldNames[i] + "\"", e);
			}
		}
		return reuse;
	}
}
