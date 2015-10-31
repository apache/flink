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

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.io.GenericCsvInputFormat;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class CommonCsvInputFormat<OUT> extends GenericCsvInputFormat<OUT> {

	private static final long serialVersionUID = 1L;

	public static final String DEFAULT_LINE_DELIMITER = "\n";

	public static final String DEFAULT_FIELD_DELIMITER = ",";

	protected transient Object[] parsedValues;

	private final  Class<OUT> pojoTypeClass;

	private String[] pojoFieldNames;

	private transient PojoTypeInfo<OUT> pojoTypeInfo;
	private transient Field[] pojoFields;

	public CommonCsvInputFormat(Path filePath, CompositeType<OUT> typeInformation) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, typeInformation);
	}

	public CommonCsvInputFormat(
			Path filePath,
			String lineDelimiter,
			String fieldDelimiter,
			CompositeType<OUT> compositeTypeInfo) {
		super(filePath);

		setDelimiter(lineDelimiter);
		setFieldDelimiter(fieldDelimiter);

		Class<?>[] classes = new Class<?>[compositeTypeInfo.getArity()];

		for (int i = 0; i < compositeTypeInfo.getArity(); i++) {
			classes[i] = compositeTypeInfo.getTypeAt(i).getTypeClass();
		}

		setFieldTypes(classes);

		if (compositeTypeInfo instanceof PojoTypeInfo) {
			pojoTypeInfo = (PojoTypeInfo<OUT>) compositeTypeInfo;

			pojoTypeClass = compositeTypeInfo.getTypeClass();
			setOrderOfPOJOFields(compositeTypeInfo.getFieldNames());
		} else {
			pojoTypeClass = null;
			pojoFieldNames = null;
		}
	}

	public void setOrderOfPOJOFields(String[] fieldNames) {
		Preconditions.checkNotNull(pojoTypeClass, "Field order can only be specified if output type is a POJO.");
		Preconditions.checkNotNull(fieldNames);

		int includedCount = 0;
		for (boolean isIncluded : fieldIncluded) {
			if (isIncluded) {
				includedCount++;
			}
		}

		Preconditions.checkArgument(includedCount == fieldNames.length, includedCount +
			" CSV fields and " + fieldNames.length + " POJO fields selected. The number of selected CSV and POJO fields must be equal.");

		for (String field : fieldNames) {
			Preconditions.checkNotNull(field, "The field name cannot be null.");
			Preconditions.checkArgument(pojoTypeInfo.getFieldIndex(field) != -1,
				"Field \""+ field + "\" is not a member of POJO class " + pojoTypeClass.getName());
		}

		pojoFieldNames = Arrays.copyOfRange(fieldNames, 0, fieldNames.length);
	}

	public void setFieldTypes(Class<?>... fieldTypes) {
		if (fieldTypes == null || fieldTypes.length == 0) {
			throw new IllegalArgumentException("Field types must not be null or empty.");
		}

		setFieldTypesGeneric(fieldTypes);
	}

	public void setFields(int[] sourceFieldIndices, Class<?>[] fieldTypes) {
		Preconditions.checkNotNull(sourceFieldIndices);
		Preconditions.checkNotNull(fieldTypes);

		checkForMonotonousOrder(sourceFieldIndices, fieldTypes);

		setFieldsGeneric(sourceFieldIndices, fieldTypes);
	}

	public  void setFields(boolean[] sourceFieldMask, Class<?>[] fieldTypes) {
		Preconditions.checkNotNull(sourceFieldMask);
		Preconditions.checkNotNull(fieldTypes);

		setFieldsGeneric(sourceFieldMask, fieldTypes);
	}

	public Class<?>[] getFieldTypes() {
		return super.getGenericFieldTypes();
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);

		@SuppressWarnings("unchecked")
		FieldParser<Object>[] fieldParsers = (FieldParser<Object>[]) getFieldParsers();

		//throw exception if no field parsers are available
		if (fieldParsers.length == 0) {
			throw new IOException("CsvInputFormat.open(FileInputSplit split) - no field parsers to parse input");
		}

		// create the value holders
		this.parsedValues = new Object[fieldParsers.length];
		for (int i = 0; i < fieldParsers.length; i++) {
			this.parsedValues[i] = fieldParsers[i].createValue();
		}

		// left to right evaluation makes access [0] okay
		// this marker is used to fasten up readRecord, so that it doesn't have to check each call if the line ending is set to default
		if (this.getDelimiter().length == 1 && this.getDelimiter()[0] == '\n' ) {
			this.lineDelimiterIsLinebreak = true;
		}

		// for POJO type
		if (pojoTypeClass != null) {
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

		this.commentCount = 0;
		this.invalidLineCount = 0;
	}

	/**
	 * Finds all declared fields in a class and all its super classes.
	 *
	 * @param clazz Class for which all declared fields are found
	 * @param allFields Map containing all found fields so far
	 */
	private void findAllFields(Class<?> clazz, Map<String, Field> allFields) {
		for (Field field: clazz.getDeclaredFields()) {
			allFields.put(field.getName(), field);
		}

		if (clazz.getSuperclass() != null) {
			findAllFields(clazz.getSuperclass(), allFields);
		}
	}

	@Override
	public OUT nextRecord(OUT record) throws IOException {
		OUT returnRecord = null;
		do {
			returnRecord = super.nextRecord(record);
		} while (returnRecord == null && !reachedEnd());

		return returnRecord;
	}

	@Override
	public OUT readRecord(OUT reuse, byte[] bytes, int offset, int numBytes) throws IOException {
		/*
		 * Fix to support windows line endings in CSVInputFiles with standard delimiter setup = \n
		 */
		//Find windows end line, so find carriage return before the newline
		if (this.lineDelimiterIsLinebreak == true && numBytes > 0 && bytes[offset + numBytes -1] == '\r' ) {
			//reduce the number of bytes so that the Carriage return is not taken as data
			numBytes--;
		}

		if (commentPrefix != null && commentPrefix.length <= numBytes) {
			//check record for comments
			boolean isComment = true;
			for (int i = 0; i < commentPrefix.length; i++) {
				if (commentPrefix[i] != bytes[offset + i]) {
					isComment = false;
					break;
				}
			}
			if (isComment) {
				this.commentCount++;
				return null;
			}
		}

		if (parseRecord(parsedValues, bytes, offset, numBytes)) {
			if (pojoTypeClass == null) {
				// result type is tuple
				return createTuple(reuse);
			} else {
				// result type is POJO
				for (int i = 0; i < parsedValues.length; i++) {
					try {
						pojoFields[i].set(reuse, parsedValues[i]);
					} catch (IllegalAccessException e) {
						throw new RuntimeException("Parsed value could not be set in POJO field \"" + pojoFieldNames[i] + "\"", e);
					}
				}
				return reuse;
			}

		} else {
			this.invalidLineCount++;
			return null;
		}
	}

	protected abstract OUT createTuple(OUT reuse);
}
