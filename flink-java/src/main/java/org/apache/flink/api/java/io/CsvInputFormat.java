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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;


public class CsvInputFormat<OUT> extends GenericCsvInputFormat<OUT> {

	private static final long serialVersionUID = 1L;
	
	/**
	 * The log.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(CsvInputFormat.class);
	
	public static final String DEFAULT_LINE_DELIMITER = "\n";

	public static final String DEFAULT_FIELD_DELIMITER = ",";

	private transient Object[] parsedValues;

	private Class<OUT> pojoTypeClass = null;
	private String[] pojoFieldsName = null;
	private transient Field[] pojoFields = null;
	private transient PojoTypeInfo<OUT> pojoTypeInfo = null;

	public CsvInputFormat(Path filePath, TypeInformation<OUT> typeInformation) {
		this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, typeInformation);
	}
	
	public CsvInputFormat(Path filePath, String lineDelimiter, String fieldDelimiter, TypeInformation<OUT> typeInformation) {
		super(filePath);

		Preconditions.checkArgument(typeInformation instanceof CompositeType);
		CompositeType<OUT> compositeType = (CompositeType<OUT>) typeInformation;

		setDelimiter(lineDelimiter);
		setFieldDelimiter(fieldDelimiter);

		Class<?>[] classes = new Class<?>[typeInformation.getArity()];
		for (int i = 0, arity = typeInformation.getArity(); i < arity; i++) {
			classes[i] = compositeType.getTypeAt(i).getTypeClass();
		}
		setFieldTypes(classes);

		if (typeInformation instanceof PojoTypeInfo) {
			pojoTypeInfo = (PojoTypeInfo<OUT>) typeInformation;
			pojoTypeClass = typeInformation.getTypeClass();
			pojoFieldsName = compositeType.getFieldNames();
			setOrderOfPOJOFields(pojoFieldsName);
		}
	}

	public void setOrderOfPOJOFields(String[] fieldsOrder) {
		Preconditions.checkNotNull(pojoTypeClass, "Field order can only be specified if output type is a POJO.");
		Preconditions.checkNotNull(fieldsOrder);

		int includedCount = 0;
		for (boolean isIncluded : fieldIncluded) {
			if (isIncluded) {
				includedCount++;
			}
		}

		Preconditions.checkArgument(includedCount == fieldsOrder.length, includedCount +
			" CSV fields and " + fieldsOrder.length + " POJO fields selected. The number of selected CSV and POJO fields must be equal.");

		for (String field : fieldsOrder) {
			Preconditions.checkNotNull(field, "The field name cannot be null.");
			Preconditions.checkArgument(pojoTypeInfo.getFieldIndex(field) != -1,
				"Field \""+ field + "\" is not a member of POJO class " + pojoTypeClass.getName());
		}

		pojoFieldsName = Arrays.copyOfRange(fieldsOrder, 0, fieldsOrder.length);
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

	public void setFields(boolean[] sourceFieldMask, Class<?>[] fieldTypes) {
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
			pojoFields = new Field[pojoFieldsName.length];
			for (int i = 0; i < pojoFieldsName.length; i++) {
				try {
					pojoFields[i] = pojoTypeClass.getDeclaredField(pojoFieldsName[i]);
					pojoFields[i].setAccessible(true);
				} catch (NoSuchFieldException e) {
					throw new RuntimeException("There is no field called \"" + pojoFieldsName[i] + "\" in " + pojoTypeClass.getName(), e);
				}
			}
		}
		
		this.commentCount = 0;
		this.invalidLineCount = 0;
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
				Tuple result = (Tuple) reuse;
				for (int i = 0; i < parsedValues.length; i++) {
					result.setField(parsedValues[i], i);
				}
			} else {
				// result type is POJO
				for (int i = 0; i < parsedValues.length; i++) {
					try {
						pojoFields[i].set(reuse, parsedValues[i]);
					} catch (IllegalAccessException e) {
						throw new RuntimeException("Parsed value could not be set in POJO field \"" + pojoFieldsName[i] + "\"", e);
					}
				}
			}
			return reuse;
		} else {
			this.invalidLineCount++;
			return null;
		}
	}
	
	
	@Override
	public String toString() {
		return "CSV Input (" + StringUtils.showControlCharacters(String.valueOf(getFieldDelimiter())) + ") " + getFilePath();
	}
	
}
