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

package org.apache.flink.table.runtime.batch.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.commons.io.input.BoundedInputStream;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * InputFormat that reads csv files and compliant with RFC 4180 standards.
 *
 * @param <OUT>
 */
@Internal
public abstract class RFCCsvInputFormat<OUT> extends FileInputFormat<OUT> {

	private static final long serialVersionUID = 1L;

	public static final String DEFAULT_LINE_DELIMITER = "\n";

	public static final String DEFAULT_FIELD_DELIMITER = ",";

	private String recordDelimiter = "\n";
	private char fieldDelimiter = ',';

	private boolean skipFirstLineAsHeader;
	private boolean skipFirstLine = false; // only for first split

	private boolean quotedStringParsing = false;
	private char quoteCharacter;

	private boolean lenient;

	private String commentPrefix = null;
	private boolean allowComments = false;

	private static final Class<?>[] EMPTY_TYPES = new Class<?>[0];

	private static final boolean[] EMPTY_INCLUDED = new boolean[0];

	private Class<?>[] fieldTypes = EMPTY_TYPES;

	private boolean[] fieldIncluded = EMPTY_INCLUDED;

	MappingIterator<Object[]> recordIterator = null;

	private boolean endOfSplit = false;

	protected RFCCsvInputFormat(Path filePath) {
		super(filePath);
	}

	@Override
	public void open(FileInputSplit split) throws IOException {

		super.open(split);

		CsvMapper mapper = new CsvMapper();
		mapper.disable(CsvParser.Feature.WRAP_AS_ARRAY);

		long firstDelimiterPosition = findFirstDelimiterPosition();
		long lastDelimiterPosition = findLastDelimiterPosition();
		long startPos = this.splitStart + firstDelimiterPosition;
		long endPos = this.splitLength + lastDelimiterPosition - firstDelimiterPosition;
		this.stream.seek(startPos);
		BoundedInputStream boundedInputStream = new BoundedInputStream(this.stream, endPos);

		if (skipFirstLineAsHeader && startPos == 0) {
			skipFirstLine = true;
		}

		CsvParser csvParser = mapper.getFactory().createParser(boundedInputStream);
		CsvSchema csvSchema = configureParserSettings();
		csvParser.setSchema(csvSchema);

		recordIterator = mapper.readerFor(Object[].class).readValues(csvParser);
	}

	private CsvSchema configureParserSettings() {

		CsvSchema csvSchema = CsvSchema.builder()
			.setLineSeparator(this.recordDelimiter)
			.setColumnSeparator(this.fieldDelimiter)
			.setSkipFirstDataRow(skipFirstLine)
			.setQuoteChar(this.quoteCharacter)
			.setAllowComments(allowComments)
			.build();
		return  csvSchema;
	}

	public OUT nextRecord(OUT reuse) throws IOException {

		if (recordIterator == null) {
			return null;
		}

		if (recordIterator.hasNext()) {

			Object[] record = recordIterator.next();

			if (record.length < fieldTypes.length) {
				if (isLenient()) {
					return nextRecord(reuse);
				}
				else {
					throw new ParseException();
				}
			}

			try {
				return fillRecord(reuse, castRecord(projectedFields(record)));
			}
			catch (IOException e) {
				if (isLenient()) {
					return nextRecord(reuse);
				}
				else {
					throw new ParseException(e);
				}
			}
		}
		endOfSplit = true;
		return null;
	}

	protected abstract OUT fillRecord(OUT reuse, Object[] parsedValues);

	@Override
	public String toString() {
		return "CSV Input (" + StringUtils.showControlCharacters(String.valueOf(fieldDelimiter)) + ") " + getFilePath();
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return endOfSplit;
	}

	private Object[] projectedFields(Object[] record) throws IOException {

		Object[] resultantRecord = new Object[fieldTypes.length];
		int index = 0;
		for (int i = 0; i < this.fieldIncluded.length; i++) {

			try {
				if (fieldIncluded[i]) {
					resultantRecord[index++] = record[i];
				}
			}
			catch (Exception e) {
				throw new IOException();
			}
		}
		return resultantRecord;
	}

	private long findFirstDelimiterPosition() throws IOException{

		this.stream.seek(this.getSplitStart());
		if (this.stream.getPos() == 0) {
			return 0;
		}
		else {
			int pos = 1;
			while ((this.stream.read()) != this.recordDelimiter.charAt(0)) {
				pos++;
			}
			return pos;
		}
	}

	private long findLastDelimiterPosition() throws IOException{

		this.stream.seek(this.splitStart + this.splitLength);
		int pos = 0;
		char c;
		int read = this.stream.read();
		while (read != -1) {
			c = (char) read;
			if (c == this.recordDelimiter.charAt(0)) {
				break;
			}
			else {
				read = this.stream.read();
			}
			pos++;
		}
		return pos;
	}

	private Object[] castRecord(Object[] record)  throws IOException {

		if (isLenient()) {
			for (int i = 0; i < this.fieldTypes.length; i++) {
				try {
					record[i] = dataTypeConversion(record[i], fieldTypes[i]);
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
		}
		else {
			for (int i = 0; i < this.fieldTypes.length; i++) {
				try {
					record[i] = dataTypeConversion(record[i], fieldTypes[i]);
				} catch (Exception e) {
					throw new IOException(e);
				}
			}
		}
		return record;
	}

	public <I, O> O dataTypeConversion(I input, Class<O> outputClass) throws Exception {

		String type = outputClass.getSimpleName().toLowerCase();
		try {
			switch (type) {
				case "string":
					return (O) input.toString();
				case "boolean":
					if (input.toString().length() != 0) { // special case for boolean
						return (O) Boolean.valueOf(input.toString());
					}
					return null;
				case "byte":
					return (O) Byte.valueOf(input.toString());
				case "short":
					return (O) Short.valueOf(input.toString());
				case "integer":
					return (O) Integer.valueOf(input.toString());
				case "long":
					return (O) Long.valueOf(input.toString());
				case "float":
					return (O) Float.valueOf(input.toString());
				case "double":
					return (O) Double.valueOf(input.toString());
				case "decimal":
					return (O) Double.valueOf(input.toString());
				case "date":
					return (O) Date.valueOf(input.toString());
				case "time":
					return (O) Time.valueOf(input.toString());
				case "timestamp":
					return (O) Timestamp.valueOf(input.toString());
				default:
					return (O) input;
			}
		}
		catch (Exception e) {
			if (isLenient()) {
				return null;
			}
			else {
				throw new ParseException();
			}
		}

	}

	// create projection mask

	protected static boolean[] createDefaultMask(int size) {
		boolean[] includedMask = new boolean[size];
		for (int x = 0; x < includedMask.length; x++) {
			includedMask[x] = true;
		}
		return includedMask;
	}

	protected static boolean[] toBooleanMask(int[] sourceFieldIndices) {
		Preconditions.checkNotNull(sourceFieldIndices);

		int max = 0;
		for (int i : sourceFieldIndices) {
			if (i < 0) {
				throw new IllegalArgumentException("Field indices must not be smaller than zero.");
			}
			max = Math.max(i, max);
		}

		boolean[] includedMask = new boolean[max + 1];

		// check if we support parsers for these types
		for (int i = 0; i < sourceFieldIndices.length; i++) {
			includedMask[sourceFieldIndices[i]] = true;
		}

		return includedMask;
	}

	// configuration setting

	public String getDelimiter() {
		return recordDelimiter;
	}

	public void setDelimiter(char delimiter) {
		setDelimiter(String.valueOf(delimiter));
	}

	public void setDelimiter(String delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}
		if (delimiter.length() == 0) {
			throw new IllegalArgumentException("Delimiter must not be empty");
		}
		this.recordDelimiter = delimiter;
	}

	public void setFieldDelimiter(char delimiter) {
		if (String.valueOf(delimiter) == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}
		if (String.valueOf(delimiter).length() == 0) {
			throw new IllegalArgumentException("Delimiter must not be empty");
		}
		this.fieldDelimiter = delimiter;
	}

	public void setFieldDelimiter(String delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}
		if (delimiter.length() == 0) {
			throw new IllegalArgumentException("Delimiter must not be empty");
		}
		this.fieldDelimiter = delimiter.charAt(0);
	}

	public char getFieldDelimiter() {
		return this.fieldDelimiter;
	}

	public void setSkipFirstLineAsHeader(boolean skipFirstLine) {
		this.skipFirstLineAsHeader = skipFirstLine;
	}

	public void enableQuotedStringParsing(char quoteCharacter) {
		quotedStringParsing = true;
		this.quoteCharacter = quoteCharacter;
	}

	public boolean isLenient() {
		return lenient;
	}

	public void setLenient(boolean lenient) {
		this.lenient = lenient;
	}

	public void setCommentPrefix(String commentPrefix) {
		this.commentPrefix = commentPrefix;
		this.allowComments = true;
	}

	protected Class<?>[] getGenericFieldTypes() {
		// check if we are dense, i.e., we read all fields
		if (this.fieldIncluded.length == this.fieldTypes.length) {
			return this.fieldTypes;
		}
		else {
			// sparse type array which we made dense for internal book keeping.
			// create a sparse copy to return
			Class<?>[] types = new Class<?>[this.fieldIncluded.length];

			for (int i = 0, k = 0; i < this.fieldIncluded.length; i++) {
				if (this.fieldIncluded[i]) {
					types[i] = this.fieldTypes[k++];
				}
			}

			return types;
		}
	}

	protected void setFieldsGeneric(boolean[] includedMask, Class<?>[] fieldTypes) {
		checkNotNull(includedMask);
		checkNotNull(fieldTypes);

		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if types are valid for included fields
		int typeIndex = 0;
		for (int i = 0; i < includedMask.length; i++) {

			if (includedMask[i]) {
				if (typeIndex > fieldTypes.length - 1) {
					throw new IllegalArgumentException("Missing type for included field " + i + ".");
				}
				Class<?> type = fieldTypes[typeIndex++];

				if (type == null) {
					throw new IllegalArgumentException("Type for included field " + i + " should not be null.");
				} else {
					// check if we support parsers for this type
					if (FieldParser.getParserForType(type) == null) {
						throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
					}
					types.add(type);
				}
			}
		}

		this.fieldTypes = types.toArray(new Class<?>[types.size()]);
		this.fieldIncluded = includedMask;
	}

	public Class<?>[] getFieldTypes() {
		return getGenericFieldTypes();
	}

}
