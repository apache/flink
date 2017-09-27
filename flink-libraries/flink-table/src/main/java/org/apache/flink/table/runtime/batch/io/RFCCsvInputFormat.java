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

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava18.com.google.common.primitives.Bytes;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.commons.io.input.BoundedInputStream;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * InputFormat to read data from CSV files that are compliant with the RFC 4180 standard.
 *
 * @param <OUT> type of output
 */
public abstract class RFCCsvInputFormat<OUT> extends FileInputFormat<OUT> {

	private static final long serialVersionUID = 1L;

	/** Record delimiter, "\n" by default. */
	public static final String DEFAULT_RECORD_DELIMITER = "\n";
	private String recordDelimiterString = DEFAULT_RECORD_DELIMITER;
	private byte[] recordDelimiter = recordDelimiterString.getBytes();

	/** Field delimiter, ',' by default. */
	public static final char DEFAULT_FIELD_DELIMITER = ',';
	private char fieldDelimiter = DEFAULT_FIELD_DELIMITER;

	/** Skip the header line only for the first split. */
	private boolean skipFirstLineAsHeader;
	private boolean skipFirstLine = false;

	/** To enable quoted string parsing, by default it's disabled. */
	private boolean quotedStringParsing = false;
	private char quoteCharacter;

	/** To skip records with parse error instead to fail. Throw an exception by default. */
	private boolean lenient = false;

	/** to skip the records with prefix '#'. */
	private boolean allowComments = false;

	/** Type Information describing the result type. */
	private TypeInformation[] fieldTypes = null;

	/** Projection mask to get only selected fields. */
	private boolean[] fieldIncluded = new boolean[0];

	/** Record Iterator for a split returned by Csv parser. */
	private MappingIterator<String[]> recordIterator = null;

	/** End of split, if record no record in split. */
	private boolean endOfSplit = false;

	/** The read buffer to find the first and last record delimiters for splits, default size is 4KB. */
	private transient byte[] readBuffer;
	private static final int DEFAULT_READ_BUFFER_SIZE = 4 * 1024;
	private int bufferSize = -1;

	protected RFCCsvInputFormat(Path filePath) {
		super(filePath);
	}

	@Override
	public void open(FileInputSplit split) throws IOException {

		super.open(split);

		initBuffers();
		long firstDelimiterPosition = findFirstDelimiterPosition();
		long lastDelimiterPosition = findLastDelimiterPosition();
		long readStartingPoint = this.splitStart + firstDelimiterPosition;
		long readLength = this.splitLength + lastDelimiterPosition - firstDelimiterPosition;
		this.stream.seek(readStartingPoint);
		BoundedInputStream boundedInputStream = new BoundedInputStream(this.stream, readLength);

		if (skipFirstLineAsHeader && readStartingPoint == 0) {
			skipFirstLine = true;
		}

		CsvMapper mapper = new CsvMapper();
		mapper.disable(CsvParser.Feature.WRAP_AS_ARRAY);
		CsvParser csvParser = mapper.getFactory().createParser(boundedInputStream);
		CsvSchema csvSchema = configureParserSettings();
		csvParser.setSchema(csvSchema);

		recordIterator = mapper.readerFor(String[].class).readValues(csvParser);

	}

	public OUT nextRecord(OUT reuse) throws IOException {

		if (recordIterator == null) {
			throw new IllegalStateException("Parser is not initialized, first call open() function of RFCCsvInputFormat");
		}

		if (recordIterator.hasNext()) {

			String[] record;
			try {
				record = recordIterator.next();
				if (record.length < fieldTypes.length) {
					if (isLenient()) {
						return nextRecord(reuse);
					}
					else {
						throw new ParseException();
					}
				}
				return fillRecord(reuse, projectAndCastFields(record));
			}
			catch (Exception e) {
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

	/**
	 * Configure the CsvSchema used by the Csv Parser.
	 * @return CsvSchema for parsing
	 */
	private CsvSchema configureParserSettings() {

		CsvSchema.Builder csvSchema = CsvSchema.builder()
			.setLineSeparator(this.recordDelimiterString)
			.setColumnSeparator(this.fieldDelimiter)
			.setSkipFirstDataRow(this.skipFirstLine)
			.setAllowComments(this.allowComments);
		if (this.quotedStringParsing) {
			if (getRuntimeContext().getNumberOfParallelSubtasks() > 1) {
				throw new ParseException("Quoted string parsing is only supported with parallelism One");
			}
			csvSchema.setQuoteChar(this.quoteCharacter);
		}
		return  csvSchema.build();
	}

	/**
	 * Initialize the read buffer, used to find first and last record delimiters for current split.
	 */
	private void initBuffers() {
		this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;

		if (this.bufferSize <= this.recordDelimiter.length) {
			throw new IllegalArgumentException("Buffer size must be greater than length of delimiter.");
		}

		if (this.readBuffer == null || this.readBuffer.length != this.bufferSize) {
			this.readBuffer = new byte[this.bufferSize];
		}

		this.endOfSplit = false;
	}

	/**
	 * Fills the read buffer with bytes read from the stream.
	 * @return	the total number of bytes read into the buffer, or
	 * 			<code>-1</code> if there is no more data because the end of
	 * 			the stream has been reached.
	 * @throws	IOException if the input stream has been closed, or if
	 * 			some other I/O error occurs.
	 */
	private int fillBuffer() throws IOException {
		return this.stream.read(this.readBuffer, 0, this.bufferSize);
	}

	/**
	 * Find the index of first record delimiter in a 'current split', used as a starting point of split
	 * to create BoundedInputStream for csv parser.
	 * @return	index of first record delimiter in a split + <code>1</code>,
	 * 			<code>0</code> if current split is first split
	 * @throws	IOException
	 */
	private long findFirstDelimiterPosition() throws IOException{

		if (this.stream.getPos() == 0) {
			return 0;
		}
		else {
			this.stream.seek(this.getSplitStart());

			int requests = 0;
			int offset = 0;
			while (requests < 10) {

				int read = fillBuffer();
				if (read < 0) {
					throw new IOException();
				}
				offset = offset + this.bufferSize;

				int indexOf = Bytes.indexOf(this.readBuffer, this.recordDelimiter);
				if (indexOf > 0) {
					return indexOf + 1;
				}

				requests++;
			}
			throw new IOException();

		}
	}

	/**
	 * Find the index of first record delimiter in the 'next split' of 'current split', used as an ending point of split
	 * to create BoundedInputStream for csv parser.
	 * @return	index of first record delimiter in the 'next split' of 'current split',
	 * 			<code>0</code> if 'current split' is 'last split'
	 *
	 * @throws	IOException
	 */
	private long findLastDelimiterPosition() throws IOException{

		this.stream.seek(this.splitStart + this.splitLength);

		int offset = 0;
		int read = fillBuffer();
		if (read < 0) {
			return 0;
		}
		offset = offset + this.bufferSize;

		int indexOf = Bytes.indexOf(this.readBuffer, this.recordDelimiter);
		if (indexOf > 0) {
			return indexOf;
		}

		int requests = 0;
		while (requests < 10) {

			read = fillBuffer();
			offset = offset + this.bufferSize;
			if (read < 0) {
				throw new IOException();
			}

			indexOf = Bytes.indexOf(this.readBuffer, this.recordDelimiter);
			if (indexOf > 0) {
				return indexOf;
			}

			requests++;
		}
		throw new IOException();

	}

	/**
	 * Project and Cast the string array into object array using given projection mask and filed types.
	 * @param record string array of
	 * @return record as an object array
	 * @throws IOException
	 */
	private Object[] projectAndCastFields(String[] record) throws IOException {

		Object[] projectedFields = new Object[fieldTypes.length];
		int index = 0;
		for (int i = 0; i < this.fieldIncluded.length; i++) {

			try {
				if (fieldIncluded[i]) {

					TypeInformation type = fieldTypes[index];
					try {
						if (record[i].length() == 0) {
							if (type.equals(Types.STRING)) {
								projectedFields[index] = record[i];
							}
							else {
								projectedFields[index] = null;
							}
						}
						else if (type.equals(Types.BOOLEAN)) {
							projectedFields[index] = Boolean.valueOf(record[i]);
						}
						else if (type.equals(Types.BYTE)) {
							projectedFields[index] = Byte.valueOf(record[i]);
						}
						else if (type.equals(Types.SHORT)) {
							projectedFields[index] = Short.valueOf(record[i]);
						}
						else if (type.equals(Types.INT)) {
							projectedFields[index] = Integer.valueOf(record[i]);
						}
						else if (type.equals(Types.LONG)) {
							projectedFields[index] = Long.valueOf(record[i]);
						}
						else if (type.equals(Types.FLOAT)) {
							projectedFields[index] = Float.valueOf(record[i]);
						}
						else if (type.equals(Types.DOUBLE)) {
							projectedFields[index] = Double.valueOf(record[i]);
						}
						else if (type.equals(Types.DECIMAL)) {
							projectedFields[index] = Double.valueOf(record[i]);
						}
						else if (type.equals(Types.SQL_DATE)) {
							projectedFields[index] = Date.valueOf(record[i]);
						}
						else if (type.equals(Types.SQL_TIME)) {
							projectedFields[index] = Time.valueOf(record[i]);
						}
						else if (type.equals(Types.SQL_TIMESTAMP)) {
							projectedFields[index] = Timestamp.valueOf(record[i]);
						}
						else {
							projectedFields[index] = record[i];
						}
					}
					catch (Exception e) {
						throw new ParseException();
					}
					index++;
				}
			}
			catch (Exception e) {
				throw new IOException();
			}
		}
		return projectedFields;
	}

	/**
	 * Default projection mask to select all fields in a record.
	 * @param size number of fields in a record
	 * @return boolean array of given size with all true values
	 */
	protected static boolean[] createDefaultMask(int size) {
		boolean[] includedMask = new boolean[size];
		Arrays.fill(includedMask, true);
		return includedMask;
	}

	/**
	 * Projection mask to select required fields in a record.
	 * @param sourceFieldIndices Indices of projected fields
	 * @return boolean array with true values only for projected fields
	 */
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
		for (int i : sourceFieldIndices) {
			includedMask[i] = true;
		}

		return includedMask;
	}

	/**
	 * Set the field types and projection mask.
	 * Also, check that for given filed types parsing is supported or not.
	 * @param includedMask projection mask
	 * @param fieldTypes field types of projected fields
	 */
	protected void setOnlySupportedFieldsTypes(boolean[] includedMask, TypeInformation[] fieldTypes) {
		checkNotNull(includedMask);
		checkNotNull(fieldTypes);

		// check if types are valid for included fields
		int typeIndex = 0;
		for (int i = 0; i < includedMask.length; i++) {

			if (includedMask[i]) {
				if (typeIndex > fieldTypes.length - 1) {
					throw new IllegalArgumentException("Missing type for included field " + i + ".");
				}
				Class<?> type = fieldTypes[typeIndex++].getTypeClass();

				if (type == null) {
					throw new IllegalArgumentException("Type for included field " + i + " should not be null.");
				} else {
					// check if we support parsers for this type
					if (FieldParser.getParserForType(type) == null) {
						throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
					}
				}
			}
		}
		this.fieldTypes = fieldTypes;
		this.fieldIncluded = includedMask;
	}

	/**
	 * Get record delimiter used for parsing.
	 * @return record delimiter as byte array
	 */
	public byte[] getRecordDelimiter() {
		return this.recordDelimiter;
	}

	/**
	 * Set record delimiter used for parsing.
	 * @param recordDelimiter char record delimiter
	 */
	public void setRecordDelimiter(char recordDelimiter) {
		setRecordDelimiter(String.valueOf(recordDelimiter));
	}

	/**
	 * Set record delimiter used for parsing.
	 * @param recordDelimiter string record delimiter
	 */
	public void setRecordDelimiter(String recordDelimiter) {
		if (recordDelimiter == null) {
			throw new IllegalArgumentException("Record delimiter must not be null");
		}
		if (recordDelimiter.length() == 0) {
			throw new IllegalArgumentException("Record delimiter must not be empty");
		}
		if ((recordDelimiter.compareTo("\n") != 0) &&
			(recordDelimiter.compareTo("\r\n") != 0) &&
			(recordDelimiter.compareTo("\r") != 0)){
			throw new IllegalArgumentException("Record delimiter " + recordDelimiter + " is not supported yet, " +
				"Only \"\\r\", \"\\r\\n\", \"\\n\" are supported as record delimiter.");
		}

		this.recordDelimiterString = recordDelimiter;
		this.recordDelimiter = recordDelimiter.getBytes();
	}

	/**
	 * Get field delimiter used for parsing.
	 * @return field delimiter as a char
	 */
	public char getFieldDelimiter() {
		return this.fieldDelimiter;
	}

	/**
	 * Set field delimiter used for parsing.
	 * @param delimiter field delimiter
	 */
	public void setFieldDelimiter(char delimiter) {
		if (String.valueOf(delimiter) == null) {
			throw new IllegalArgumentException("Field delimiter must not be null");
		}
		if (String.valueOf(delimiter).length() == 0) {
			throw new IllegalArgumentException("Field delimiter must not be empty");
		}
		if (String.valueOf(delimiter).length() != 1) {
			throw new IllegalArgumentException("Filed delimiter must be a single character");
		}
		this.fieldDelimiter = delimiter;
	}

	/**
	 * Set field delimiter used for parsing.
	 * @param delimiter field delimiter
	 */
	public void setFieldDelimiter(String delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Field delimiter must not be null");
		}
		if (delimiter.length() == 0) {
			throw new IllegalArgumentException("Field delimiter must not be empty");
		}
		if (delimiter.length() != 1) {
			throw new IllegalArgumentException("Filed delimiter must be a single character");
		}
		this.fieldDelimiter = delimiter.charAt(0);
	}

	/**
	 * Skip the first line of csv file. Normally to skip header.
	 * @param skipFirstLine true to skip the first line or header
	 */
	public void enableSkipFirstLine(boolean skipFirstLine) {
		this.skipFirstLineAsHeader = skipFirstLine;
	}

	/**
	 * Enable parsing for quoted strings in a csv file.
	 * @param quoteCharacter Quote Character
	 */
	public void enableQuotedStringParsing(char quoteCharacter) {
		quotedStringParsing = true;
		this.quoteCharacter = quoteCharacter;
	}

	/**
	 * Check that Lenient parsing is enabled or not.
	 * @return return true if Lenient parsing is enabled, otherwise return false.
	 */
	private boolean isLenient() {
		return lenient;
	}

	/**
	 * Enable Lenient parsing. Ignore records with missing or wrond values.
	 * @param lenient set true to enable lenient parsing
	 */
	public void enableLenientParsing(boolean lenient) {
		this.lenient = lenient;
	}

	/**
	 * Skip the comment lines.
	 * Lines starting with a prefix '#'.
	 * @param skipComment set true to skip lines starting with '#'
	 */
	public void enableSkipComment(boolean skipComment) {
		this.allowComments = skipComment;
	}

}
