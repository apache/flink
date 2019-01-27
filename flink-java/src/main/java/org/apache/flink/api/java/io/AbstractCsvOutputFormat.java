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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeConvertUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.TimeZone;

/**
 * Base class of CsvOutputFormat, it serializes records to text. The output is
 * structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common). Field
 * delimiters separate fields within a record.
 */
@PublicEvolving
public abstract class AbstractCsvOutputFormat<T> extends FileOutputFormat<T> {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(AbstractCsvOutputFormat.class);

	// --------------------------------------------------------------------------------------------

	public static final String DEFAULT_LINE_DELIMITER = CsvInputFormat.DEFAULT_LINE_DELIMITER;

	public static final String DEFAULT_FIELD_DELIMITER = String.valueOf(CsvInputFormat.DEFAULT_FIELD_DELIMITER);

	// --------------------------------------------------------------------------------------------

	private transient Writer wrt;

	private String recordDelimiter;

	private String fieldDelimiter;

	private String quoteCharacter;

	private String doubleQuotes;

	private String charsetName;

	private boolean allowNullValues;

	private boolean quoteStrings = false;

	private TimeZone timezone = TimeZone.getTimeZone("UTC");

	private boolean outputFieldName = false;

	private String[] fieldNames = null;

	// --------------------------------------------------------------------------------------------
	// Constructors and getters/setters for the configurable parameters
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates an instance of AbstractCsvOutputFormat. Lines are separated by the newline character
	 * '\n', fields are separated by ',', no escape character.
	 *
	 * @param outputPath The path where the CSV file is written.
	 */
	public AbstractCsvOutputFormat(Path outputPath) {
		this(outputPath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER);
	}

	/**
	 * Creates an instance of AbstractCsvOutputFormat. Lines are separated by the newline character
	 * '\n', fields by the given field delimiter, no escape character.
	 *
	 * @param outputPath     The path where the CSV file is written.
	 * @param fieldDelimiter The delimiter that is used to separate fields in a tuple.
	 */
	public AbstractCsvOutputFormat(Path outputPath, String fieldDelimiter) {
		this(outputPath, DEFAULT_LINE_DELIMITER, fieldDelimiter);
	}

	/**
	 * Creates an instance of AbstractCsvOutputFormat. Lines are separated by the given record
	 * delimiter, fields by the given field delimiter, no escape character.
	 *
	 * @param outputPath      The path where the CSV file is written.
	 * @param recordDelimiter The delimiter that is used to separate the record.
	 * @param fieldDelimiter  The delimiter that is used to separate fields in a record.
	 */
	public AbstractCsvOutputFormat(Path outputPath, String recordDelimiter, String fieldDelimiter) {
		this(outputPath, recordDelimiter, fieldDelimiter, null);
	}

	/**
	 * Creates an instance of AbstractCsvOutputFormat.
	 *
	 * @param outputPath      The path where the CSV file is written.
	 * @param recordDelimiter The delimiter that is used to separate the record.
	 * @param fieldDelimiter  The delimiter that is used to separate fields in a record.
	 * @param quoteCharacter  The quote character that is used to escape other characters.
	 */
	public AbstractCsvOutputFormat(
		Path outputPath, String recordDelimiter, String fieldDelimiter, String quoteCharacter) {
		super(outputPath);
		setRecordDelimiter(recordDelimiter);
		setFieldDelimiter(fieldDelimiter);
		setQuoteCharacter(quoteCharacter);
		this.allowNullValues = false;
	}

	/**
	 * Creates an instance of AbstractCsvOutputFormat.
	 *
	 * @param outputPath      The path where the CSV file is written.
	 * @param recordDelimiter The delimiter that is used to separate the record.
	 * @param fieldDelimiter  The delimiter that is used to separate fields in a record.
	 * @param quoteCharacter  The quote character that is used to escape other characters.
	 */
	public AbstractCsvOutputFormat(
		Path outputPath, String recordDelimiter, String fieldDelimiter, char quoteCharacter) {
		super(outputPath);
		setRecordDelimiter(recordDelimiter);
		setFieldDelimiter(fieldDelimiter);
		setQuoteCharacter(quoteCharacter);
		this.allowNullValues = false;
	}

	public void setRecordDelimiter(String recordDelimiter) {
		Preconditions.checkArgument(recordDelimiter != null, "RecordDelmiter shall not be null.");
		this.recordDelimiter = recordDelimiter;
	}

	public void setFieldDelimiter(String fieldDelimiter) {
		Preconditions.checkArgument(fieldDelimiter != null, "FieldDelimiter shall not be null.");
		this.fieldDelimiter = fieldDelimiter;
	}

	public void setQuoteCharacter(String character) {
		Preconditions.checkArgument(
			character == null || character.length() == 1, "character should be a single character string");
		if (character == null) {
			this.quoteCharacter = null;
			this.doubleQuotes = null;
		} else {
			this.quoteCharacter = character;
			this.doubleQuotes = this.quoteCharacter + this.quoteCharacter;
		}
	}

	public void setQuoteCharacter(char character) {
		this.quoteCharacter = Character.toString(character);
		this.doubleQuotes = this.quoteCharacter + this.quoteCharacter;
	}

	/**
	 * Configures the format to either allow null values (writing an empty field),
	 * or to throw an exception when encountering a null field.
	 *
	 * <p>by default, null values are disallowed.
	 *
	 * @param allowNulls Flag to indicate whether the output format should accept null values.
	 */
	public void setAllowNullValues(boolean allowNulls) {
		this.allowNullValues = allowNulls;
	}

	/**
	 * Sets the charset with which the CSV strings are written to the file.
	 * If not specified, the output format uses the systems default character encoding.
	 *
	 * @param charsetName The name of charset to use for encoding the output.
	 */
	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}

	/**
	 * Configures whether the output format should quote string values. String values are fields
	 * of type {@link String} and {@link StringValue}, as well as
	 * all subclasses of the latter.
	 *
	 * <p>By default, strings are not quoted.
	 *
	 * @param quoteStrings Flag indicating whether string fields should be quoted.
	 */
	public void setQuoteStrings(boolean quoteStrings) {
		this.quoteStrings = quoteStrings;
	}

	public TimeZone getTimezone() {
		return timezone;
	}

	public void setTimezone(TimeZone timezone) {
		this.timezone = timezone;
	}

	public void setOutputFieldName(boolean outputFieldName) {
		this.outputFieldName = outputFieldName;
	}

	public void setFieldNames(String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		this.wrt = this.charsetName == null ? new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096)) :
			new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);
	}

	@Override
	public void close() throws IOException {
		if (wrt != null) {
			this.wrt.flush();
			this.wrt.close();
			// Make sure we set the Writer to null after close. If the operators are
			// chained together and closes multiple times, it will throw an IOException.
			this.wrt = null;
		}
		super.close();
	}

	/**
	 * Get the specified field of the record.
	 *
	 * @param record The processing record.
	 * @param n       The field position.
	 * @return The specified field of the record.
	 */
	protected abstract Object getSpecificField(T record, int n);

	/**
	 * Get fields num of the record.
	 *
	 * @param record The processing record.
	 * @return Fields num of the record.
	 */
	protected abstract int getFieldsNum(T record);

	@Override
	public void writeRecord(T record) throws IOException {
		if (outputFieldName) {
			outputFieldNames();
		}
		outputRecord(record);
	}

	private void outputFieldNames() throws IOException {
		outputFieldName = false;
		if (null == fieldNames || fieldNames.length == 0) {
			throw new RuntimeException("Field names are empty");
		}
		for (int i = 0; i < fieldNames.length; i++) {
			if (i != 0) {
				this.wrt.write(this.fieldDelimiter);
			}
			this.wrt.write(getEscapedString(fieldNames[i]));
		}
		// add the record delimiter
		this.wrt.write(this.recordDelimiter);
	}

	private void outputRecord(T record) throws IOException {
		int numFields = getFieldsNum(record);

		for (int i = 0; i < numFields; i++) {
			Object v = getSpecificField(record, i);
			if (v != null) {
				if (i != 0) {
					this.wrt.write(this.fieldDelimiter);
				}

				if (quoteStrings && quoteCharacter != null &&
					(v instanceof String || v instanceof StringValue)) {
					String value = v.toString();
					this.wrt.write(getQuotedEscapedString(value));
				} else if (v instanceof Date) {
					String date = TimeConvertUtils.unixDateToString(TimeConvertUtils.toInt((Date) v));
					this.wrt.write(date);
				} else if (v instanceof Time) {
					String time = TimeConvertUtils.unixTimeToString(TimeConvertUtils.toInt((Time) v));
					this.wrt.write(time);
				} else if (v instanceof Timestamp) {
					long val = ((Timestamp) v).getTime();
					long offset = timezone.getOffset(val);
					String timestamp =
						TimeConvertUtils.unixTimestampToString(val + offset, 3);
					this.wrt.write(timestamp);
				} else if (v instanceof BigDecimal) {
					String value = ((BigDecimal) v).toPlainString();
					this.wrt.write(value);
				} else if (v instanceof byte[]) {
					String value = Arrays.toString((byte[]) v);
					this.wrt.write(getEscapedString(value));
				} else {
					String value = v.toString();
					this.wrt.write(getEscapedString(value));
				}
			} else {
				if (this.allowNullValues) {
					if (i != 0) {
						this.wrt.write(this.fieldDelimiter);
					}
				} else {
					throw new RuntimeException("Cannot write tuple with <null> value at position: " + i);
				}
			}
		}

		// add the record delimiter
		this.wrt.write(this.recordDelimiter);
	}

	private String getEscapedString(String value) {
		if (quoteCharacter == null) {
			return value;
		} else {
			boolean quote = value.contains(quoteCharacter);
			boolean hasDelim = value.contains(fieldDelimiter) || value.contains(recordDelimiter);
			if (quote || hasDelim) {
				return getQuotedEscapedString(value);
			} else {
				return value;
			}
		}
	}

	private String getQuotedEscapedString(String value) {
		Preconditions.checkArgument(quoteCharacter != null, "Quote character can't be null");
		value = value.replaceAll(quoteCharacter, doubleQuotes);
		return quoteCharacter + value + quoteCharacter;
	}

	// --------------------------------------------------------------------------------------------
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " (path: " + this.getOutputFilePath() + ", delimiter: " + this.fieldDelimiter + ")";
	}
}
