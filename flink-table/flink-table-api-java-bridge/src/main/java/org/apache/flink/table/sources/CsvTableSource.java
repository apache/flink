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

package org.apache.flink.table.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * A {@link StreamTableSource} and {@link BatchTableSource} for simple CSV files with a
 * (logically) unlimited number of fields.
 */
public class CsvTableSource
	implements StreamTableSource<Row>, BatchTableSource<Row>, ProjectableTableSource<Row> {

	private final CsvInputFormatConfig config;

	/**
	 * A {@link InputFormatTableSource} and {@link LookupableTableSource} for simple CSV files with
	 * a (logically) unlimited number of fields.
	 *
	 * @param path       The path to the CSV file.
	 * @param fieldNames The names of the table fields.
	 * @param fieldTypes The types of the table fields.
	 */
	public CsvTableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this(path, fieldNames, fieldTypes,
			IntStream.range(0, fieldNames.length).toArray(),
			CsvInputFormat.DEFAULT_FIELD_DELIMITER, CsvInputFormat.DEFAULT_LINE_DELIMITER,
			null, false, null, false);
	}

	/**
	 * A {@link InputFormatTableSource} and {@link LookupableTableSource} for simple CSV files with
	 * a (logically) unlimited number of fields.
	 *
	 * @param path            The path to the CSV file.
	 * @param fieldNames      The names of the table fields.
	 * @param fieldTypes      The types of the table fields.
	 * @param fieldDelim      The field delimiter, "," by default.
	 * @param lineDelim       The row delimiter, "\n" by default.
	 * @param quoteCharacter  An optional quote character for String values, null by default.
	 * @param ignoreFirstLine Flag to ignore the first line, false by default.
	 * @param ignoreComments  An optional prefix to indicate comments, null by default.
	 * @param lenient         Flag to skip records with parse error instead to fail, false by
	 *                        default.
	 */
	public CsvTableSource(
		String path,
		String[] fieldNames,
		TypeInformation<?>[] fieldTypes,
		String fieldDelim,
		String lineDelim,
		Character quoteCharacter,
		boolean ignoreFirstLine,
		String ignoreComments,
		boolean lenient) {

		this(path, fieldNames, fieldTypes,
			IntStream.range(0, fieldNames.length).toArray(),
			fieldDelim, lineDelim,
			quoteCharacter, ignoreFirstLine, ignoreComments, lenient);
	}

	/**
	 * A {@link InputFormatTableSource} and {@link LookupableTableSource} for simple CSV files with
	 * a (logically) unlimited number of fields.
	 *
	 * @param path            The path to the CSV file.
	 * @param fieldNames      The names of the table fields.
	 * @param fieldTypes      The types of the table fields.
	 * @param selectedFields  The fields which will be read and returned by the table source. If
	 *                        None, all fields are returned.
	 * @param fieldDelim      The field delimiter, "," by default.
	 * @param lineDelim       The row delimiter, "\n" by default.
	 * @param quoteCharacter  An optional quote character for String values, null by default.
	 * @param ignoreFirstLine Flag to ignore the first line, false by default.
	 * @param ignoreComments  An optional prefix to indicate comments, null by default.
	 * @param lenient         Flag to skip records with parse error instead to fail, false by
	 *                        default.
	 */
	public CsvTableSource(
		String path,
		String[] fieldNames,
		TypeInformation<?>[] fieldTypes,
		int[] selectedFields,
		String fieldDelim,
		String lineDelim,
		Character quoteCharacter,
		boolean ignoreFirstLine,
		String ignoreComments,
		boolean lenient) {
		this(new CsvInputFormatConfig(path, fieldNames, fieldTypes, selectedFields,
			fieldDelim, lineDelim, quoteCharacter, ignoreFirstLine, ignoreComments, lenient));
	}

	private CsvTableSource(CsvInputFormatConfig config) {
		this.config = config;
	}

	/**
	 * Return a new builder that builds a CsvTableSource. For example:
	 * <pre>
	 * CsvTableSource source = new CsvTableSource.builder()
	 *     .path("/path/to/your/file.csv")
	 *     .field("myfield", Types.STRING)
	 *     .field("myfield2", Types.INT)
	 *     .build();
	 * </pre>
	 *
	 * @return a new builder to build a CsvTableSource
	 */
	public static Builder builder() {
		return new Builder();
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(config.getSelectedFieldTypes(), config.getSelectedFieldNames());
	}

	@Override
	public TableSchema getTableSchema() {
		return new TableSchema(config.fieldNames, config.fieldTypes);
	}

	@Override
	public CsvTableSource projectFields(int[] fields) {
		if (fields.length == 0) {
			fields = new int[]{0};
		}
		return new CsvTableSource(config.select(fields));
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.createInput(config.createInputFormat(), getReturnType()).name(explainSource());
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(config.createInputFormat(), getReturnType()).name(explainSource());
	}

	@Override
	public String explainSource() {
		String[] fields = config.getSelectedFieldNames();
		return "CsvTableSource(read fields: " + String.join(", ", fields) + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CsvTableSource that = (CsvTableSource) o;
		return Objects.equals(config, that.config);
	}

	@Override
	public int hashCode() {
		return Objects.hash(config);
	}

	/**
	 * A builder for creating CsvTableSource instances.
	 */
	public static class Builder {
		private LinkedHashMap<String, TypeInformation<?>> schema = new LinkedHashMap<>();
		private Character quoteCharacter;
		private String path;
		private String fieldDelim = CsvInputFormat.DEFAULT_FIELD_DELIMITER;
		private String lineDelim = CsvInputFormat.DEFAULT_LINE_DELIMITER;
		private boolean isIgnoreFirstLine = false;
		private String commentPrefix;
		private boolean lenient = false;

		/**
		 * Sets the path to the CSV file. Required.
		 *
		 * @param path the path to the CSV file
		 */
		public Builder path(String path) {
			this.path = path;
			return this;
		}

		/**
		 * Sets the field delimiter, "," by default.
		 *
		 * @param delim the field delimiter
		 */
		public Builder fieldDelimiter(String delim) {
			this.fieldDelim = delim;
			return this;
		}

		/**
		 * Sets the line delimiter, "\n" by default.
		 *
		 * @param delim the line delimiter
		 */
		public Builder lineDelimiter(String delim) {
			this.lineDelim = delim;
			return this;
		}

		/**
		 * Adds a field with the field name and the type information. Required. This method can be
		 * called multiple times. The call order of this method defines also the order of the fields
		 * in a row.
		 *
		 * @param fieldName the field name
		 * @param fieldType the type information of the field
		 */
		public Builder field(String fieldName, TypeInformation<?> fieldType) {
			if (schema.containsKey(fieldName)) {
				throw new IllegalArgumentException("Duplicate field name " + fieldName);
			}
			schema.put(fieldName, fieldType);
			return this;
		}

		/**
		 * Sets a quote character for String values, null by default.
		 *
		 * @param quote the quote character
		 */
		public Builder quoteCharacter(Character quote) {
			this.quoteCharacter = quote;
			return this;
		}

		/**
		 * Sets a prefix to indicate comments, null by default.
		 *
		 * @param prefix the prefix to indicate comments
		 */
		public Builder commentPrefix(String prefix) {
			this.commentPrefix = prefix;
			return this;
		}

		/**
		 * Ignore the first line. Not skip the first line by default.
		 */
		public Builder ignoreFirstLine() {
			this.isIgnoreFirstLine = true;
			return this;
		}

		/**
		 * Skip records with parse error instead to fail. Throw an exception by default.
		 */
		public Builder ignoreParseErrors() {
			this.lenient = true;
			return this;
		}

		/**
		 * Apply the current values and constructs a newly-created CsvTableSource.
		 *
		 * @return a newly-created CsvTableSource
		 */
		public CsvTableSource build() {
			if (path == null) {
				throw new IllegalArgumentException("Path must be defined.");
			}
			if (schema.isEmpty()) {
				throw new IllegalArgumentException("Fields can not be empty.");
			}
			return new CsvTableSource(
				path,
				schema.keySet().toArray(new String[0]),
				schema.values().toArray(new TypeInformation<?>[0]),
				fieldDelim,
				lineDelim,
				quoteCharacter,
				isIgnoreFirstLine,
				commentPrefix,
				lenient);
		}

	}

	private static class CsvInputFormatConfig implements Serializable {
		private static final long serialVersionUID = 1L;

		private final String path;
		private final String[] fieldNames;
		private final TypeInformation<?>[] fieldTypes;
		private final int[] selectedFields;

		private final String fieldDelim;
		private final String lineDelim;
		private final Character quoteCharacter;
		private final boolean ignoreFirstLine;
		private final String ignoreComments;
		private final boolean lenient;

		CsvInputFormatConfig(
			String path,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes,
			int[] selectedFields,
			String fieldDelim,
			String lineDelim,
			Character quoteCharacter,
			boolean ignoreFirstLine,
			String ignoreComments,
			boolean lenient) {

			this.path = path;
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
			this.selectedFields = selectedFields;
			this.fieldDelim = fieldDelim;
			this.lineDelim = lineDelim;
			this.quoteCharacter = quoteCharacter;
			this.ignoreFirstLine = ignoreFirstLine;
			this.ignoreComments = ignoreComments;
			this.lenient = lenient;
		}

		String[] getSelectedFieldNames() {
			String[] selectedFieldNames = new String[selectedFields.length];
			for (int i = 0; i < selectedFields.length; i++) {
				selectedFieldNames[i] = fieldNames[selectedFields[i]];
			}
			return selectedFieldNames;
		}

		TypeInformation<?>[] getSelectedFieldTypes() {
			TypeInformation<?>[] selectedFieldTypes = new TypeInformation<?>[selectedFields.length];
			for (int i = 0; i < selectedFields.length; i++) {
				selectedFieldTypes[i] = fieldTypes[selectedFields[i]];
			}
			return selectedFieldTypes;
		}

		RowCsvInputFormat createInputFormat() {
			RowCsvInputFormat inputFormat = new RowCsvInputFormat(
				new Path(path),
				getSelectedFieldTypes(),
				lineDelim,
				fieldDelim,
				selectedFields);
			inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine);
			inputFormat.setCommentPrefix(ignoreComments);
			inputFormat.setLenient(lenient);
			if (quoteCharacter != null) {
				inputFormat.enableQuotedStringParsing(quoteCharacter);
			}
			return inputFormat;
		}

		CsvInputFormatConfig select(int[] fields) {
			return new CsvInputFormatConfig(path, fieldNames, fieldTypes, fields,
				fieldDelim, lineDelim, quoteCharacter, ignoreFirstLine, ignoreComments, lenient);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CsvInputFormatConfig that = (CsvInputFormatConfig) o;
			return ignoreFirstLine == that.ignoreFirstLine &&
				lenient == that.lenient &&
				Objects.equals(path, that.path) &&
				Arrays.equals(fieldNames, that.fieldNames) &&
				Arrays.equals(fieldTypes, that.fieldTypes) &&
				Arrays.equals(selectedFields, that.selectedFields) &&
				Objects.equals(fieldDelim, that.fieldDelim) &&
				Objects.equals(lineDelim, that.lineDelim) &&
				Objects.equals(quoteCharacter, that.quoteCharacter) &&
				Objects.equals(ignoreComments, that.ignoreComments);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(path, fieldDelim, lineDelim, quoteCharacter, ignoreFirstLine,
				ignoreComments, lenient);
			result = 31 * result + Arrays.hashCode(fieldNames);
			result = 31 * result + Arrays.hashCode(fieldTypes);
			result = 31 * result + Arrays.hashCode(selectedFields);
			return result;
		}
	}
}
