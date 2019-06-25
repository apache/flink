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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * A [[InputFormatTableSource]] and [[LookupableTableSource]] for simple CSV files with a
 * (logically) unlimited number of fields.
 */
public class CsvTableSource extends InputFormatTableSource<Row> implements
	LookupableTableSource<Row>, ProjectableTableSource<Row>, DefinedIndexes, BatchTableSource<Row> {

	private final CsvInputFormatConfig config;

	private CsvTableSource(
		String path,
		String[] fieldNames,
		TypeInformation<?>[] fieldTypes,
		String fieldDelim,
		String rowDelim,
		Character quoteCharacter,
		boolean ignoreFirstLine,
		String ignoreComments,
		boolean lenient) {

		this(path, fieldNames, fieldTypes,
			IntStream.range(0, fieldNames.length).toArray(),
			fieldDelim, rowDelim,
			quoteCharacter, ignoreFirstLine, ignoreComments, lenient);
	}

	private CsvTableSource(
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
	 * Return a new builder that builds a [[CsvTableSource]].
	 * For example:
	 * {{{
	 *   val source: CsvTableSource = CsvTableSource
	 *     .builder()
	 *     .path("/path/to/your/file.csv")
	 *     .field("myfield", Types.STRING)
	 *     .field("myfield2", Types.INT)
	 *     .build()
	 * }}}
	 * @return a new builder to build a [[CsvTableSource]]
	 */
	public static Builder builder() {
		return new Builder();
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(config.getSelectedFieldTypes(), config.getSelectedFieldNames());
	}

	@Override
	public InputFormat<Row, ?> getInputFormat() {
		return config.createInputFormat();
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		return new CsvLookupFunction(config, lookupKeys);
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("CSV do not support async lookup");
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}

	@Override
	public TableSchema getTableSchema() {
		return new TableSchema(config.fieldNames, config.fieldTypes);
	}

	@Override
	// CsvLookupTableSource can use any field as an index field since the whole file is
	// loaded and can be rearranged by any required key.
	public Collection<TableIndex> getIndexes() {
		List<TableIndex> idxes = new ArrayList<>();
		for (String f : config.getSelectedFieldNames()) {
			idxes.add(TableIndex.builder().normalIndex().name(f).indexedColumns(f).build());
		}
		return idxes;
	}

	@Override
	public CsvTableSource projectFields(int[] fields) {
		return new CsvTableSource(config.select(fields));
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(config.createInputFormat(),
			new RowTypeInfo(config.getSelectedFieldTypes(), config.getSelectedFieldNames()))
			.name(explainSource());
	}

	@Override
	public String explainSource() {
		String[] fields = config.getSelectedFieldNames();
		StringBuilder builder = new StringBuilder();
		builder.append("CsvTableSource(read fields: ");
		boolean first = true;
		for (String f : fields) {
			if (!first) {
				builder.append(", ");
			}
			builder.append(f);
			first = false;
		}
		builder.append(")");
		return builder.toString();
	}

	/**
	 * A builder for creating [[CsvTableSource]] instances.
	 * For example:
	 * {{{
	 *   val source: CsvTableSource = new CsvTableSource.builder()
	 *     .path("/path/to/your/file.csv")
	 *     .field("myfield", Types.STRING)
	 *     .field("myfield2", Types.INT)
	 *     .build()
	 * }}}
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
		 * Apply the current values and constructs a newly-created [[CsvTableSource]].
		 *
		 * @return a newly-created [[CsvTableSource]].
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

	/**
	 * LookupFunction to support lookup in [[CsvTableSource]].
	 */
	public static class CsvLookupFunction extends TableFunction<Row> {
		private final CsvInputFormatConfig config;

		private final List<Integer> sourceKeys = new ArrayList<>();
		private final List<Integer> targetKeys = new ArrayList<>();
		private final Map<Object, List<Row>> one2manyDataMap = new HashMap<>();

		CsvLookupFunction(CsvInputFormatConfig config, String[] lookupKeys) {
			this.config = config;

			List<String> fields = Arrays.asList(config.getSelectedFieldNames());
			for (int i = 0; i < lookupKeys.length; i++) {
				sourceKeys.add(i);
				int targetIdx = fields.indexOf(lookupKeys[i]);
				assert targetIdx != -1;
				targetKeys.add(targetIdx);
			}
		}

		@Override
		public TypeInformation<Row> getResultType() {
			return new RowTypeInfo(config.getSelectedFieldTypes(), config.getSelectedFieldNames());
		}

		@Override
		public void open(FunctionContext context) throws Exception {
			super.open(context);
			TypeInformation<Row> rowType = getResultType();
			TypeSerializer<Row> rowSerializer = rowType.createSerializer(new ExecutionConfig());

			RowCsvInputFormat inputFormat = config.createInputFormat();
			FileInputSplit[] inputSplits = inputFormat.createInputSplits(1);
			for (FileInputSplit split : inputSplits) {
				inputFormat.open(split);
				Row row = new Row(rowType.getArity());
				while (true) {
					Row r = inputFormat.nextRecord(row);
					if (r == null) {
						break;
					} else {
						Object key = getTargetKey(r);
						if (one2manyDataMap.containsKey(key)) {
							one2manyDataMap.get(key).add(rowSerializer.copy(r));
						} else {
							List<Row> rows = new ArrayList<>();
							rows.add(rowSerializer.copy(r));
							one2manyDataMap.put(key, rows);
						}
					}
				}
				inputFormat.close();
			}
		}

		public void eval(Object... values) {
			Object srcKey = getSourceKey(Row.of(values));
			if (one2manyDataMap.containsKey(srcKey)) {
				for (Row row1 : one2manyDataMap.get(srcKey)) {
					collect(row1);
				}
			}
		}

		private Object getSourceKey(Row source) {
			return getKey(source, sourceKeys);
		}

		private Object getTargetKey(Row target) {
			return getKey(target, targetKeys);
		}

		private Object getKey(Row input, List<Integer> keys) {
			if (keys.size() == 1) {
				int keyIdx = keys.get(0);
				if (input.getField(keyIdx) != null) {
					return input.getField(keyIdx);
				}
				return null;
			} else {
				Row key = new Row(keys.size());
				for (int i = 0; i < keys.size(); i++) {
					int keyIdx = keys.get(i);
					Object field = null;
					if (input.getField(keyIdx) != null) {
						field = input.getField(keyIdx);
					}
					if (field == null) {
						return null;
					}
					key.setField(i, field);
				}
				return key;
			}
		}

		@Override
		public void close() throws Exception {
			super.close();
		}
	}

	private static class CsvInputFormatConfig implements Serializable {
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
				fieldTypes,
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
	}
}
