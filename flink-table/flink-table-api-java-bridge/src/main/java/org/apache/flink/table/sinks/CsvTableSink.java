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

package org.apache.flink.table.sinks;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.LinkedHashMap;

/**
 * A simple {@link TableSink} to emit data as CSV files.
 */
public class CsvTableSink implements BatchTableSink<Row>, AppendStreamTableSink<Row> {
	private String path;
	private String fieldDelim;
	private int numFiles = -1;
	private FileSystem.WriteMode writeMode;

	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;

	/**
	 * A simple {@link TableSink} to emit data as CSV files.
	 *
	 * @param path       The output path to write the Table to.
	 * @param fieldDelim The field delimiter
	 * @param numFiles   The number of files to write to
	 * @param writeMode  The write mode to specify whether existing files are overwritten or not.
	 */
	public CsvTableSink(
		String path,
		String fieldDelim,
		int numFiles,
		FileSystem.WriteMode writeMode) {
		this.path = path;
		this.fieldDelim = fieldDelim;
		this.numFiles = numFiles;
		this.writeMode = writeMode;
	}

	/**
	 * A simple {@link TableSink} to emit data as CSV files using comma as field delimiter, with default
	 * parallelism and write mode.
	 *
	 * @param path The output path to write the Table to.
	 */
	public CsvTableSink(String path) {
		this(path, ",");
	}

	/**
	 * A simple {@link TableSink} to emit data as CSV files, with default parallelism and write mode.
	 *
	 * @param path       The output path to write the Table to.
	 * @param fieldDelim The field delimiter
	 */
	public CsvTableSink(String path, String fieldDelim) {
		this(path, fieldDelim, -1, null);
	}

	@Override
	public void emitDataSet(DataSet<Row> dataSet) {
		MapOperator<Row, String> csvRows =
			dataSet.map(new CsvFormatter(fieldDelim == null ? "," : fieldDelim));

		DataSink<String> sink;
		if (writeMode != null) {
			sink = csvRows.writeAsText(path, writeMode);
		} else {
			sink = csvRows.writeAsText(path);
		}

		if (numFiles > 0) {
			csvRows.setParallelism(numFiles);
			sink.setParallelism(numFiles);
		}

		sink.name(TableConnectorUtils.generateRuntimeName(CsvTableSink.class, fieldNames));
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		SingleOutputStreamOperator<String> csvRows =
			dataStream.map(new CsvFormatter(fieldDelim == null ? "," : fieldDelim));

		DataStreamSink<String> sink;
		if (writeMode != null) {
			sink = csvRows.writeAsText(path, writeMode);
		} else {
			sink = csvRows.writeAsText(path);
		}

		if (numFiles > 0) {
			csvRows.setParallelism(numFiles);
			sink.setParallelism(numFiles);
		}

		sink.name(TableConnectorUtils.generateRuntimeName(CsvTableSink.class, fieldNames));

		return sink;
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		CsvTableSink configuredSink = new CsvTableSink(path, fieldDelim, numFiles, writeMode);
		configuredSink.fieldNames = fieldNames;
		configuredSink.fieldTypes = fieldTypes;
		return configuredSink;
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(getFieldTypes(), getFieldNames());
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	/**
	 * Return a new builder that builds a CsvTableSink. For example:
	 * <pre>
	 * CsvTableSink sink = new CsvTableSink.builder()
	 *     .path("/path/to/..")
	 *     .field("myfield", Types.STRING)
	 *     .field("myfield2", Types.INT)
	 *     .build();
	 * </pre>
	 *
	 * @return a new builder to build a CsvTableSink
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * A builder for creating CsvTableSink instances.
	 */
	public static class Builder {
		private LinkedHashMap<String, TypeInformation<?>> schema = new LinkedHashMap<>();
		private String path;
		private String fieldDelim;
		private int numFiles = -1; ;
		private FileSystem.WriteMode writeMode;

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
		 * Sets the number of files to write to.
		 *
		 * @param numFiles the number of files to write to
		 */
		public Builder numFiles(int numFiles) {
			this.numFiles = numFiles;
			return this;
		}

		/**
		 * Sets the write mode to specify whether existing files are overwritten or not.
		 *
		 * @param writeMode the write mode to specify whether existing files are overwritten or not
		 */
		public Builder writeMode(FileSystem.WriteMode writeMode) {
			this.writeMode = writeMode;
			return this;
		}

		/**
		 * Adds a field with the field name and the type information. This method can be
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
		 * Apply the current values and constructs a newly-created CsvTableSink.
		 *
		 * @return a newly-created CsvTableSink
		 */
		public CsvTableSink build() {
			if (path == null) {
				throw new IllegalArgumentException("Path must be defined.");
			}

			CsvTableSink csvTableSink = new CsvTableSink(
				path,
				fieldDelim,
				numFiles,
				writeMode);

			if(schema.size() != 0){
				csvTableSink.fieldNames = schema.keySet().toArray(new String[0]);
				csvTableSink.fieldTypes	= schema.values().toArray(new TypeInformation<?>[0]);
			}
			return csvTableSink;
		}
	}

	/**
	 * Formats a Row into a String with fields separated by the field delimiter.
	 */
	public static class CsvFormatter implements MapFunction<Row, String> {
		private static final long serialVersionUID = 1L;

		private final String fieldDelim;

		/**
		 * Constructor with field delimiter.
		 *
		 * @param fieldDelim The field delimiter.
		 */
		CsvFormatter(String fieldDelim) {
			this.fieldDelim = fieldDelim;
		}

		@Override
		public String map(Row row) {
			StringBuilder builder = new StringBuilder();
			Object o;
			for (int i = 0; i < row.getArity(); i++) {
				if (builder.length() != 0) {
					builder.append(fieldDelim);
				}
				if ((o = row.getField(i)) != null) {
					builder.append(o);
				}
			}
			return builder.toString();
		}
	}
}
