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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * A simple {@link TableSink} to emit data as CSV files.
 */
public class CsvTableSink implements BatchTableSink<Row>, AppendStreamTableSink<Row> {
	private String path;
	private String fieldDelim;
	private int numFiles = -1;
	private FileSystem.WriteMode writeMode;

	private String[] fieldNames;
	private DataType[] fieldTypes;

	/**
	 * A simple {@link TableSink} to emit data as CSV files.
	 *
	 * @param path       The output path to write the Table to.
	 * @param fieldDelim The field delimiter
	 * @param numFiles   The number of files to write to
	 * @param writeMode  The write mode to specify whether existing files are overwritten or not.
	 * @param fieldNames The field names of the table to emit.
	 * @param fieldTypes The field types of the table to emit.
	 */
	public CsvTableSink(
			String path,
			String fieldDelim,
			int numFiles,
			FileSystem.WriteMode writeMode,
			String[] fieldNames,
			DataType[] fieldTypes) {
		this.path = path;
		this.fieldDelim = fieldDelim;
		this.numFiles = numFiles;
		this.writeMode = writeMode;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
	}

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
		} else {
			// if file number is not set, use input parallelism to make it chained.
			csvRows.setParallelism(dataStream.getParallelism());
			sink.setParallelism(dataStream.getParallelism());
		}

		sink.name(TableConnectorUtils.generateRuntimeName(CsvTableSink.class, fieldNames));

		return sink;
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (this.fieldNames != null || this.fieldTypes != null) {
			throw new IllegalStateException(
				"CsvTableSink has already been configured field names and field types.");
		}
		DataType[] dataTypes = Arrays.stream(fieldTypes)
			.map(TypeConversions::fromLegacyInfoToDataType)
			.toArray(DataType[]::new);
		return new CsvTableSink(path, fieldDelim, numFiles, writeMode, fieldNames, dataTypes);
	}

	@Override
	public DataType getConsumedDataType() {
		return getTableSchema().toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchema.builder().fields(fieldNames, fieldTypes).build();
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
				if (i > 0) {
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
