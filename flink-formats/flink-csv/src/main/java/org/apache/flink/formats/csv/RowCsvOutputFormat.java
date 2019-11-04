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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvRowSerializationSchema.RuntimeConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SequenceWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.formats.csv.CsvRowSerializationSchema.createRowRuntimeConverter;

/**
 * Row {@link OutputFormat} that serializes {@link Row}s to text. The output is structured by line
 * delimiters and field delimiters as common in CSV files. Line delimiter separate records from
 * each other ('\n' is common). Field delimiters separate fields within a record.
 *
 * <p>These can be continuously improved in this csv output format:
 * 1.Not support configure multi chars field delimiter.
 * 2.Only support configure line delimiter: "\r" or "\n" or "\r\n".
 */
public class RowCsvOutputFormat extends FileOutputFormat<Row> {

	/** Runtime instance that performs the actual work. */
	private final RuntimeConverter runtimeConverter;
	private final CsvSchema csvSchema;

	/** CsvMapper used to write {@link JsonNode} into bytes. */
	private transient CsvMapper csvMapper;

	/** Sequence writer used to write rows. It is configured by {@link CsvSchema}. */
	private transient SequenceWriter sequenceWriter;

	/** Reusable object node. */
	private transient ObjectNode root;

	/**
	 * @param typeInfo Type information describing the input CSV data.
	 * @param csvSchema Schema describing the input CSV data.
	 * @param path output path.
	 */
	public RowCsvOutputFormat(
			RowTypeInfo typeInfo,
			CsvSchema csvSchema,
			Path path) {
		super(path);
		this.runtimeConverter = createRowRuntimeConverter(typeInfo, true);
		this.csvSchema = csvSchema;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		this.csvMapper = new CsvMapper();
		this.root = this.csvMapper.createObjectNode();
		this.sequenceWriter = this.csvMapper.writer(this.csvSchema).writeValues(
				new BufferedOutputStream(this.stream, 4096));
	}

	@Override
	public void writeRecord(Row record) throws IOException {
		this.runtimeConverter.convert(csvMapper, root, record);
		this.sequenceWriter.write(root);
	}

	@Override
	public void close() throws IOException {
		if (this.sequenceWriter != null) {
			this.sequenceWriter.flush();
			this.sequenceWriter.close();
			this.sequenceWriter = null;
		}
		super.close();
	}

	@Override
	public String toString() {
		return "RowCsvOutputFormat{" +
				"outputFilePath=" + outputFilePath +
				", csvSchema=" + csvSchema +
				'}';
	}

	/**
	 * A builder for creating a {@link RowCsvOutputFormat}.
	 */
	@PublicEvolving
	public static class Builder implements Serializable {

		private final RowTypeInfo typeInfo;
		private final Path path;
		private CsvSchema csvSchema;

		/**
		 * Creates a {@link RowCsvOutputFormat} expecting the given {@link TypeInformation}
		 * and given {@link Path}.
		 *
		 * @param typeInfo type information used to create output format.
		 * @param path output path used to create output format.
		 */
		public Builder(TypeInformation<Row> typeInfo, Path path) {
			Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
			Preconditions.checkNotNull(path, "Path must not be null.");

			if (!(typeInfo instanceof RowTypeInfo)) {
				throw new IllegalArgumentException("Row type information expected.");
			}

			this.typeInfo = (RowTypeInfo) typeInfo;
			this.path = path;
			this.csvSchema = CsvRowSchemaConverter.convert((RowTypeInfo) typeInfo);
		}

		public Builder setFieldDelimiter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(c).build();
			return this;
		}

		public Builder setLineDelimiter(String delimiter) {
			Preconditions.checkNotNull(delimiter, "Delimiter must not be null.");
			if (!delimiter.equals("\n") && !delimiter.equals("\r") && !delimiter.equals("\r\n")) {
				throw new IllegalArgumentException(
						"Unsupported new line delimiter. Only \\n, \\r, or \\r\\n are supported.");
			}
			this.csvSchema = this.csvSchema.rebuild().setLineSeparator(delimiter).build();
			return this;
		}

		public Builder setArrayElementDelimiter(String delimiter) {
			Preconditions.checkNotNull(delimiter, "Delimiter must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
			return this;
		}

		public Builder setQuoteCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
			return this;
		}

		public Builder setEscapeCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
			return this;
		}

		public Builder setNullLiteral(String s) {
			this.csvSchema = this.csvSchema.rebuild().setNullValue(s).build();
			return this;
		}

		public RowCsvOutputFormat build() {
			return new RowCsvOutputFormat(
					typeInfo,
					csvSchema,
					path);
		}
	}
}
