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

package org.apache.flink.table.planner.utils;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.api.java.io.CsvOutputFormat.DEFAULT_FIELD_DELIMITER;
import static org.apache.flink.api.java.io.CsvOutputFormat.DEFAULT_LINE_DELIMITER;

/**
 * Test csv {@link FileSystemFormatFactory}.
 */
public class TestCsvFileSystemFormatFactory implements FileSystemFormatFactory {

	public static final ConfigOption<Boolean> USE_BULK_WRITER = ConfigOptions.key("use-bulk-writer")
			.booleanType()
			.defaultValue(false);

	@Override
	public String factoryIdentifier() {
		return "testcsv";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(USE_BULK_WRITER);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return new HashSet<>();
	}

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		return new TestRowDataCsvInputFormat(
				context.getPaths(),
				context.getSchema(),
				context.getPartitionKeys(),
				context.getDefaultPartName(),
				context.getProjectFields(),
				context.getPushedDownLimit());
	}

	private boolean useBulkWriter(WriterContext context) {
		return context.getFormatOptions().get(USE_BULK_WRITER);
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
		if (useBulkWriter(context)) {
			return Optional.empty();
		}

		DataType[] types = context.getFormatFieldTypes();
		return Optional.of((rowData, stream) -> {
			writeCsvToStream(types, rowData, stream);
		});
	}

	private static void writeCsvToStream(
			DataType[] types,
			RowData rowData,
			OutputStream stream) throws IOException {
		LogicalType[] fieldTypes = Arrays.stream(types)
				.map(DataType::getLogicalType)
				.toArray(LogicalType[]::new);
		DataFormatConverters.DataFormatConverter converter = DataFormatConverters.getConverterForDataType(
				TypeConversions.fromLogicalToDataType(RowType.of(fieldTypes)));

		Row row = (Row) converter.toExternal(rowData);
		StringBuilder builder = new StringBuilder();
		Object o;
		for (int i = 0; i < row.getArity(); i++) {
			if (i > 0) {
				builder.append(DEFAULT_FIELD_DELIMITER);
			}
			if ((o = row.getField(i)) != null) {
				builder.append(o);
			}
		}
		String str = builder.toString();
		stream.write(str.getBytes(StandardCharsets.UTF_8));
		stream.write(DEFAULT_LINE_DELIMITER.getBytes(StandardCharsets.UTF_8));
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		if (!useBulkWriter(context)) {
			return Optional.empty();
		}

		DataType[] types = context.getFormatFieldTypes();
		return Optional.of(out -> new CsvBulkWriter(types, out));
	}

	private static class CsvBulkWriter implements BulkWriter<RowData> {

		private final DataType[] types;
		private final OutputStream stream;

		private CsvBulkWriter(DataType[] types, OutputStream stream) {
			this.types = types;
			this.stream = stream;
		}

		@Override
		public void addElement(RowData element) throws IOException {
			writeCsvToStream(types, element, stream);
		}

		@Override
		public void flush() {
		}

		@Override
		public void finish() {
		}
	}
}
