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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.DataFormatConverters;
import org.apache.flink.table.filesystem.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.api.java.io.CsvOutputFormat.DEFAULT_FIELD_DELIMITER;
import static org.apache.flink.api.java.io.CsvOutputFormat.DEFAULT_LINE_DELIMITER;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;

/**
 * Test csv {@link FileSystemFormatFactory}.
 */
public class TestCsvFileSystemFormatFactory implements FileSystemFormatFactory {

	public static final String USE_BULK_WRITER = "format.use-bulk-writer";

	@Override
	public boolean supportsSchemaDerivation() {
		return true;
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(FORMAT, "testcsv");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return Collections.singletonList(USE_BULK_WRITER);
	}

	@Override
	public InputFormat<BaseRow, ?> createReader(ReaderContext context) {
		return new TestRowDataCsvInputFormat(
				context.getPaths(),
				context.getSchema(),
				context.getPartitionKeys(),
				context.getDefaultPartName(),
				context.getProjectFields(),
				context.getPushedDownLimit());
	}

	private boolean useBulkWriter(WriterContext context) {
		return Boolean.parseBoolean(context.getFormatProperties().get(USE_BULK_WRITER));
	}

	@Override
	public Optional<Encoder<BaseRow>> createEncoder(WriterContext context) {
		if (useBulkWriter(context)) {
			return Optional.empty();
		}

		DataType[] types = context.getFieldTypesWithoutPartKeys();
		return Optional.of((baseRow, stream) -> {
			writeCsvToStream(types, baseRow, stream);
		});
	}

	private static void writeCsvToStream(
			DataType[] types,
			BaseRow baseRow,
			OutputStream stream) throws IOException {
		LogicalType[] fieldTypes = Arrays.stream(types)
				.map(DataType::getLogicalType)
				.toArray(LogicalType[]::new);
		DataFormatConverters.DataFormatConverter converter = DataFormatConverters.getConverterForDataType(
				TypeConversions.fromLogicalToDataType(RowType.of(fieldTypes)));

		Row row = (Row) converter.toExternal(baseRow);
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
	public Optional<BulkWriter.Factory<BaseRow>> createBulkWriterFactory(WriterContext context) {
		if (!useBulkWriter(context)) {
			return Optional.empty();
		}

		DataType[] types = context.getFieldTypesWithoutPartKeys();
		return Optional.of(out -> new CsvBulkWriter(types, out));
	}

	private static class CsvBulkWriter implements BulkWriter<BaseRow> {

		private final DataType[] types;
		private final OutputStream stream;

		private CsvBulkWriter(DataType[] types, OutputStream stream) {
			this.types = types;
			this.stream = stream;
		}

		@Override
		public void addElement(BaseRow element) throws IOException {
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
