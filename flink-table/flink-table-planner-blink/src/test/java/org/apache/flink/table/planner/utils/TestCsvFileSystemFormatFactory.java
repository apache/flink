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
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.table.filesystem.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.api.java.io.CsvOutputFormat.DEFAULT_FIELD_DELIMITER;
import static org.apache.flink.api.java.io.CsvOutputFormat.DEFAULT_LINE_DELIMITER;

/**
 * Test csv {@link FileSystemFormatFactory}.
 */
public class TestCsvFileSystemFormatFactory extends TableFormatFactoryBase<BaseRow> implements FileSystemFormatFactory {

	public TestCsvFileSystemFormatFactory() {
		super("testcsv", 1, true);
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

	@Override
	public Optional<Encoder<BaseRow>> createEncoder(WriterContext context) {
		LogicalType[] fieldTypes = Arrays.stream(context.getFieldTypes())
				.map(DataType::getLogicalType)
				.toArray(LogicalType[]::new);
		DataFormatConverters.DataFormatConverter converter = DataFormatConverters.getConverterForDataType(
				TypeConversions.fromLogicalToDataType(RowType.of(fieldTypes)));
		return Optional.of((Encoder<BaseRow>) (baseRow, stream) -> {
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
		});
	}

	@Override
	public Optional<BulkWriter.Factory<BaseRow>> createBulkWriterFactory(WriterContext context) {
		return Optional.empty();
	}
}
