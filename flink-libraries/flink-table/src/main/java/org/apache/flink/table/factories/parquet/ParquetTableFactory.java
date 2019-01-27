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

package org.apache.flink.table.factories.parquet;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.parquet.ParquetTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.parquet.ParquetVectorizedColumnRowTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.util.StringUtils;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Option;
import scala.Some;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;

/**
 * Parquet table factory.
 */
public class ParquetTableFactory implements
	StreamTableSourceFactory<ColumnarRow>,
	BatchTableSourceFactory<ColumnarRow>,
	BatchTableSinkFactory<BaseRow> {
	private static final String DEFAULT_WRITE_MODE = "None";

	private ParquetVectorizedColumnRowTableSource getSource(Map<String, String> props) {
		TableProperties tableProperties = new TableProperties();
		tableProperties.putProperties(props);
		RichTableSchema richTableSchema = tableProperties.readSchemaFromProperties(null);

		String filePath = tableProperties.getString(ParquetOptions.FILE_PATH);
		if (StringUtils.isNullOrWhitespaceOnly(filePath)) {
			throw new RuntimeException(ParquetOptions.PARAMS_HELP_MSG);
		}

		boolean enumerateNestedFiles = tableProperties.getBoolean(ParquetOptions.ENUMERATE_NESTED_FILES);
		return new ParquetVectorizedColumnRowTableSource(
				new Path(filePath),
				richTableSchema.getColumnTypes(),
				richTableSchema.getColumnNames(),
				enumerateNestedFiles);
	}

	@Override
	public StreamTableSource<ColumnarRow> createStreamTableSource(Map<String, String> props) {
		return getSource(props);
	}

	@Override
	public BatchTableSource<ColumnarRow> createBatchTableSource(Map<String, String> props) {
		return getSource(props);
	}

	@Override
	public List<String> supportedProperties() {
		return ParquetOptions.SUPPORTED_KEYS;
	}

	@Override
	public BatchTableSink<BaseRow> createBatchTableSink(Map<String, String> props) {
		TableProperties tableProperties = new TableProperties();
		tableProperties.putProperties(props);

		String filePath = tableProperties.getString(ParquetOptions.FILE_PATH);
		if (StringUtils.isNullOrWhitespaceOnly(filePath)) {
			throw new RuntimeException(ParquetOptions.PARAMS_HELP_MSG);
		}

		Option<WriteMode> writeModeOption = null;
		String writeMode = tableProperties.getString(ParquetOptions.WRITE_MODE);
		if (!DEFAULT_WRITE_MODE.equals(writeMode)) {
			writeModeOption = new Some(WriteMode.valueOf(
					tableProperties.getString(ParquetOptions.WRITE_MODE)));
		}

		CompressionCodecName compressionCodecName = CompressionCodecName.valueOf(tableProperties
				.getString(ParquetOptions.COMPRESSION_CODEC_NAME));

		return new ParquetTableSink(filePath, writeModeOption, compressionCodecName);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, FileSystemValidator.CONNECTOR_TYPE_VALUE()); // PARQUET
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		context.put(FORMAT_TYPE, "PARQUET");
		context.put(FORMAT_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}
}
