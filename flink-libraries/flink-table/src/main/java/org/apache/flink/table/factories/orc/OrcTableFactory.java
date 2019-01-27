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

package org.apache.flink.table.factories.orc;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.orc.OrcTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.orc.OrcVectorizedColumnRowTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.util.StringUtils;

import org.apache.orc.CompressionKind;

import java.util.Arrays;
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
 * Orc TableFactory.
 */
public class OrcTableFactory implements BatchTableSinkFactory<BaseRow>,
	StreamTableSourceFactory<ColumnarRow>,
	BatchTableSourceFactory<ColumnarRow> {
	private static final String DEFAULT_WRITE_MODE = "None";

	private OrcVectorizedColumnRowTableSource createSource(Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);
		RichTableSchema schema = properties.readSchemaFromProperties(null);

		String filePath = properties.getString(ORCOptions.FILE_PATH);
		if (StringUtils.isNullOrWhitespaceOnly(filePath)) {
			throw new RuntimeException(ORCOptions.PARAMS_HELP_MSG);
		}

		boolean enumerateNestedFiles = properties.getBoolean(ORCOptions.ENUMERATE_NESTED_FILES);

		InternalType[] dataTypes =
			Arrays.stream(schema.getColumnTypes()).toArray(InternalType[]::new);

		OrcVectorizedColumnRowTableSource t =  new OrcVectorizedColumnRowTableSource(
			new Path(filePath),
			dataTypes,
			schema.getColumnNames(),
			enumerateNestedFiles);
		t.setSchemaFields(schema.getColumnNames());
		return t;
	}

	@Override
	public List<String> supportedProperties() {
		return ORCOptions.SUPPORTED_KEYS;
	}

	@Override
	public BatchTableSink<BaseRow> createBatchTableSink(Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);

		String filePath = properties.getString(ORCOptions.FILE_PATH);
		if (StringUtils.isNullOrWhitespaceOnly(filePath)) {
			throw new RuntimeException(ORCOptions.PARAMS_HELP_MSG);
		}

		Option<FileSystem.WriteMode> writeModeOption = null;
		String writeMode = properties.getString(ORCOptions.WRITE_MODE);
		if (!DEFAULT_WRITE_MODE.equals(writeMode)) {
			writeModeOption = new Some(FileSystem.WriteMode.valueOf(
					properties.getString(ORCOptions.WRITE_MODE)));
		}

		CompressionKind compressionKind = CompressionKind.valueOf(
				properties.getString(ORCOptions.COMPRESSION_CODEC_NAME));

		return new OrcTableSink(filePath, writeModeOption, compressionKind);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, FileSystemValidator.CONNECTOR_TYPE_VALUE()); // ORC
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		context.put(FORMAT_TYPE, "ORC");
		context.put(FORMAT_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public BatchTableSource<ColumnarRow> createBatchTableSource(Map<String, String> properties) {
		return createSource(properties);
	}

	@Override
	public StreamTableSource<ColumnarRow> createStreamTableSource(Map<String, String> properties) {
		return createSource(properties);
	}
}
