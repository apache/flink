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

package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_TRIGGER;

/**
 * File system {@link TableFactory}.
 *
 * <p>1.The partition information should be in the file system path, whether it's a temporary
 * table or a catalog table.
 * 2.Support insert into (append) and insert overwrite.
 * 3.Support static and dynamic partition inserting.
 *
 * <p>Migrate to new source/sink interface after FLIP-95 is ready.
 */
public class FileSystemTableFactory implements
		TableSourceFactory<RowData>,
		TableSinkFactory<RowData> {

	public static final String CONNECTOR_VALUE = "filesystem";

	/**
	 * Not use "connector.path" because:
	 * 1.Using "connector.path" will conflict with current batch csv source and batch csv sink.
	 * 2.This is compatible with FLIP-122.
	 */
	public static final String PATH = "path";

	/**
	 * Move these properties to validator after FLINK-16904.
	 */
	public static final ConfigOption<String> PARTITION_DEFAULT_NAME = key("partition.default-name")
			.stringType()
			.defaultValue("__DEFAULT_PARTITION__")
			.withDescription("The default partition name in case the dynamic partition" +
					" column value is null/empty string");

	public static final ConfigOption<Long> SINK_ROLLING_POLICY_FILE_SIZE = key("sink.rolling-policy.file-size")
			.longType()
			.defaultValue(1024L * 1024L * 128L)
			.withDescription("The maximum part file size before rolling (by default 128MB).");

	public static final ConfigOption<Long> SINK_ROLLING_POLICY_TIME_INTERVAL = key("sink.rolling-policy.time.interval")
			.longType()
			.defaultValue(30L * 60 * 1000L)
			.withDescription("The maximum time duration a part file can stay open before rolling" +
					" (by default 30 min to avoid to many small files).");

	public static final ConfigOption<Boolean> SINK_SHUFFLE_BY_PARTITION = key("sink.shuffle-by-partition.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("The option to enable shuffle data by dynamic partition fields in sink" +
					" phase, this can greatly reduce the number of file for filesystem sink but may" +
					" lead data skew, the default value is disabled.");

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR, CONNECTOR_VALUE);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// path
		properties.add(PATH);

		// schema
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_NAME);

		// partition
		properties.add(DescriptorProperties.PARTITION_KEYS + ".#." +
				DescriptorProperties.PARTITION_KEYS_NAME);
		properties.add(PARTITION_DEFAULT_NAME.key());

		properties.add(SINK_ROLLING_POLICY_FILE_SIZE.key());
		properties.add(SINK_ROLLING_POLICY_TIME_INTERVAL.key());
		properties.add(SINK_SHUFFLE_BY_PARTITION.key());
		properties.add(PARTITION_TIME_EXTRACTOR_KIND.key());
		properties.add(PARTITION_TIME_EXTRACTOR_CLASS.key());
		properties.add(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key());
		properties.add(SINK_PARTITION_COMMIT_TRIGGER.key());
		properties.add(SINK_PARTITION_COMMIT_DELAY.key());
		properties.add(SINK_PARTITION_COMMIT_POLICY_KIND.key());
		properties.add(SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.key());

		// format
		properties.add(FORMAT);
		properties.add(FORMAT + ".*");

		return properties;
	}

	@Override
	public TableSource<RowData> createTableSource(TableSourceFactory.Context context) {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(context.getTable().getProperties());

		return new FileSystemTableSource(
				context.getTable().getSchema(),
				new Path(properties.getString(PATH)),
				context.getTable().getPartitionKeys(),
				getPartitionDefaultName(properties),
				context.getTable().getProperties());
	}

	@Override
	public TableSink<RowData> createTableSink(TableSinkFactory.Context context) {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(context.getTable().getProperties());

		return new FileSystemTableSink(
				context.getObjectIdentifier(),
				context.isBounded(),
				context.getTable().getSchema(),
				new Path(properties.getString(PATH)),
				context.getTable().getPartitionKeys(),
				getPartitionDefaultName(properties),
				context.getTable().getOptions());
	}

	private static String getPartitionDefaultName(DescriptorProperties properties) {
		return properties
				.getOptionalString(PARTITION_DEFAULT_NAME.key())
				.orElse(PARTITION_DEFAULT_NAME.defaultValue());
	}

	public static FileSystemFormatFactory createFormatFactory(Map<String, String> properties) {
		return TableFactoryService.find(
				FileSystemFormatFactory.class,
				properties,
				FileSystemTableFactory.class.getClassLoader());
	}
}
