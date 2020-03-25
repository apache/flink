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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.descriptors.DescriptorProperties;
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
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_PATH;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_TYPE_VALUE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * File system {@link TableFactory}.
 *
 * <P>File system support:
 * 1.The partition information should be in the file system path, whether it's a temporary
 * table or a catalog table.
 * 2.Support insert into (append) and insert overwrite.
 * 3.Support static and dynamic partition inserting.
 */
public class FileSystemTableFactory implements
		TableSourceFactory<BaseRow>,
		TableSinkFactory<BaseRow> {

	public static final ConfigOption<String> PARTITION_DEFAULT_NAME = key("partition.default-name")
			.stringType()
			.defaultValue("__DEFAULT_PARTITION__")
			.withDescription("The default partition name in case the dynamic partition" +
					" column value is null/empty string");

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// path
		properties.add(CONNECTOR_PATH);

		// schema
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_NAME);

		properties.add(PARTITION_DEFAULT_NAME.key());

		// format wildcard
		properties.add(FORMAT + ".*");

		return properties;
	}

	@Override
	public TableSource<BaseRow> createTableSource(TableSourceFactory.Context context) {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(context.getTable().getProperties());

		return new FileSystemTableSource(
				context.getTable().getSchema(),
				new Path(properties.getString(CONNECTOR_PATH)),
				context.getTable().getPartitionKeys(),
				getPartitionDefaultName(properties),
				getFormatFactory(context.getTable().getProperties()));
	}

	@Override
	public TableSink<BaseRow> createTableSink(TableSinkFactory.Context context) {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(context.getTable().getProperties());

		return new FileSystemTableSink(
				context.getTable().getSchema(),
				new Path(properties.getString(CONNECTOR_PATH)),
				context.getTable().getPartitionKeys(),
				getPartitionDefaultName(properties),
				getFormatFactory(context.getTable().getProperties()));
	}

	private String getPartitionDefaultName(DescriptorProperties properties) {
		return properties
				.getOptionalString(PARTITION_DEFAULT_NAME.key())
				.orElse(PARTITION_DEFAULT_NAME.defaultValue());
	}

	private FileSystemFormatFactory getFormatFactory(Map<String, String> properties) {
		return TableFactoryService.find(
				FileSystemFormatFactory.class,
				properties,
				this.getClass().getClassLoader());
	}
}
