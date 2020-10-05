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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Factory for creating {@link KinesisDynamicSource} and {@link KinesisDynamicSink} instances.
 */
@Internal
public class KinesisDynamicTableFactory implements
	DynamicTableSourceFactory,
	DynamicTableSinkFactory {
	public static final String IDENTIFIER = "kinesis";

	// --------------------------------------------------------------------------------------------
	// DynamicTableSourceFactory
	// --------------------------------------------------------------------------------------------

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		ReadableConfig tableOptions = helper.getOptions();
		CatalogTable catalogTable = context.getCatalogTable();
		Properties properties = KinesisOptions.getConsumerProperties(catalogTable.getOptions());

		// initialize the table format early in order to register its consumedOptionKeys
		// in the TableFactoryHelper, as those are needed for correct option validation
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper
			.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);

		// Validate option data types
		helper.validateExcept(KinesisOptions.NON_VALIDATED_PREFIXES);
		// Validate option values
		validateConsumerProperties(tableOptions.get(KinesisOptions.STREAM), properties);

		return new KinesisDynamicSource(
			catalogTable.getSchema().toPhysicalRowDataType(),
			tableOptions.get(KinesisOptions.STREAM),
			properties,
			decodingFormat
		);
	}

	// --------------------------------------------------------------------------------------------
	// DynamicTableSinkFactory
	// --------------------------------------------------------------------------------------------

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		ReadableConfig tableOptions = helper.getOptions();
		CatalogTable catalogTable = context.getCatalogTable();
		Properties properties = KinesisOptions.getProducerProperties(catalogTable.getOptions());

		// initialize the table format early in order to register its consumedOptionKeys
		// in the TableFactoryHelper, as those are needed for correct option validation
		EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper
			.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);

		// validate the data types of the table options
		helper.validateExcept(KinesisOptions.NON_VALIDATED_PREFIXES);
		// Validate option values
		validateKinesisPartitioner(tableOptions, catalogTable);
		validateProducerProperties(properties);

		return new KinesisDynamicSink(
			catalogTable.getSchema().toPhysicalRowDataType(),
			tableOptions.get(KinesisOptions.STREAM),
			properties,
			encodingFormat,
			KinesisOptions.getKinesisPartitioner(
				tableOptions,
				catalogTable,
				context.getClassLoader()));
	}

	// --------------------------------------------------------------------------------------------
	// Factory
	// --------------------------------------------------------------------------------------------

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(KinesisOptions.STREAM);
		options.add(FactoryUtil.FORMAT);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(KinesisOptions.SINK_PARTITIONER);
		options.add(KinesisOptions.SINK_PARTITIONER_FIELD_DELIMITER);
		return options;
	}

	// --------------------------------------------------------------------------------------------
	// Validation helpers
	// --------------------------------------------------------------------------------------------

	private static void validateConsumerProperties(String stream, Properties properties) {
		List<String> streams = Collections.singletonList(stream);
		KinesisConfigUtil.validateConsumerConfiguration(properties, streams);
	}

	private static void validateProducerProperties(Properties properties) {
		// this assumes no side-effects as it is also called later from FlinkKinesisProducer#open()
		KinesisConfigUtil.getValidatedProducerConfiguration(properties);
	}

	public static void validateKinesisPartitioner(
		ReadableConfig tableOptions,
		CatalogTable targetTable) {
		tableOptions.getOptional(KinesisOptions.SINK_PARTITIONER).ifPresent(partitioner -> {
			if (targetTable.isPartitioned()) {
				throw new ValidationException(String.format(
					"Cannot set %s option for a table defined with a PARTITIONED BY clause",
					KinesisOptions.SINK_PARTITIONER.key()));
			}
		});
	}
}
