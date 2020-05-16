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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.ScanFormat;
import org.apache.flink.table.connector.format.SinkFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.Set;

/**
 * Table format factory for providing configured instances of Avro to RowData {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public class AvroFormatFactory implements
		DeserializationFormatFactory,
		SerializationFormatFactory {

	public static final String IDENTIFIER = "avro";

	@Override
	public ScanFormat<DeserializationSchema<RowData>> createScanFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);

		return new ScanFormat<DeserializationSchema<RowData>>() {
			@Override
			public DeserializationSchema<RowData> createScanFormat(
					ScanTableSource.Context scanContext,
					DataType producedDataType) {
				final RowType rowType = (RowType) producedDataType.getLogicalType();
				final TypeInformation<RowData> rowDataTypeInfo =
						(TypeInformation<RowData>) scanContext.createTypeInformation(producedDataType);
				return new AvroRowDataDeserializationSchema(rowType, rowDataTypeInfo);
			}

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}
		};
	}

	@Override
	public SinkFormat<SerializationSchema<RowData>> createSinkFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);

		return new SinkFormat<SerializationSchema<RowData>>() {
			@Override
			public SerializationSchema<RowData> createSinkFormat(
					DynamicTableSink.Context context,
					DataType consumedDataType) {
				final RowType rowType = (RowType) consumedDataType.getLogicalType();
				return new AvroRowDataSerializationSchema(rowType);
			}

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}
		};
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return Collections.emptySet();
	}
}
