/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.atomic;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.pulsar.util.DataTypeUtils;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * rowDataFormatFactory for atomic type.
 */
public class AtomicRowDataFormatFactory implements SerializationFormatFactory, DeserializationFormatFactory {

	private static final String IDENTIFIER = "atomic";

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
		DynamicTableFactory.Context context,
		ReadableConfig readableConfig) {

		return new DecodingFormat<DeserializationSchema<RowData>>() {
			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}

			@Override
			public DeserializationSchema<RowData> createRuntimeDecoder(
				DynamicTableSource.Context context,
				DataType dataType) {
				return new AtomicRowDataDeserializationSchema.Builder(getClassName(dataType))
					.useExtendFields(false)
					.build();
			}
		};
	}

	private String getClassName(@Nullable DataType dataType) {
		String classname;
		if (dataType instanceof AtomicDataType) {
			Optional<Class<Object>> classOptional = DataTypeUtils.extractType(dataType);
			classname = classOptional.map(Class::getName).orElse(null);
		} else if (dataType instanceof FieldsDataType) {
			final DataType type = dataType.getChildren().get(0);
			Optional<Class<Object>> classOptional = DataTypeUtils.extractType(type);
			classname = classOptional.map(Class::getName).orElse(null);
		} else {
			throw new IllegalArgumentException();
		}
		return classname;
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
		DynamicTableFactory.Context context,
		ReadableConfig readableConfig) {
		return new EncodingFormat<SerializationSchema<RowData>>() {
			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}

			@Override
			public SerializationSchema<RowData> createRuntimeEncoder(
				DynamicTableSink.Context context,
				DataType dataType) {
				return new AtomicRowDataSerializationSchema.Builder(getClassName(dataType))
					.useExtendFields(false)
					.build();
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
