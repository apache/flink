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

package org.apache.flink.table.factories;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Tests implementations for {@link DeserializationFormatFactory} and {@link SerializationFormatFactory}.
 */
public class TestFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

	public static final String IDENTIFIER = "test-format";

	public static final ConfigOption<String> DELIMITER = ConfigOptions
		.key("delimiter")
		.stringType()
		.noDefaultValue();

	public static final ConfigOption<Boolean> FAIL_ON_MISSING = ConfigOptions
		.key("fail-on-missing")
		.booleanType()
		.defaultValue(false);

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatConfig) {
		FactoryUtil.validateFactoryOptions(this, formatConfig);
		return new DecodingFormatMock(formatConfig.get(DELIMITER), formatConfig.get(FAIL_ON_MISSING));
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatConfig) {
		FactoryUtil.validateFactoryOptions(this, formatConfig);
		return new EncodingFormatMock(formatConfig.get(DELIMITER));
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(DELIMITER);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FAIL_ON_MISSING);
		return options;
	}

	// --------------------------------------------------------------------------------------------
	// Table source format
	// --------------------------------------------------------------------------------------------

	/**
	 * {@link DecodingFormat} for testing.
	 */
	public static class DecodingFormatMock implements DecodingFormat<DeserializationSchema<RowData>> {

		public final String delimiter;
		public final Boolean failOnMissing;

		public DecodingFormatMock(String delimiter, Boolean failOnMissing) {
			this.delimiter = delimiter;
			this.failOnMissing = failOnMissing;
		}

		@Override
		public DeserializationSchema<RowData> createRuntimeDecoder(
				DynamicTableSource.Context context,
				DataType producedDataType) {
			return null;
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return null;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			DecodingFormatMock that = (DecodingFormatMock) o;
			return delimiter.equals(that.delimiter) && failOnMissing.equals(that.failOnMissing);
		}

		@Override
		public int hashCode() {
			return Objects.hash(delimiter, failOnMissing);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Table sink format
	// --------------------------------------------------------------------------------------------

	/**
	 * {@link EncodingFormat} for testing.
	 */
	public static class EncodingFormatMock implements EncodingFormat<SerializationSchema<RowData>> {

		public final String delimiter;

		public EncodingFormatMock(String delimiter) {
			this.delimiter = delimiter;
		}

		@Override
		public SerializationSchema<RowData> createRuntimeEncoder(
				DynamicTableSink.Context context,
				DataType consumeDataType) {
			return null;
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return null;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			EncodingFormatMock that = (EncodingFormatMock) o;
			return delimiter.equals(that.delimiter);
		}

		@Override
		public int hashCode() {
			return Objects.hash(delimiter);
		}
	}
}
