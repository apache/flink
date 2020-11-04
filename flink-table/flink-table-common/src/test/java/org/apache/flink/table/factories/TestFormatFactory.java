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
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.List;
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

	public static final ConfigOption<List<String>> CHANGELOG_MODE = ConfigOptions
			.key("changelog-mode")
			.stringType()
			.asList()
			.noDefaultValue();

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatConfig) {
		FactoryUtil.validateFactoryOptions(this, formatConfig);
		return new DecodingFormatMock(
				formatConfig.get(DELIMITER), formatConfig.get(FAIL_ON_MISSING), parseChangelogMode(formatConfig));
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatConfig) {
		FactoryUtil.validateFactoryOptions(this, formatConfig);
		return new EncodingFormatMock(
				formatConfig.get(DELIMITER), parseChangelogMode(formatConfig));
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
		options.add(CHANGELOG_MODE);
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
		private final ChangelogMode changelogMode;

		// we make the format stateful for capturing parameterization during testing
		public DataType producedDataType;

		public DecodingFormatMock(String delimiter, Boolean failOnMissing) {
			this(delimiter, failOnMissing, ChangelogMode.insertOnly());
		}

		public DecodingFormatMock(String delimiter, Boolean failOnMissing, ChangelogMode changelogMode) {
			this.delimiter = delimiter;
			this.failOnMissing = failOnMissing;
			this.changelogMode = changelogMode;
		}

		@Override
		public DeserializationSchema<RowData> createRuntimeDecoder(
				DynamicTableSource.Context context,
				DataType producedDataType) {
			this.producedDataType = producedDataType;
			return new DeserializationSchema<RowData>() {
				@Override
				public RowData deserialize(byte[] message) {
					throw new UnsupportedOperationException("Test deserialization schema doesn't support deserialize.");
				}

				@Override
				public boolean isEndOfStream(RowData nextElement) {
					return false;
				}

				@Override
				public TypeInformation<RowData> getProducedType() {
					return context.createTypeInformation(producedDataType);
				}
			};
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return changelogMode;
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
			return delimiter.equals(that.delimiter)
					&& failOnMissing.equals(that.failOnMissing)
					&& Objects.equals(producedDataType, that.producedDataType);
		}

		@Override
		public int hashCode() {
			return Objects.hash(delimiter, failOnMissing, producedDataType);
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

		// we make the format stateful for capturing parameterization during testing
		public DataType consumedDataType;

		private ChangelogMode changelogMode;

		public EncodingFormatMock(String delimiter) {
			this(delimiter, ChangelogMode.insertOnly());
		}

		public EncodingFormatMock(String delimiter, ChangelogMode changelogMode) {
			this.delimiter = delimiter;
			this.changelogMode = changelogMode;
		}

		@Override
		public SerializationSchema<RowData> createRuntimeEncoder(
				DynamicTableSink.Context context,
				DataType consumedDataType) {
			this.consumedDataType = consumedDataType;
			return new SerializationSchema<RowData>() {
				@Override
				public byte[] serialize(RowData element) {
					throw new UnsupportedOperationException("Test serialization schema doesn't support serialize.");
				}
			};
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return changelogMode;
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
			return delimiter.equals(that.delimiter)
					&& Objects.equals(consumedDataType, that.consumedDataType);
		}

		@Override
		public int hashCode() {
			return Objects.hash(delimiter, consumedDataType);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Utils
	// --------------------------------------------------------------------------------------------

	private ChangelogMode parseChangelogMode(ReadableConfig config) {
		if (config.getOptional(CHANGELOG_MODE).isPresent()) {
			ChangelogMode.Builder builder = ChangelogMode.newBuilder();
			for (String mode: config.get(CHANGELOG_MODE)) {
				switch (mode) {
					case "I":
						builder.addContainedKind(RowKind.INSERT);
						break;
					case "UA":
						builder.addContainedKind(RowKind.UPDATE_AFTER);
						break;
					case "UB":
						builder.addContainedKind(RowKind.UPDATE_BEFORE);
						break;
					case "D":
						builder.addContainedKind(RowKind.DELETE);
						break;
					default:
						throw new IllegalArgumentException(
								String.format("Unrecognized type %s for config %s", mode, CHANGELOG_MODE.key()));
				}
			}
			return builder.build();
		} else {
			return ChangelogMode.insertOnly();
		}
	}

}
