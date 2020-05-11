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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.ScanFormat;
import org.apache.flink.table.connector.format.SinkFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Test implementations for {@link DynamicTableSourceFactory} and {@link DynamicTableSinkFactory}.
 */
public final class TestDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	public static final String IDENTIFIER = "test-connector";

	public static final ConfigOption<String> TARGET = ConfigOptions
		.key("target")
		.stringType()
		.noDefaultValue();

	public static final ConfigOption<Long> BUFFER_SIZE = ConfigOptions
		.key("buffer-size")
		.longType()
		.defaultValue(100L);

	public static final ConfigOption<String> KEY_FORMAT = ConfigOptions
		.key("key.format.kind")
		.stringType()
		.noDefaultValue();

	public static final ConfigOption<String> VALUE_FORMAT = ConfigOptions
		.key("value.format.kind")
		.stringType()
		.noDefaultValue();

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		final Optional<ScanFormat<DeserializationSchema<RowData>>> keyFormat = helper.discoverOptionalScanFormat(
			DeserializationFormatFactory.class,
			KEY_FORMAT,
			FactoryUtil.KEY_FORMAT_PREFIX);
		final ScanFormat<DeserializationSchema<RowData>> valueFormat = helper.discoverScanFormat(
			DeserializationFormatFactory.class,
			VALUE_FORMAT,
			FactoryUtil.VALUE_FORMAT_PREFIX);
		helper.validate();

		return new DynamicTableSourceMock(
			helper.getOptions().get(TARGET),
			keyFormat.orElse(null),
			valueFormat);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		final Optional<SinkFormat<SerializationSchema<RowData>>> keyFormat = helper.discoverOptionalSinkFormat(
			SerializationFormatFactory.class,
			KEY_FORMAT,
			FactoryUtil.KEY_FORMAT_PREFIX);
		final SinkFormat<SerializationSchema<RowData>> valueFormat = helper.discoverSinkFormat(
			SerializationFormatFactory.class,
			VALUE_FORMAT,
			FactoryUtil.VALUE_FORMAT_PREFIX);
		helper.validate();

		return new DynamicTableSinkMock(
			helper.getOptions().get(TARGET),
			helper.getOptions().get(BUFFER_SIZE),
			keyFormat.orElse(null),
			valueFormat);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(TARGET);
		options.add(VALUE_FORMAT);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(BUFFER_SIZE);
		options.add(KEY_FORMAT);
		return options;
	}

	// --------------------------------------------------------------------------------------------
	// Table source
	// --------------------------------------------------------------------------------------------

	/**
	 * {@link DynamicTableSource} for testing.
	 */
	public static class DynamicTableSourceMock implements ScanTableSource {

		public final String target;
		public final @Nullable ScanFormat<DeserializationSchema<RowData>> sourceKeyFormat;
		public final ScanFormat<DeserializationSchema<RowData>> sourceValueFormat;

		DynamicTableSourceMock(
				String target,
				@Nullable ScanFormat<DeserializationSchema<RowData>> sourceKeyFormat,
				ScanFormat<DeserializationSchema<RowData>> sourceValueFormat) {
			this.target = target;
			this.sourceKeyFormat = sourceKeyFormat;
			this.sourceValueFormat = sourceValueFormat;
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return null;
		}

		@Override
		public ScanRuntimeProvider getScanRuntimeProvider(Context runtimeProviderContext) {
			return null;
		}

		@Override
		public DynamicTableSource copy() {
			return null;
		}

		@Override
		public String asSummaryString() {
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
			DynamicTableSourceMock that = (DynamicTableSourceMock) o;
			return target.equals(that.target) &&
				Objects.equals(sourceKeyFormat, that.sourceKeyFormat) &&
				sourceValueFormat.equals(that.sourceValueFormat);
		}

		@Override
		public int hashCode() {
			return Objects.hash(target, sourceKeyFormat, sourceValueFormat);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Table sink
	// --------------------------------------------------------------------------------------------

	/**
	 * {@link DynamicTableSink} for testing.
	 */
	public static class DynamicTableSinkMock implements DynamicTableSink {

		public final String target;
		public final Long bufferSize;
		public final @Nullable SinkFormat<SerializationSchema<RowData>> sinkKeyFormat;
		public final SinkFormat<SerializationSchema<RowData>> sinkValueFormat;

		DynamicTableSinkMock(
				String target,
				Long bufferSize,
				@Nullable SinkFormat<SerializationSchema<RowData>> sinkKeyFormat,
				SinkFormat<SerializationSchema<RowData>> sinkValueFormat) {
			this.target = target;
			this.bufferSize = bufferSize;
			this.sinkKeyFormat = sinkKeyFormat;
			this.sinkValueFormat = sinkValueFormat;
		}

		@Override
		public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
			return null;
		}

		@Override
		public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
			return null;
		}

		@Override
		public DynamicTableSink copy() {
			return null;
		}

		@Override
		public String asSummaryString() {
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
			DynamicTableSinkMock that = (DynamicTableSinkMock) o;
			return target.equals(that.target) &&
				bufferSize.equals(that.bufferSize) &&
				Objects.equals(sinkKeyFormat, that.sinkKeyFormat) &&
				sinkValueFormat.equals(that.sinkValueFormat);
		}

		@Override
		public int hashCode() {
			return Objects.hash(target, bufferSize, sinkKeyFormat, sinkValueFormat);
		}
	}
}
