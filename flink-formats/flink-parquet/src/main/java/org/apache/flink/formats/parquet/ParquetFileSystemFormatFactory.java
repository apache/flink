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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;
import static org.apache.flink.table.filesystem.RowPartitionComputer.restorePartValueFromType;

/**
 * Parquet {@link FileSystemFormatFactory} for file system.
 */
public class ParquetFileSystemFormatFactory implements FileSystemFormatFactory {

	public static final String IDENTIFIER = "parquet";

	public static final ConfigOption<Boolean> UTC_TIMEZONE = key("utc-timezone")
			.booleanType()
			.defaultValue(false)
			.withDescription("Use UTC timezone or local timezone to the conversion between epoch" +
					" time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x" +
					" use UTC timezone");

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return new HashSet<>();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(UTC_TIMEZONE);
		// support "parquet.*"
		return options;
	}

	private static Configuration getParquetConfiguration(ReadableConfig options) {
		Configuration conf = new Configuration();
		Properties properties = new Properties();
		((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
		properties.forEach((k, v) -> conf.set(IDENTIFIER + "." + k, v.toString()));
		return conf;
	}

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		return new ParquetInputFormat(
				context.getPaths(),
				context.getSchema().getFieldNames(),
				context.getSchema().getFieldDataTypes(),
				context.getProjectFields(),
				context.getDefaultPartName(),
				context.getPushedDownLimit(),
				getParquetConfiguration(context.getFormatOptions()),
				context.getFormatOptions().get(UTC_TIMEZONE));
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		return Optional.of(ParquetRowDataBuilder.createWriterFactory(
				RowType.of(Arrays.stream(context.getFormatFieldTypes())
								.map(DataType::getLogicalType)
								.toArray(LogicalType[]::new),
						context.getFormatFieldNames()),
				getParquetConfiguration(context.getFormatOptions()),
				context.getFormatOptions().get(UTC_TIMEZONE)));
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
		return Optional.empty();
	}

	/**
	 * An implementation of {@link ParquetInputFormat} to read {@link RowData} records
	 * from Parquet files.
	 */
	public static class ParquetInputFormat extends FileInputFormat<RowData> {

		private static final long serialVersionUID = 1L;

		private final String[] fullFieldNames;
		private final DataType[] fullFieldTypes;
		private final int[] selectedFields;
		private final String partDefaultName;
		private final boolean utcTimestamp;
		private final SerializableConfiguration conf;
		private final long limit;

		private transient ParquetColumnarRowSplitReader reader;
		private transient long currentReadCount;

		public ParquetInputFormat(
				Path[] paths,
				String[] fullFieldNames,
				DataType[] fullFieldTypes,
				int[] selectedFields,
				String partDefaultName,
				long limit,
				Configuration conf,
				boolean utcTimestamp) {
			super.setFilePaths(paths);
			this.limit = limit;
			this.partDefaultName = partDefaultName;
			this.fullFieldNames = fullFieldNames;
			this.fullFieldTypes = fullFieldTypes;
			this.selectedFields = selectedFields;
			this.conf = new SerializableConfiguration(conf);
			this.utcTimestamp = utcTimestamp;
		}

		@Override
		public void open(FileInputSplit fileSplit) throws IOException {
			// generate partition specs.
			List<String> fieldNameList = Arrays.asList(fullFieldNames);
			LinkedHashMap<String, String> partSpec = PartitionPathUtils.extractPartitionSpecFromPath(
					fileSplit.getPath());
			LinkedHashMap<String, Object> partObjects = new LinkedHashMap<>();
			partSpec.forEach((k, v) -> partObjects.put(k, restorePartValueFromType(
					partDefaultName.equals(v) ? null : v,
					fullFieldTypes[fieldNameList.indexOf(k)])));

			this.reader = ParquetSplitReaderUtil.genPartColumnarRowReader(
					utcTimestamp,
					conf.conf(),
					fullFieldNames,
					fullFieldTypes,
					partObjects,
					selectedFields,
					DEFAULT_SIZE,
					new Path(fileSplit.getPath().toString()),
					fileSplit.getStart(),
					fileSplit.getLength());
			this.currentReadCount = 0L;
		}

		@Override
		public boolean supportsMultiPaths() {
			return true;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (currentReadCount >= limit) {
				return true;
			} else {
				return reader.reachedEnd();
			}
		}

		@Override
		public RowData nextRecord(RowData reuse) {
			currentReadCount++;
			return reader.nextRecord();
		}

		@Override
		public void close() throws IOException {
			if (reader != null) {
				this.reader.close();
			}
			this.reader = null;
		}
	}
}
