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

package org.apache.flink.orc;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;
import static org.apache.flink.table.filesystem.RowPartitionComputer.restorePartValueFromType;

/**
 * Orc {@link FileSystemFormatFactory} for file system.
 */
public class OrcFileSystemFormatFactory implements FileSystemFormatFactory {

	public static final String IDENTIFIER = "orc";

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
		// support "orc.*"
		return new HashSet<>();
	}

	private static Properties getOrcProperties(ReadableConfig options) {
		Properties orcProperties = new Properties();
		Properties properties = new Properties();
		((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
		properties.forEach((k, v) -> orcProperties.put(IDENTIFIER + "." + k, v));
		return orcProperties;
	}

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		return new OrcRowDataInputFormat(
				context.getPaths(),
				context.getSchema().getFieldNames(),
				context.getSchema().getFieldDataTypes(),
				context.getProjectFields(),
				context.getDefaultPartName(),
				context.getPushedDownLimit(),
				getOrcProperties(context.getFormatOptions()));
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		LogicalType[] orcTypes = Arrays.stream(context.getFormatFieldTypes())
				.map(DataType::getLogicalType)
				.toArray(LogicalType[]::new);

		TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(
				RowType.of(orcTypes, context.getFormatFieldNames()));

		OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(
				new RowDataVectorizer(typeDescription.toString(), orcTypes),
				getOrcProperties(context.getFormatOptions()),
				new Configuration());
		return Optional.of(factory);
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
		return Optional.empty();
	}

	/**
	 * An implementation of {@link FileInputFormat} to read {@link RowData} records
	 * from orc files.
	 */
	public static class OrcRowDataInputFormat extends FileInputFormat<RowData> {

		private static final long serialVersionUID = 1L;

		private final String[] fullFieldNames;
		private final DataType[] fullFieldTypes;
		private final int[] selectedFields;
		private final String partDefaultName;
		private final Properties properties;
		private final long limit;

		private transient OrcColumnarRowSplitReader<VectorizedRowBatch> reader;
		private transient long currentReadCount;

		public OrcRowDataInputFormat(
				Path[] paths,
				String[] fullFieldNames,
				DataType[] fullFieldTypes,
				int[] selectedFields,
				String partDefaultName,
				long limit,
				Properties properties) {
			super.setFilePaths(paths);
			this.limit = limit;
			this.partDefaultName = partDefaultName;
			this.fullFieldNames = fullFieldNames;
			this.fullFieldTypes = fullFieldTypes;
			this.selectedFields = selectedFields;
			this.properties = properties;
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

			Configuration conf = new Configuration();
			properties.forEach((k, v) -> conf.set(k.toString(), v.toString()));

			this.reader = OrcSplitReaderUtil.genPartColumnarRowReader(
					"3.1.1", // use the latest hive version
					conf,
					fullFieldNames,
					fullFieldTypes,
					partObjects,
					selectedFields,
					new ArrayList<>(),
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
			return reader.nextRecord(reuse);
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
