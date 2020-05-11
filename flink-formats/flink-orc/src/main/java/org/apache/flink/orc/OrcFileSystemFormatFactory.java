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
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.filesystem.RowPartitionComputer.restorePartValueFromType;

/**
 * Orc {@link FileSystemFormatFactory} for file system.
 */
public class OrcFileSystemFormatFactory implements FileSystemFormatFactory {

	/**
	 * Prefix for orc-related properties, besides format, start with "orc".
	 * See more in {@link org.apache.orc.OrcConf}.
	 */
	public static final String ORC_PROPERTIES_PREFIX = "format.orc";

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(FORMAT, "orc");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return Collections.singletonList(
				ORC_PROPERTIES_PREFIX + ".*"
		);
	}

	private static Properties getOrcProperties(DescriptorProperties properties) {
		Properties conf = new Properties();
		properties.asMap().keySet()
				.stream()
				.filter(key -> key.startsWith(ORC_PROPERTIES_PREFIX))
				.forEach(key -> {
					String value = properties.getString(key);
					String subKey = key.substring((FORMAT + '.').length());
					conf.put(subKey, value);
				});
		return conf;
	}

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(context.getFormatProperties());

		return new OrcRowDataInputFormat(
				context.getPaths(),
				context.getSchema().getFieldNames(),
				context.getSchema().getFieldDataTypes(),
				context.getProjectFields(),
				context.getDefaultPartName(),
				context.getPushedDownLimit(),
				getOrcProperties(properties));
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(context.getFormatProperties());

		LogicalType[] orcTypes = Arrays.stream(context.getFormatFieldTypes())
				.map(DataType::getLogicalType)
				.toArray(LogicalType[]::new);

		TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(
				RowType.of(orcTypes, context.getFormatFieldNames()));

		OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(
				new RowDataVectorizer(typeDescription.toString(), orcTypes),
				getOrcProperties(properties),
				new Configuration());
		return Optional.of(factory);
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
		return Optional.empty();
	}

	@Override
	public boolean supportsSchemaDerivation() {
		return true;
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
