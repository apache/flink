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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.stream.compact.CompactBulkReader;
import org.apache.flink.table.filesystem.stream.compact.CompactContext;
import org.apache.flink.table.filesystem.stream.compact.CompactReader;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connectors.hive.util.HivePartitionUtils.restorePartitionValueFromType;
import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/**
 * The {@link CompactReader} to delegate hive bulk format.
 */
public class HiveCompactReader extends CompactBulkReader<RowData> {

	private HiveCompactReader(BulkFormat.Reader<RowData> reader) throws IOException {
		super(reader);
	}

	public static CompactReader.Factory<RowData> factory(
			StorageDescriptor sd,
			Properties properties,
			JobConf jobConf,
			CatalogTable catalogTable,
			String hiveVersion,
			RowType producedRowType,
			boolean useMapRedReader) {
		return new Factory(
				sd,
				properties,
				new JobConfWrapper(jobConf),
				catalogTable.getPartitionKeys(),
				catalogTable.getSchema().getFieldNames(),
				catalogTable.getSchema().getFieldDataTypes(),
				hiveVersion,
				producedRowType,
				useMapRedReader);
	}

	/**
	 * Factory to create {@link HiveCompactReader}.
	 */
	private static class Factory implements CompactReader.Factory<RowData> {

		private static final long serialVersionUID = 1L;

		private final StorageDescriptor sd;
		private final Properties properties;
		private final JobConfWrapper jobConfWrapper;
		private final List<String> partitionKeys;
		private final String[] fieldNames;
		private final DataType[] fieldTypes;
		private final String hiveVersion;
		private final HiveShim shim;
		private final RowType producedRowType;
		private final boolean useMapRedReader;

		private Factory(
				StorageDescriptor sd,
				Properties properties,
				JobConfWrapper jobConfWrapper,
				List<String> partitionKeys,
				String[] fieldNames,
				DataType[] fieldTypes,
				String hiveVersion,
				RowType producedRowType,
				boolean useMapRedReader) {
			this.sd = sd;
			this.properties = properties;
			this.jobConfWrapper = jobConfWrapper;
			this.partitionKeys = partitionKeys;
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
			this.hiveVersion = hiveVersion;
			this.shim = HiveShimLoader.loadHiveShim(hiveVersion);
			this.producedRowType = producedRowType;
			this.useMapRedReader = useMapRedReader;
		}

		@Override
		public CompactReader<RowData> create(CompactContext context) throws IOException {
			HiveSourceSplit split = createSplit(context.getPath(), context.getFileSystem());
			HiveBulkFormatAdapter format = new HiveBulkFormatAdapter(
					jobConfWrapper, partitionKeys, fieldNames, fieldTypes, hiveVersion, producedRowType, useMapRedReader);
			BulkFormat.Reader<RowData> reader = format.createReader(context.getConfig(), split);
			return new HiveCompactReader(reader);
		}

		private HiveSourceSplit createSplit(Path path, FileSystem fs) throws IOException {
			long len = fs.getFileStatus(path).getLen();
			return new HiveSourceSplit("id", path, 0, len, new String[0], null, createPartition(path));
		}

		private HiveTablePartition createPartition(Path path) {
			Map<String, Object> partitionSpec = new LinkedHashMap<>();
			Map<String, DataType> nameToTypes = new HashMap<>();
			for (int i = 0; i < fieldNames.length; i++) {
				nameToTypes.put(fieldNames[i], fieldTypes[i]);
			}
			for (Map.Entry<String, String> entry : extractPartitionSpecFromPath(path).entrySet()) {
				Object partitionValue = restorePartitionValueFromType(
						shim, entry.getValue(), nameToTypes.get(entry.getKey()));
				partitionSpec.put(entry.getKey(), partitionValue);
			}

			return new HiveTablePartition(sd, partitionSpec, properties);
		}
	}
}
