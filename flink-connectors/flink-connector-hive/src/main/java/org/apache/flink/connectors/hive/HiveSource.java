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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.file.src.AbstractFileSource;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connectors.hive.read.HiveBulkFormatAdapter;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.LimitableBulkFormat;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.flink.connector.file.src.FileSource.DEFAULT_SPLIT_ASSIGNER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A unified data source that reads a hive table.
 */
public class HiveSource extends AbstractFileSource<RowData, HiveSourceSplit> implements ResultTypeQueryable<RowData> {

	private static final long serialVersionUID = 1L;

	HiveSource(
			JobConf jobConf,
			CatalogTable catalogTable,
			List<HiveTablePartition> partitions,
			@Nullable Long limit,
			String hiveVersion,
			boolean useMapRedReader,
			boolean isStreamingSource,
			RowType producedRowType) {
		super(
				new org.apache.flink.core.fs.Path[1],
				new HiveSourceFileEnumerator.Provider(partitions, new JobConfWrapper(jobConf)),
				DEFAULT_SPLIT_ASSIGNER,
				createBulkFormat(new JobConf(jobConf), catalogTable, hiveVersion, producedRowType, useMapRedReader, limit),
				null);
		Preconditions.checkArgument(!isStreamingSource, "HiveSource currently only supports bounded mode");
	}

	@Override
	public SimpleVersionedSerializer<HiveSourceSplit> getSplitSerializer() {
		return HiveSourceSplitSerializer.INSTANCE;
	}

	private static BulkFormat<RowData, HiveSourceSplit> createBulkFormat(
			JobConf jobConf,
			CatalogTable catalogTable,
			String hiveVersion,
			RowType producedRowType,
			boolean useMapRedReader,
			Long limit) {
		checkNotNull(catalogTable, "catalogTable can not be null.");
		return LimitableBulkFormat.create(
				new HiveBulkFormatAdapter(
						new JobConfWrapper(jobConf),
						catalogTable.getPartitionKeys(),
						catalogTable.getSchema().getFieldNames(),
						catalogTable.getSchema().getFieldDataTypes(),
						hiveVersion,
						producedRowType,
						useMapRedReader),
				limit
		);
	}
}
