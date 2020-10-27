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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.file.src.AbstractFileSource;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connectors.hive.read.HiveBulkFormatAdapter;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.connector.file.src.FileSource.DEFAULT_SPLIT_ASSIGNER;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * A unified data source that reads a hive table.
 */
public class HiveSource extends AbstractFileSource<RowData, HiveSourceSplit> implements ResultTypeQueryable<RowData> {

	private static final long serialVersionUID = 1L;

	// DataType of the records to be returned, with projection applied (if any)
	private final DataType producedDataType;

	HiveSource(
			JobConf jobConf,
			CatalogTable catalogTable,
			List<HiveTablePartition> partitions,
			int[] projectedFields,
			long limit,
			String hiveVersion,
			boolean useMapRedReader,
			boolean isStreamingSource,
			DataType producedDataType) {
		super(
				new org.apache.flink.core.fs.Path[1],
				new HiveSourceFileEnumerator.Provider(partitions, new JobConfWrapper(jobConf)),
				DEFAULT_SPLIT_ASSIGNER,
				createBulkFormat(new JobConf(jobConf), catalogTable, projectedFields, hiveVersion, producedDataType, useMapRedReader, limit),
				null);
		Preconditions.checkArgument(!isStreamingSource, "HiveSource currently only supports bounded mode");
		this.producedDataType = producedDataType;
	}

	@Override
	public SimpleVersionedSerializer<HiveSourceSplit> getSplitSerializer() {
		return HiveSourceSplitSerializer.INSTANCE;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return (TypeInformation<RowData>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(producedDataType);
	}

	private static BulkFormat<RowData, HiveSourceSplit> createBulkFormat(
			JobConf jobConf,
			CatalogTable catalogTable,
			@Nullable int[] projectedFields,
			String hiveVersion,
			DataType producedDataType,
			boolean useMapRedReader,
			long limit) {
		checkNotNull(catalogTable, "catalogTable can not be null.");
		return new HiveBulkFormatAdapter(
				new JobConfWrapper(jobConf),
				catalogTable.getPartitionKeys(),
				catalogTable.getSchema().getFieldNames(),
				catalogTable.getSchema().getFieldDataTypes(),
				projectedFields != null ? projectedFields : IntStream.range(0, catalogTable.getSchema().getFieldCount()).toArray(),
				hiveVersion,
				producedDataType,
				useMapRedReader,
				limit);
	}

	public static List<HiveSourceSplit> createInputSplits(
			int minNumSplits,
			List<HiveTablePartition> partitions,
			JobConf jobConf) throws IOException {
		List<HiveSourceSplit> hiveSplits = new ArrayList<>();
		FileSystem fs = null;
		for (HiveTablePartition partition : partitions) {
			StorageDescriptor sd = partition.getStorageDescriptor();
			Path inputPath = new Path(sd.getLocation());
			if (fs == null) {
				fs = inputPath.getFileSystem(jobConf);
			}
			// it's possible a partition exists in metastore but the data has been removed
			if (!fs.exists(inputPath)) {
				continue;
			}
			InputFormat format;
			try {
				format = (InputFormat)
						Class.forName(sd.getInputFormat(), true, Thread.currentThread().getContextClassLoader()).newInstance();
			} catch (Exception e) {
				throw new FlinkHiveException("Unable to instantiate the hadoop input format", e);
			}
			ReflectionUtils.setConf(format, jobConf);
			jobConf.set(INPUT_DIR, sd.getLocation());
			//TODO: we should consider how to calculate the splits according to minNumSplits in the future.
			org.apache.hadoop.mapred.InputSplit[] splitArray = format.getSplits(jobConf, minNumSplits);
			for (org.apache.hadoop.mapred.InputSplit inputSplit : splitArray) {
				Preconditions.checkState(inputSplit instanceof FileSplit,
						"Unsupported InputSplit type: " + inputSplit.getClass().getName());
				hiveSplits.add(new HiveSourceSplit((FileSplit) inputSplit, partition, null));
			}
		}

		return hiveSplits;
	}

	public static int getNumFiles(List<HiveTablePartition> partitions, JobConf jobConf) throws IOException {
		int numFiles = 0;
		FileSystem fs = null;
		for (HiveTablePartition partition : partitions) {
			StorageDescriptor sd = partition.getStorageDescriptor();
			Path inputPath = new Path(sd.getLocation());
			if (fs == null) {
				fs = inputPath.getFileSystem(jobConf);
			}
			// it's possible a partition exists in metastore but the data has been removed
			if (!fs.exists(inputPath)) {
				continue;
			}
			numFiles += fs.listStatus(inputPath).length;
		}
		return numFiles;
	}
}
