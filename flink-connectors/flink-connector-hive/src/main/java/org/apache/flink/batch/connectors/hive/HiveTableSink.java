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

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Table sink to write to Hive tables.
 */
public class HiveTableSink extends OutputFormatTableSink<Row> {

	private final JobConf jobConf;
	private final RowTypeInfo rowTypeInfo;
	private final String dbName;
	private final String tableName;
	private final List<String> partitionColumns;
	private final String hiveVersion;

	// TODO: need OverwritableTableSink to configure this
	private boolean overwrite = false;

	public HiveTableSink(JobConf jobConf, RowTypeInfo rowTypeInfo, String dbName, String tableName,
			List<String> partitionColumns) {
		this.jobConf = jobConf;
		this.rowTypeInfo = rowTypeInfo;
		this.dbName = dbName;
		this.tableName = tableName;
		this.partitionColumns = partitionColumns;
		hiveVersion = jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION, HiveShimLoader.getHiveVersion());
	}

	@Override
	public OutputFormat<Row> getOutputFormat() {
		boolean isPartitioned = partitionColumns != null && !partitionColumns.isEmpty();
		// TODO: need PartitionableTableSink to decide whether it's dynamic partitioning
		boolean isDynamicPartition = isPartitioned;
		try (HiveMetastoreClientWrapper client = HiveMetastoreClientFactory.create(new HiveConf(jobConf, HiveConf.class), hiveVersion)) {
			Table table = client.getTable(dbName, tableName);
			StorageDescriptor sd = table.getSd();
			// here we use the sdLocation to store the output path of the job, which is always a staging dir
			String sdLocation = sd.getLocation();
			HiveTablePartition hiveTablePartition;
			if (isPartitioned) {
				// TODO: validate partition spec
				// TODO: strip quotes in partition values
				LinkedHashMap<String, String> strippedPartSpec = new LinkedHashMap<>();
				if (isDynamicPartition) {
					List<String> path = new ArrayList<>(2);
					path.add(sd.getLocation());
					if (!strippedPartSpec.isEmpty()) {
						path.add(Warehouse.makePartName(strippedPartSpec, false));
					}
					sdLocation = String.join(Path.SEPARATOR, path);
				} else {
					List<Partition> partitions = client.listPartitions(dbName, tableName,
							new ArrayList<>(strippedPartSpec.values()), (short) 1);
					sdLocation = !partitions.isEmpty() ? partitions.get(0).getSd().getLocation() :
							sd.getLocation() + Path.SEPARATOR + Warehouse.makePartName(strippedPartSpec, true);
				}

				sd.setLocation(toStagingDir(sdLocation, jobConf));
				hiveTablePartition = new HiveTablePartition(sd, new LinkedHashMap<>(strippedPartSpec));
			} else {
				sd.setLocation(toStagingDir(sdLocation, jobConf));
				hiveTablePartition = new HiveTablePartition(sd, null);
			}
			return new HiveTableOutputFormat(jobConf, dbName, tableName,
					partitionColumns, rowTypeInfo, hiveTablePartition, MetaStoreUtils.getTableMetadata(table), overwrite);
		} catch (TException e) {
			throw new CatalogException("Failed to query Hive metaStore", e);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to create staging dir", e);
		}
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return new HiveTableSink(jobConf, new RowTypeInfo(fieldTypes, fieldNames), dbName, tableName, partitionColumns);
	}

	@Override
	public String[] getFieldNames() {
		return rowTypeInfo.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return rowTypeInfo.getFieldTypes();
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return rowTypeInfo;
	}

	// get a staging dir associated with a final dir
	private String toStagingDir(String finalDir, Configuration conf) throws IOException {
		String res = finalDir;
		if (!finalDir.endsWith(Path.SEPARATOR)) {
			res += Path.SEPARATOR;
		}
		// TODO: may append something more meaningful than a timestamp, like query ID
		res += ".staging_" + System.currentTimeMillis();
		Path path = new Path(res);
		FileSystem fs = path.getFileSystem(conf);
		Preconditions.checkState(fs.exists(path) || fs.mkdirs(path), "Failed to create staging dir " + path);
		fs.deleteOnExit(path);
		return res;
	}
}
