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

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table sink to write to Hive tables.
 */
public class HiveTableSink extends OutputFormatTableSink<Row> implements PartitionableTableSink, OverwritableTableSink {

	private final JobConf jobConf;
	private final CatalogTable catalogTable;
	private final ObjectPath tablePath;
	private final TableSchema tableSchema;
	private final String hiveVersion;

	private Map<String, String> staticPartitionSpec = Collections.emptyMap();

	private boolean overwrite = false;

	public HiveTableSink(JobConf jobConf, ObjectPath tablePath, CatalogTable table) {
		this.jobConf = jobConf;
		this.tablePath = tablePath;
		this.catalogTable = table;
		hiveVersion = Preconditions.checkNotNull(jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION),
				"Hive version is not defined");
		tableSchema = table.getSchema();
	}

	@Override
	public OutputFormat<Row> getOutputFormat() {
		List<String> partitionColumns = getPartitionFieldNames();
		boolean isPartitioned = partitionColumns != null && !partitionColumns.isEmpty();
		boolean isDynamicPartition = isPartitioned && partitionColumns.size() > staticPartitionSpec.size();
		String dbName = tablePath.getDatabaseName();
		String tableName = tablePath.getObjectName();
		try (HiveMetastoreClientWrapper client = HiveMetastoreClientFactory.create(new HiveConf(jobConf, HiveConf.class), hiveVersion)) {
			Table table = client.getTable(dbName, tableName);
			StorageDescriptor sd = table.getSd();
			// here we use the sdLocation to store the output path of the job, which is always a staging dir
			String sdLocation = sd.getLocation();
			HiveTablePartition hiveTablePartition;
			if (isPartitioned) {
				validatePartitionSpec();
				if (isDynamicPartition) {
					List<String> path = new ArrayList<>(2);
					path.add(sd.getLocation());
					if (!staticPartitionSpec.isEmpty()) {
						path.add(Warehouse.makePartName(staticPartitionSpec, false));
					}
					sdLocation = String.join(Path.SEPARATOR, path);
				} else {
					List<Partition> partitions = client.listPartitions(dbName, tableName,
							new ArrayList<>(staticPartitionSpec.values()), (short) 1);
					sdLocation = !partitions.isEmpty() ? partitions.get(0).getSd().getLocation() :
							sd.getLocation() + Path.SEPARATOR + Warehouse.makePartName(staticPartitionSpec, true);
				}

				sd.setLocation(toStagingDir(sdLocation, jobConf));
				hiveTablePartition = new HiveTablePartition(sd, new LinkedHashMap<>(staticPartitionSpec));
			} else {
				sd.setLocation(toStagingDir(sdLocation, jobConf));
				hiveTablePartition = new HiveTablePartition(sd, null);
			}
			return new HiveTableOutputFormat(
				jobConf,
				tablePath,
				catalogTable,
				hiveTablePartition,
				MetaStoreUtils.getTableMetadata(table),
				overwrite);
		} catch (TException e) {
			throw new CatalogException("Failed to query Hive metaStore", e);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to create staging dir", e);
		}
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return new HiveTableSink(jobConf, tablePath, catalogTable);
	}

	@Override
	public DataType getConsumedDataType() {
		return getTableSchema().toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
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

	@Override
	public List<String> getPartitionFieldNames() {
		return catalogTable.getPartitionKeys();
	}

	@Override
	public void setStaticPartition(Map<String, String> partitionSpec) {
		// make it a LinkedHashMap to maintain partition column order
		staticPartitionSpec = new LinkedHashMap<>();
		for (String partitionCol : getPartitionFieldNames()) {
			if (partitionSpec.containsKey(partitionCol)) {
				staticPartitionSpec.put(partitionCol, partitionSpec.get(partitionCol));
			}
		}
	}

	private void validatePartitionSpec() {
		List<String> partitionCols = getPartitionFieldNames();
		List<String> unknownPartCols = staticPartitionSpec.keySet().stream().filter(k -> !partitionCols.contains(k)).collect(Collectors.toList());
		Preconditions.checkArgument(
				unknownPartCols.isEmpty(),
				"Static partition spec contains unknown partition column: " + unknownPartCols.toString());
		int numStaticPart = staticPartitionSpec.size();
		if (numStaticPart < partitionCols.size()) {
			for (String partitionCol : partitionCols) {
				if (!staticPartitionSpec.containsKey(partitionCol)) {
					// this is a dynamic partition, make sure we have seen all static ones
					Preconditions.checkArgument(numStaticPart == 0,
							"Dynamic partition cannot appear before static partition");
					return;
				} else {
					numStaticPart--;
				}
			}
		}
	}

	@Override
	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}
}
