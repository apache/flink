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
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.filesystem.FileSystemOutputFormat;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Table sink to write to Hive tables.
 */
public class HiveTableSink extends OutputFormatTableSink<Row> implements PartitionableTableSink, OverwritableTableSink {

	private final JobConf jobConf;
	private final CatalogTable catalogTable;
	private final ObjectPath tablePath;
	private final TableSchema tableSchema;
	private final String hiveVersion;
	private final HiveShim hiveShim;

	private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

	private boolean overwrite = false;
	private boolean dynamicGrouping = false;

	public HiveTableSink(JobConf jobConf, ObjectPath tablePath, CatalogTable table) {
		this.jobConf = jobConf;
		this.tablePath = tablePath;
		this.catalogTable = table;
		hiveVersion = Preconditions.checkNotNull(jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION),
				"Hive version is not defined");
		hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
		tableSchema = TableSchemaUtils.getPhysicalSchema(table.getSchema());
	}

	@Override
	public OutputFormat<Row> getOutputFormat() {
		String[] partitionColumns = getPartitionFieldNames().toArray(new String[0]);
		String dbName = tablePath.getDatabaseName();
		String tableName = tablePath.getObjectName();
		try (HiveMetastoreClientWrapper client = HiveMetastoreClientFactory.create(
				new HiveConf(jobConf, HiveConf.class), hiveVersion)) {
			Table table = client.getTable(dbName, tableName);
			StorageDescriptor sd = table.getSd();

			FileSystemOutputFormat.Builder<Row> builder = new FileSystemOutputFormat.Builder<>();
			builder.setPartitionComputer(new HivePartitionComputer(
					hiveShim,
					jobConf.get(
							HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
							HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal),
					tableSchema.getFieldNames(),
					tableSchema.getFieldDataTypes(),
					partitionColumns));
			builder.setDynamicGrouped(dynamicGrouping);
			builder.setPartitionColumns(partitionColumns);
			builder.setFileSystemFactory(new HadoopFileSystemFactory(jobConf));

			boolean isCompressed = jobConf.getBoolean(HiveConf.ConfVars.COMPRESSRESULT.varname, false);
			Class hiveOutputFormatClz = hiveShim.getHiveOutputFormatClass(Class.forName(sd.getOutputFormat()));
			builder.setFormatFactory(new HiveOutputFormatFactory(
					jobConf,
					hiveOutputFormatClz,
					sd.getSerdeInfo(),
					tableSchema,
					partitionColumns,
					HiveReflectionUtils.getTableMetadata(hiveShim, table),
					hiveShim,
					isCompressed));
			builder.setMetaStoreFactory(
					new HiveTableMetaStoreFactory(jobConf, hiveVersion, dbName, tableName));
			builder.setOverwrite(overwrite);
			builder.setStaticPartitions(staticPartitionSpec);
			builder.setTempPath(new org.apache.flink.core.fs.Path(
					toStagingDir(sd.getLocation(), jobConf)));
			String extension = Utilities.getFileExtension(jobConf, isCompressed,
					(HiveOutputFormat<?, ?>) hiveOutputFormatClz.newInstance());
			extension = extension == null ? "" : extension;
			OutputFileConfig outputFileConfig = new OutputFileConfig("", extension);
			builder.setOutputFileConfig(outputFileConfig);

			return builder.build();
		} catch (TException e) {
			throw new CatalogException("Failed to query Hive metaStore", e);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to create staging dir", e);
		} catch (ClassNotFoundException e) {
			throw new FlinkHiveException("Failed to get output format class", e);
		} catch (IllegalAccessException | InstantiationException e) {
			throw new FlinkHiveException("Failed to instantiate output format instance", e);
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

	@Override
	public boolean configurePartitionGrouping(boolean supportsGrouping) {
		this.dynamicGrouping = supportsGrouping;
		return supportsGrouping;
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

	private List<String> getPartitionFieldNames() {
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

	@Override
	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}
}
