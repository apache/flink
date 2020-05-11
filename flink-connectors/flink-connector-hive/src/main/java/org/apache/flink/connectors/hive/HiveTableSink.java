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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.hive.write.HiveBulkWriterFactory;
import org.apache.flink.connectors.hive.write.HiveOutputFormatFactory;
import org.apache.flink.connectors.hive.write.HiveWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.HadoopPathBasedBulkFormatBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.BucketsBuilder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.FileSystemOutputFormat;
import org.apache.flink.table.filesystem.FileSystemTableSink;
import org.apache.flink.table.filesystem.FileSystemTableSink.TableBucketAssigner;
import org.apache.flink.table.filesystem.FileSystemTableSink.TableRollingPolicy;
import org.apache.flink.table.filesystem.stream.InactiveBucketListener;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
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
import org.apache.orc.TypeDescription;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_FILE_SIZE;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_TIME_INTERVAL;

/**
 * Table sink to write to Hive tables.
 */
public class HiveTableSink implements AppendStreamTableSink, PartitionableTableSink, OverwritableTableSink {

	private static final Logger LOG = LoggerFactory.getLogger(HiveTableSink.class);

	private final boolean userMrWriter;
	private final boolean isBounded;
	private final JobConf jobConf;
	private final CatalogTable catalogTable;
	private final ObjectIdentifier identifier;
	private final TableSchema tableSchema;
	private final String hiveVersion;
	private final HiveShim hiveShim;

	private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

	private boolean overwrite = false;
	private boolean dynamicGrouping = false;

	public HiveTableSink(
			boolean userMrWriter, boolean isBounded, JobConf jobConf, ObjectIdentifier identifier, CatalogTable table) {
		this.userMrWriter = userMrWriter;
		this.isBounded = isBounded;
		this.jobConf = jobConf;
		this.identifier = identifier;
		this.catalogTable = table;
		hiveVersion = Preconditions.checkNotNull(jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION),
				"Hive version is not defined");
		hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
		tableSchema = TableSchemaUtils.getPhysicalSchema(table.getSchema());
	}

	@Override
	public final DataStreamSink consumeDataStream(DataStream dataStream) {
		String[] partitionColumns = getPartitionKeys().toArray(new String[0]);
		String dbName = identifier.getDatabaseName();
		String tableName = identifier.getObjectName();
		try (HiveMetastoreClientWrapper client = HiveMetastoreClientFactory.create(
				new HiveConf(jobConf, HiveConf.class), hiveVersion)) {
			Table table = client.getTable(dbName, tableName);
			StorageDescriptor sd = table.getSd();
			HiveTableMetaStoreFactory msFactory = new HiveTableMetaStoreFactory(
					jobConf, hiveVersion, dbName, tableName);

			Class hiveOutputFormatClz = hiveShim.getHiveOutputFormatClass(
					Class.forName(sd.getOutputFormat()));
			boolean isCompressed = jobConf.getBoolean(HiveConf.ConfVars.COMPRESSRESULT.varname, false);
			HiveWriterFactory recordWriterFactory = new HiveWriterFactory(
					jobConf,
					hiveOutputFormatClz,
					sd.getSerdeInfo(),
					tableSchema,
					partitionColumns,
					HiveReflectionUtils.getTableMetadata(hiveShim, table),
					hiveShim,
					isCompressed);
			String extension = Utilities.getFileExtension(jobConf, isCompressed,
					(HiveOutputFormat<?, ?>) hiveOutputFormatClz.newInstance());
			extension = extension == null ? "" : extension;
			OutputFileConfig outputFileConfig = OutputFileConfig.builder()
					.withPartSuffix(extension).build();
			if (isBounded) {
				FileSystemOutputFormat.Builder<Row> builder = new FileSystemOutputFormat.Builder<>();
				builder.setPartitionComputer(new HiveRowPartitionComputer(
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
				builder.setFormatFactory(new HiveOutputFormatFactory(recordWriterFactory));
				builder.setMetaStoreFactory(
						msFactory);
				builder.setOverwrite(overwrite);
				builder.setStaticPartitions(staticPartitionSpec);
				builder.setTempPath(new org.apache.flink.core.fs.Path(
						toStagingDir(sd.getLocation(), jobConf)));
				builder.setOutputFileConfig(outputFileConfig);
				return dataStream
						.writeUsingOutputFormat(builder.build())
						.setParallelism(dataStream.getParallelism());
			} else {
				org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
				catalogTable.getOptions().forEach(conf::setString);
				HiveRowDataPartitionComputer partComputer = new HiveRowDataPartitionComputer(
						hiveShim,
						jobConf.get(
								HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
								HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal),
						tableSchema.getFieldNames(),
						tableSchema.getFieldDataTypes(),
						partitionColumns);
				TableBucketAssigner assigner = new TableBucketAssigner(partComputer);
				TableRollingPolicy rollingPolicy = new TableRollingPolicy(
						true,
						conf.get(SINK_ROLLING_POLICY_FILE_SIZE),
						conf.get(SINK_ROLLING_POLICY_TIME_INTERVAL).toMillis());
				InactiveBucketListener listener = new InactiveBucketListener();

				Optional<BulkWriter.Factory<RowData>> bulkFactory = createBulkWriterFactory(partitionColumns, sd);
				BucketsBuilder<RowData, ?, ? extends BucketsBuilder<RowData, ?, ?>> builder;
				if (userMrWriter || !bulkFactory.isPresent()) {
					HiveBulkWriterFactory hadoopBulkFactory = new HiveBulkWriterFactory(recordWriterFactory);
					builder = new HadoopPathBasedBulkFormatBuilder<>(
							new Path(sd.getLocation()), hadoopBulkFactory, jobConf, assigner)
							.withRollingPolicy(rollingPolicy)
							.withBucketLifeCycleListener(listener)
							.withOutputFileConfig(outputFileConfig);
					LOG.info("Hive streaming sink: Use MapReduce RecordWriter writer.");
				} else {
					builder = StreamingFileSink.forBulkFormat(
							new org.apache.flink.core.fs.Path(sd.getLocation()),
							new FileSystemTableSink.ProjectionBulkFactory(bulkFactory.get(), partComputer))
							.withBucketAssigner(assigner)
							.withBucketLifeCycleListener(listener)
							.withRollingPolicy(rollingPolicy)
							.withOutputFileConfig(outputFileConfig);
					LOG.info("Hive streaming sink: Use native parquet&orc writer.");
				}
				return FileSystemTableSink.createStreamingSink(
						conf,
						new org.apache.flink.core.fs.Path(sd.getLocation()),
						getPartitionKeys(),
						identifier,
						overwrite,
						dataStream,
						builder,
						listener,
						msFactory);
			}
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

	private Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(String[] partitionColumns,
			StorageDescriptor sd) {
		String serLib = sd.getSerdeInfo().getSerializationLib().toLowerCase();
		int formatFieldCount = tableSchema.getFieldCount() - partitionColumns.length;
		String[] formatNames = new String[formatFieldCount];
		LogicalType[] formatTypes = new LogicalType[formatFieldCount];
		for (int i = 0; i < formatFieldCount; i++) {
			formatNames[i] = tableSchema.getFieldName(i).get();
			formatTypes[i] = tableSchema.getFieldDataType(i).get().getLogicalType();
		}
		RowType formatType = RowType.of(formatTypes, formatNames);
		Configuration formatConf = new Configuration(jobConf);
		sd.getSerdeInfo().getParameters().forEach(formatConf::set);
		if (serLib.contains("parquet")) {
			return Optional.of(ParquetRowDataBuilder.createWriterFactory(
					formatType, formatConf, hiveVersion.startsWith("3.")));
		} else if (serLib.contains("orc")) {
			TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(formatType);
			return Optional.of(hiveShim.createOrcBulkWriterFactory(
					formatConf, typeDescription.toString(), formatTypes));
		} else {
			return Optional.empty();
		}
	}

	@Override
	public DataType getConsumedDataType() {
		DataType dataType = getTableSchema().toRowDataType();
		return isBounded ? dataType : dataType.bridgedTo(RowData.class);
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
		return this;
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

	private List<String> getPartitionKeys() {
		return catalogTable.getPartitionKeys();
	}

	@Override
	public void setStaticPartition(Map<String, String> partitionSpec) {
		// make it a LinkedHashMap to maintain partition column order
		staticPartitionSpec = new LinkedHashMap<>();
		for (String partitionCol : getPartitionKeys()) {
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
