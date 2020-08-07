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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.read.HiveContinuousMonitoringFunction;
import org.apache.flink.connectors.hive.read.HiveTableFileInputFormat;
import org.apache.flink.connectors.hive.read.HiveTableInputFormat;
import org.apache.flink.connectors.hive.read.TimestampedHiveInputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperatorFactory;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.filesystem.FileSystemLookupFunction;
import org.apache.flink.table.filesystem.FileSystemOptions;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.PartitionableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.filesystem.DefaultPartTimeExtractor.toLocalDateTime;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_CONSUME_ORDER;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_CONSUME_START_OFFSET;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_MONITOR_INTERVAL;

/**
 * A TableSource implementation to read data from Hive tables.
 */
public class HiveTableSource implements
		StreamTableSource<RowData>,
		PartitionableTableSource,
		ProjectableTableSource<RowData>,
		LimitableTableSource<RowData>,
		LookupableTableSource<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(HiveTableSource.class);

	private final JobConf jobConf;
	private final ReadableConfig flinkConf;
	private final ObjectPath tablePath;
	private final CatalogTable catalogTable;
	// Remaining partition specs after partition pruning is performed. Null if pruning is not pushed down.
	@Nullable
	private List<Map<String, String>> remainingPartitions = null;
	private String hiveVersion;
	private HiveShim hiveShim;
	private boolean partitionPruned;
	private int[] projectedFields;
	private boolean isLimitPushDown = false;
	private long limit = -1L;
	private Duration hiveTableCacheTTL;

	public HiveTableSource(
			JobConf jobConf, ReadableConfig flinkConf, ObjectPath tablePath, CatalogTable catalogTable) {
		this.jobConf = Preconditions.checkNotNull(jobConf);
		this.flinkConf = Preconditions.checkNotNull(flinkConf);
		this.tablePath = Preconditions.checkNotNull(tablePath);
		this.catalogTable = Preconditions.checkNotNull(catalogTable);
		this.hiveVersion = Preconditions.checkNotNull(jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION),
				"Hive version is not defined");
		hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
		partitionPruned = false;
	}

	// A constructor mainly used to create copies during optimizations like partition pruning and projection push down.
	private HiveTableSource(
			JobConf jobConf,
			ReadableConfig flinkConf,
			ObjectPath tablePath,
			CatalogTable catalogTable,
			List<Map<String, String>> remainingPartitions,
			String hiveVersion,
			boolean partitionPruned,
			int[] projectedFields,
			boolean isLimitPushDown,
			long limit) {
		this.jobConf = Preconditions.checkNotNull(jobConf);
		this.flinkConf = Preconditions.checkNotNull(flinkConf);
		this.tablePath = Preconditions.checkNotNull(tablePath);
		this.catalogTable = Preconditions.checkNotNull(catalogTable);
		this.remainingPartitions = remainingPartitions;
		this.hiveVersion = hiveVersion;
		hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
		this.partitionPruned = partitionPruned;
		this.projectedFields = projectedFields;
		this.isLimitPushDown = isLimitPushDown;
		this.limit = limit;
	}

	@Override
	public boolean isBounded() {
		return !isStreamingSource();
	}

	@Override
	public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
		List<HiveTablePartition> allHivePartitions = initAllPartitions();

		@SuppressWarnings("unchecked")
		TypeInformation<RowData> typeInfo =
				(TypeInformation<RowData>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(getProducedDataType());

		HiveTableInputFormat inputFormat = getInputFormat(
				allHivePartitions,
				flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER));

		if (isStreamingSource()) {
			if (catalogTable.getPartitionKeys().isEmpty()) {
				return createStreamSourceForNonPartitionTable(execEnv, typeInfo, inputFormat, allHivePartitions.get(0));
			} else {
				return createStreamSourceForPartitionTable(execEnv, typeInfo, inputFormat);
			}
		} else {
			return createBatchSource(execEnv, typeInfo, inputFormat);
		}
	}

	private boolean isStreamingSource() {
		return Boolean.parseBoolean(catalogTable.getOptions().getOrDefault(
				STREAMING_SOURCE_ENABLE.key(),
				STREAMING_SOURCE_ENABLE.defaultValue().toString()));
	}

	private DataStream<RowData> createBatchSource(StreamExecutionEnvironment execEnv,
			TypeInformation<RowData> typeInfo, HiveTableInputFormat inputFormat) {
		DataStreamSource<RowData> source = execEnv.createInput(inputFormat, typeInfo);

		int parallelism = flinkConf.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM);
		if (flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM)) {
			int max = flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX);
			if (max < 1) {
				throw new IllegalConfigurationException(
						HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX.key() +
								" cannot be less than 1");
			}

			int splitNum;
			try {
				long nano1 = System.nanoTime();
				splitNum = inputFormat.createInputSplits(0).length;
				long nano2 = System.nanoTime();
				LOG.info(
						"Hive source({}}) createInputSplits use time: {} ms",
						tablePath,
						(nano2 - nano1) / 1_000_000);
			} catch (IOException e) {
				throw new FlinkHiveException(e);
			}
			parallelism = Math.min(splitNum, max);
		}
		parallelism = limit > 0 ? Math.min(parallelism, (int) limit / 1000) : parallelism;
		parallelism = Math.max(1, parallelism);
		source.setParallelism(parallelism);
		return source.name(explainSource());
	}

	private DataStream<RowData> createStreamSourceForPartitionTable(
			StreamExecutionEnvironment execEnv,
			TypeInformation<RowData> typeInfo,
			HiveTableInputFormat inputFormat) {
		Configuration configuration = new Configuration();
		catalogTable.getOptions().forEach(configuration::setString);

		String consumeOrderStr = configuration.get(STREAMING_SOURCE_CONSUME_ORDER);
		ConsumeOrder consumeOrder = ConsumeOrder.getConsumeOrder(consumeOrderStr);
		String consumeOffset = configuration.get(STREAMING_SOURCE_CONSUME_START_OFFSET);
		String extractorKind = configuration.get(PARTITION_TIME_EXTRACTOR_KIND);
		String extractorClass = configuration.get(PARTITION_TIME_EXTRACTOR_CLASS);
		String extractorPattern = configuration.get(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);
		Duration monitorInterval = configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL);

		HiveContinuousMonitoringFunction monitoringFunction = new HiveContinuousMonitoringFunction(
				hiveShim,
				jobConf,
				tablePath,
				catalogTable,
				execEnv.getParallelism(),
				consumeOrder,
				consumeOffset,
				extractorKind,
				extractorClass,
				extractorPattern,
				monitorInterval.toMillis());

		ContinuousFileReaderOperatorFactory<RowData, TimestampedHiveInputSplit> factory =
				new ContinuousFileReaderOperatorFactory<>(inputFormat);

		String sourceName = "HiveMonitoringFunction";
		SingleOutputStreamOperator<RowData> source = execEnv
				.addSource(monitoringFunction, sourceName)
				.transform("Split Reader: " + sourceName, typeInfo, factory);

		return new DataStreamSource<>(source);
	}

	private DataStream<RowData> createStreamSourceForNonPartitionTable(
			StreamExecutionEnvironment execEnv,
			TypeInformation<RowData> typeInfo,
			HiveTableInputFormat inputFormat,
			HiveTablePartition hiveTable) {
		HiveTableFileInputFormat fileInputFormat = new HiveTableFileInputFormat(inputFormat, hiveTable);

		Configuration configuration = new Configuration();
		catalogTable.getOptions().forEach(configuration::setString);
		String consumeOrderStr = configuration.get(STREAMING_SOURCE_CONSUME_ORDER);
		ConsumeOrder consumeOrder = ConsumeOrder.getConsumeOrder(consumeOrderStr);
		if (consumeOrder != ConsumeOrder.CREATE_TIME_ORDER) {
			throw new UnsupportedOperationException(
					"Only " + ConsumeOrder.CREATE_TIME_ORDER + " is supported for non partition table.");
		}

		String consumeOffset = configuration.get(STREAMING_SOURCE_CONSUME_START_OFFSET);
		// to Local zone mills instead of UTC mills
		long currentReadTime = TimestampData.fromLocalDateTime(toLocalDateTime(consumeOffset))
				.toTimestamp().getTime();

		Duration monitorInterval = configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL);

		ContinuousFileMonitoringFunction<RowData> monitoringFunction =
				new ContinuousFileMonitoringFunction<>(
						fileInputFormat,
						FileProcessingMode.PROCESS_CONTINUOUSLY,
						execEnv.getParallelism(),
						monitorInterval.toMillis(),
						currentReadTime);

		ContinuousFileReaderOperatorFactory<RowData, TimestampedFileInputSplit> factory =
				new ContinuousFileReaderOperatorFactory<>(fileInputFormat);

		String sourceName = "HiveFileMonitoringFunction";
		SingleOutputStreamOperator<RowData> source = execEnv.addSource(monitoringFunction, sourceName)
				.transform("Split Reader: " + sourceName, typeInfo, factory);

		return new DataStreamSource<>(source);
	}

	@VisibleForTesting
	HiveTableInputFormat getInputFormat(List<HiveTablePartition> allHivePartitions, boolean useMapRedReader) {
		return new HiveTableInputFormat(
				jobConf,
				catalogTable,
				allHivePartitions,
				projectedFields,
				limit,
				hiveVersion,
				useMapRedReader);
	}

	@Override
	public TableSchema getTableSchema() {
		return catalogTable.getSchema();
	}

	@Override
	public DataType getProducedDataType() {
		return getProducedTableSchema().toRowDataType().bridgedTo(RowData.class);
	}

	private TableSchema getProducedTableSchema() {
		TableSchema fullSchema = getTableSchema();
		if (projectedFields == null) {
			return fullSchema;
		} else {
			String[] fullNames = fullSchema.getFieldNames();
			DataType[] fullTypes = fullSchema.getFieldDataTypes();
			return TableSchema.builder().fields(
					Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
					Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new)).build();
		}
	}

	@Override
	public boolean isLimitPushedDown() {
		return isLimitPushDown;
	}

	@Override
	public TableSource<RowData> applyLimit(long limit) {
		return new HiveTableSource(
				jobConf,
				flinkConf,
				tablePath,
				catalogTable,
				remainingPartitions,
				hiveVersion,
				partitionPruned,
				projectedFields,
				true,
				limit);
	}

	@Override
	public List<Map<String, String>> getPartitions() {
		throw new UnsupportedOperationException(
				"Please use Catalog API to retrieve all partitions of a table");
	}

	@Override
	public TableSource<RowData> applyPartitionPruning(List<Map<String, String>> remainingPartitions) {
		if (catalogTable.getPartitionKeys() == null || catalogTable.getPartitionKeys().size() == 0) {
			return this;
		} else {
			return new HiveTableSource(
					jobConf,
					flinkConf,
					tablePath,
					catalogTable,
					remainingPartitions,
					hiveVersion,
					true,
					projectedFields,
					isLimitPushDown,
					limit);
		}
	}

	@Override
	public TableSource<RowData> projectFields(int[] fields) {
		return new HiveTableSource(
				jobConf,
				flinkConf,
				tablePath,
				catalogTable,
				remainingPartitions,
				hiveVersion,
				partitionPruned,
				fields,
				isLimitPushDown,
				limit);
	}

	private List<HiveTablePartition> initAllPartitions() {
		List<HiveTablePartition> allHivePartitions = new ArrayList<>();
		// Please note that the following directly accesses Hive metastore, which is only a temporary workaround.
		// Ideally, we need to go thru Catalog API to get all info we need here, which requires some major
		// refactoring. We will postpone this until we merge Blink to Flink.
		try (HiveMetastoreClientWrapper client = HiveMetastoreClientFactory.create(new HiveConf(jobConf, HiveConf.class), hiveVersion)) {
			String dbName = tablePath.getDatabaseName();
			String tableName = tablePath.getObjectName();
			List<String> partitionColNames = catalogTable.getPartitionKeys();
			Table hiveTable = client.getTable(dbName, tableName);
			Properties tableProps = HiveReflectionUtils.getTableMetadata(hiveShim, hiveTable);
			String ttlStr = tableProps.getProperty(FileSystemOptions.LOOKUP_JOIN_CACHE_TTL.key());
			hiveTableCacheTTL = ttlStr != null ?
					TimeUtils.parseDuration(ttlStr) :
					FileSystemOptions.LOOKUP_JOIN_CACHE_TTL.defaultValue();
			if (partitionColNames != null && partitionColNames.size() > 0) {
				final String defaultPartitionName = jobConf.get(HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
						HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);
				List<Partition> partitions = new ArrayList<>();
				if (remainingPartitions != null) {
					for (Map<String, String> spec : remainingPartitions) {
						partitions.add(client.getPartition(dbName, tableName, partitionSpecToValues(spec, partitionColNames)));
					}
				} else {
					partitions.addAll(client.listPartitions(dbName, tableName, (short) -1));
				}
				for (Partition partition : partitions) {
					HiveTablePartition hiveTablePartition = toHiveTablePartition(
							catalogTable.getPartitionKeys(),
							catalogTable.getSchema().getFieldNames(),
							catalogTable.getSchema().getFieldDataTypes(),
							hiveShim,
							tableProps,
							defaultPartitionName,
							partition);
					allHivePartitions.add(hiveTablePartition);
				}
			} else {
				allHivePartitions.add(new HiveTablePartition(hiveTable.getSd(), tableProps));
			}
		} catch (TException e) {
			throw new FlinkHiveException("Failed to collect all partitions from hive metaStore", e);
		}
		return allHivePartitions;
	}

	public static HiveTablePartition toHiveTablePartition(
			List<String> partitionKeys,
			String[] fieldNames,
			DataType[] fieldTypes,
			HiveShim shim,
			Properties tableProps,
			String defaultPartitionName,
			Partition partition) {
		StorageDescriptor sd = partition.getSd();
		Map<String, Object> partitionColValues = new HashMap<>();
		List<String> nameList = Arrays.asList(fieldNames);
		for (int i = 0; i < partitionKeys.size(); i++) {
			String partitionColName = partitionKeys.get(i);
			String partitionValue = partition.getValues().get(i);
			DataType type = fieldTypes[nameList.indexOf(partitionColName)];
			Object partitionObject;
			if (defaultPartitionName.equals(partitionValue)) {
				LogicalTypeRoot typeRoot = type.getLogicalType().getTypeRoot();
				// while this is inline with Hive, seems it should be null for string columns as well
				partitionObject = typeRoot == LogicalTypeRoot.CHAR || typeRoot == LogicalTypeRoot.VARCHAR ? defaultPartitionName : null;
			} else {
				partitionObject = restorePartitionValueFromFromType(shim, partitionValue, type);
			}
			partitionColValues.put(partitionColName, partitionObject);
		}
		return new HiveTablePartition(sd, partitionColValues, tableProps);
	}

	private static List<String> partitionSpecToValues(Map<String, String> spec, List<String> partitionColNames) {
		Preconditions.checkArgument(spec.size() == partitionColNames.size() && spec.keySet().containsAll(partitionColNames),
				"Partition spec (%s) and partition column names (%s) doesn't match", spec, partitionColNames);
		return partitionColNames.stream().map(spec::get).collect(Collectors.toList());
	}

	private static Object restorePartitionValueFromFromType(HiveShim shim, String valStr, DataType type) {
		LogicalTypeRoot typeRoot = type.getLogicalType().getTypeRoot();
		//note: it's not a complete list ofr partition key types that Hive support, we may need add more later.
		switch (typeRoot) {
			case CHAR:
			case VARCHAR:
				return valStr;
			case BOOLEAN:
				return Boolean.parseBoolean(valStr);
			case TINYINT:
				return Integer.valueOf(valStr).byteValue();
			case SMALLINT:
				return Short.valueOf(valStr);
			case INTEGER:
				return Integer.valueOf(valStr);
			case BIGINT:
				return Long.valueOf(valStr);
			case FLOAT:
				return Float.valueOf(valStr);
			case DOUBLE:
				return Double.valueOf(valStr);
			case DATE:
				return HiveInspectors.toFlinkObject(
						HiveInspectors.getObjectInspector(type),
						shim.toHiveDate(Date.valueOf(valStr)),
						shim);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return HiveInspectors.toFlinkObject(
						HiveInspectors.getObjectInspector(type),
						shim.toHiveTimestamp(Timestamp.valueOf(valStr)),
						shim);
			default:
				break;
		}
		throw new FlinkHiveException(
				new IllegalArgumentException(String.format("Can not convert %s to type %s for partition value", valStr, type)));
	}

	@Override
	public String explainSource() {
		String explain = String.format(" TablePath: %s, PartitionPruned: %s, PartitionNums: %d",
				tablePath.getFullName(), partitionPruned, null == remainingPartitions ? null : remainingPartitions.size());
		if (projectedFields != null) {
			explain += ", ProjectedFields: " + Arrays.toString(projectedFields);
		}
		if (isLimitPushDown) {
			explain += String.format(", LimitPushDown %s, Limit %d", isLimitPushDown, limit);
		}
		return TableConnectorUtils.generateRuntimeName(getClass(), getTableSchema().getFieldNames()) + explain;
	}

	@Override
	public TableFunction<RowData> getLookupFunction(String[] lookupKeys) {
		List<HiveTablePartition> allPartitions = initAllPartitions();
		TableSchema producedSchema = getProducedTableSchema();
		return new FileSystemLookupFunction<>(
				getInputFormat(allPartitions, flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER)),
				lookupKeys,
				producedSchema.getFieldNames(),
				producedSchema.getFieldDataTypes(),
				hiveTableCacheTTL
		);
	}

	@Override
	public AsyncTableFunction<RowData> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("Hive table doesn't support async lookup");
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}
}
