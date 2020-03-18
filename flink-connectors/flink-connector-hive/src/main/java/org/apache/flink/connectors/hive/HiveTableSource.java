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
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.read.HiveTableInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.PartitionableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A TableSource implementation to read data from Hive tables.
 */
public class HiveTableSource implements
		StreamTableSource<BaseRow>,
		PartitionableTableSource,
		ProjectableTableSource<BaseRow>,
		LimitableTableSource<BaseRow> {

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
		return true;
	}

	@Override
	public DataStream<BaseRow> getDataStream(StreamExecutionEnvironment execEnv) {
		List<HiveTablePartition> allHivePartitions = initAllPartitions();

		@SuppressWarnings("unchecked")
		TypeInformation<BaseRow> typeInfo =
				(TypeInformation<BaseRow>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(getProducedDataType());

		HiveTableInputFormat inputFormat = getInputFormat(
				allHivePartitions,
				flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER));

		DataStreamSource<BaseRow> source = execEnv.createInput(inputFormat, typeInfo);

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
		TableSchema fullSchema = getTableSchema();
		DataType type;
		if (projectedFields == null) {
			type = fullSchema.toRowDataType();
		} else {
			String[] fullNames = fullSchema.getFieldNames();
			DataType[] fullTypes = fullSchema.getFieldDataTypes();
			type = TableSchema.builder().fields(
					Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
					Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new))
					.build().toRowDataType();
		}
		return type.bridgedTo(BaseRow.class);
	}

	@Override
	public boolean isLimitPushedDown() {
		return isLimitPushDown;
	}

	@Override
	public TableSource<BaseRow> applyLimit(long limit) {
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
		throw new RuntimeException("This method is not expected to be called. " +
				"Please use Catalog API to retrieve all partitions of a table");
	}

	@Override
	public TableSource<BaseRow> applyPartitionPruning(List<Map<String, String>> remainingPartitions) {
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
	public TableSource<BaseRow> projectFields(int[] fields) {
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
					StorageDescriptor sd = partition.getSd();
					Map<String, Object> partitionColValues = new HashMap<>();
					for (int i = 0; i < partitionColNames.size(); i++) {
						String partitionColName = partitionColNames.get(i);
						String partitionValue = partition.getValues().get(i);
						DataType type = catalogTable.getSchema().getFieldDataType(partitionColName).get();
						Object partitionObject;
						if (defaultPartitionName.equals(partitionValue)) {
							LogicalTypeRoot typeRoot = type.getLogicalType().getTypeRoot();
							// while this is inline with Hive, seems it should be null for string columns as well
							partitionObject = typeRoot == LogicalTypeRoot.CHAR || typeRoot == LogicalTypeRoot.VARCHAR ? defaultPartitionName : null;
						} else {
							partitionObject = restorePartitionValueFromFromType(partitionValue, type);
						}
						partitionColValues.put(partitionColName, partitionObject);
					}
					HiveTablePartition hiveTablePartition = new HiveTablePartition(sd, partitionColValues);
					allHivePartitions.add(hiveTablePartition);
				}
			} else {
				allHivePartitions.add(new HiveTablePartition(client.getTable(dbName, tableName).getSd()));
			}
		} catch (TException e) {
			throw new FlinkHiveException("Failed to collect all partitions from hive metaStore", e);
		}
		return allHivePartitions;
	}

	private static List<String> partitionSpecToValues(Map<String, String> spec, List<String> partitionColNames) {
		Preconditions.checkArgument(spec.size() == partitionColNames.size() && spec.keySet().containsAll(partitionColNames),
				"Partition spec (%s) and partition column names (%s) doesn't match", spec, partitionColNames);
		return partitionColNames.stream().map(spec::get).collect(Collectors.toList());
	}

	private Object restorePartitionValueFromFromType(String valStr, DataType type) {
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
						hiveShim.toHiveDate(Date.valueOf(valStr)),
						hiveShim);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return HiveInspectors.toFlinkObject(
						HiveInspectors.getObjectInspector(type),
						hiveShim.toHiveTimestamp(Timestamp.valueOf(valStr)),
						hiveShim);
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
}
