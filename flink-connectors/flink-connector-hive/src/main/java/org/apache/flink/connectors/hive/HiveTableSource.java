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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.connectors.hive.read.HiveTableInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.PartitionableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A TableSource implementation to read data from Hive tables.
 */
public class HiveTableSource extends InputFormatTableSource<BaseRow>
		implements PartitionableTableSource, ProjectableTableSource<BaseRow>, LimitableTableSource<BaseRow> {

	private final JobConf jobConf;
	private final ObjectPath tablePath;
	private final CatalogTable catalogTable;
	private List<HiveTablePartition> allHivePartitions;
	private String hiveVersion;
	//partitionList represent all partitions in map list format used in partition-pruning situation.
	private List<Map<String, String>> partitionList = new ArrayList<>();
	private Map<Map<String, String>, HiveTablePartition> partitionSpec2HiveTablePartition = new HashMap<>();
	private boolean initAllPartitions;
	private boolean partitionPruned;
	private int[] projectedFields;
	private boolean isLimitPushDown = false;
	private long limit = -1L;

	public HiveTableSource(JobConf jobConf, ObjectPath tablePath, CatalogTable catalogTable) {
		this.jobConf = Preconditions.checkNotNull(jobConf);
		this.tablePath = Preconditions.checkNotNull(tablePath);
		this.catalogTable = Preconditions.checkNotNull(catalogTable);
		this.hiveVersion = Preconditions.checkNotNull(jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION),
				"Hive version is not defined");
		initAllPartitions = false;
		partitionPruned = false;
	}

	// A constructor mainly used to create copies during optimizations like partition pruning and projection push down.
	private HiveTableSource(JobConf jobConf, ObjectPath tablePath, CatalogTable catalogTable,
							List<HiveTablePartition> allHivePartitions,
							String hiveVersion,
							List<Map<String, String>> partitionList,
							boolean initAllPartitions,
							boolean partitionPruned,
							int[] projectedFields,
							boolean isLimitPushDown,
							long limit) {
		this.jobConf = Preconditions.checkNotNull(jobConf);
		this.tablePath = Preconditions.checkNotNull(tablePath);
		this.catalogTable = Preconditions.checkNotNull(catalogTable);
		this.allHivePartitions = allHivePartitions;
		this.hiveVersion = hiveVersion;
		this.partitionList = partitionList;
		this.initAllPartitions = initAllPartitions;
		this.partitionPruned = partitionPruned;
		this.projectedFields = projectedFields;
		this.isLimitPushDown = isLimitPushDown;
		this.limit = limit;
	}

	@Override
	public InputFormat<BaseRow, ?> getInputFormat() {
		if (!initAllPartitions) {
			initAllPartitions();
		}
		return new HiveTableInputFormat(jobConf, catalogTable, allHivePartitions, projectedFields, limit, hiveVersion);
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
		return new HiveTableSource(jobConf, tablePath, catalogTable, allHivePartitions, hiveVersion,
						partitionList, initAllPartitions, partitionPruned, projectedFields, true, limit);
	}

	@Override
	public List<Map<String, String>> getPartitions() {
		if (!initAllPartitions) {
			initAllPartitions();
		}
		return partitionList;
	}

	@Override
	public TableSource<BaseRow> applyPartitionPruning(List<Map<String, String>> remainingPartitions) {
		if (catalogTable.getPartitionKeys() == null || catalogTable.getPartitionKeys().size() == 0) {
			return this;
		} else {
			List<HiveTablePartition> remainingHivePartitions = new ArrayList<>();
			for (Map<String, String> partitionSpec : remainingPartitions) {
				HiveTablePartition hiveTablePartition = partitionSpec2HiveTablePartition.get(partitionSpec);
				Preconditions.checkNotNull(hiveTablePartition, String.format("remainingPartitions must contain " +
																			"partition spec %s", partitionSpec));
				remainingHivePartitions.add(hiveTablePartition);
			}
			return new HiveTableSource(jobConf, tablePath, catalogTable, remainingHivePartitions, hiveVersion,
						partitionList, true, true, projectedFields, isLimitPushDown, limit);
		}
	}

	private void initAllPartitions() {
		allHivePartitions = new ArrayList<>();
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
				List<Partition> partitions =
						client.listPartitions(dbName, tableName, (short) -1);
				for (Partition partition : partitions) {
					StorageDescriptor sd = partition.getSd();
					Map<String, Object> partitionColValues = new HashMap<>();
					Map<String, String> partitionSpec = new HashMap<>();
					for (int i = 0; i < partitionColNames.size(); i++) {
						String partitionColName = partitionColNames.get(i);
						String partitionValue = partition.getValues().get(i);
						partitionSpec.put(partitionColName, partitionValue);
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
					partitionList.add(partitionSpec);
					partitionSpec2HiveTablePartition.put(partitionSpec, hiveTablePartition);
				}
			} else {
				allHivePartitions.add(new HiveTablePartition(client.getTable(dbName, tableName).getSd()));
			}
		} catch (TException e) {
			throw new FlinkHiveException("Failed to collect all partitions from hive metaStore", e);
		}
		initAllPartitions = true;
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
				return Date.valueOf(valStr);
			default:
				break;
		}
		throw new FlinkHiveException(
				new IllegalArgumentException(String.format("Can not convert %s to type %s for partition value", valStr, type)));
	}

	@Override
	public String explainSource() {
		String explain = String.format(" TablePath: %s, PartitionPruned: %s, PartitionNums: %d",
				tablePath.getFullName(), partitionPruned, null == allHivePartitions ? 0 : allHivePartitions.size());
		if (projectedFields != null) {
			explain += ", ProjectedFields: " + Arrays.toString(projectedFields);
		}
		if (isLimitPushDown) {
			explain += String.format(", LimitPushDown %s, Limit %d", isLimitPushDown, limit);
		}
		return super.explainSource() + explain;
	}

	@Override
	public TableSource<BaseRow> projectFields(int[] fields) {
		return new HiveTableSource(jobConf, tablePath, catalogTable, allHivePartitions, hiveVersion,
				partitionList, initAllPartitions, partitionPruned, fields, isLimitPushDown, limit);
	}
}
