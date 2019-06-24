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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A TableSource implementation to read data from Hive tables.
 */
public class HiveTableSource extends InputFormatTableSource<Row> {

	private static Logger logger = LoggerFactory.getLogger(HiveTableSource.class);

	private final TableSchema tableSchema;
	private final JobConf jobConf;
	private final String dbName;
	private final String tableName;
	private final Boolean isPartitionTable;
	private final String[] partitionColNames;
	private List<HiveTablePartition> allPartitions;
	private String hiveVersion;

	public HiveTableSource(TableSchema tableSchema,
						JobConf jobConf,
						String dbName,
						String tableName,
						String[] partitionColNames) {
		this.tableSchema = tableSchema;
		this.jobConf = jobConf;
		this.dbName = dbName;
		this.tableName = tableName;
		this.isPartitionTable = (null != partitionColNames && partitionColNames.length != 0);
		this.partitionColNames = partitionColNames;
		this.hiveVersion = jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION, HiveShimLoader.getHiveVersion());
	}

	@Override
	public InputFormat getInputFormat() {
		initAllPartitions();
		return new HiveTableInputFormat(jobConf, isPartitionTable, partitionColNames, allPartitions, new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames()));
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public DataType getProducedDataType() {
		DataTypes.Field[] fields = new DataTypes.Field[tableSchema.getFieldCount()];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = DataTypes.FIELD(tableSchema.getFieldName(i).get(), tableSchema.getFieldDataType(i).get());
		}
		return DataTypes.ROW(fields);
	}

	private void initAllPartitions() {
		allPartitions = new ArrayList<>();
		// Please note that the following directly accesses Hive metastore, which is only a temporary workaround.
		// Ideally, we need to go thru Catalog API to get all info we need here, which requires some major
		// refactoring. We will postpone this until we merge Blink to Flink.
		try (HiveMetastoreClientWrapper client = HiveMetastoreClientFactory.create(new HiveConf(jobConf, HiveConf.class), hiveVersion)) {
			if (isPartitionTable) {
				List<Partition> partitions =
						client.listPartitions(dbName, tableName, (short) -1);
				for (Partition partition : partitions) {
					StorageDescriptor sd = partition.getSd();
					Map<String, Object> partitionColValues = new HashMap<>();
					for (int i = 0; i < partitionColNames.length; i++) {
						String partitionValue = partition.getValues().get(i);
						DataType type = tableSchema.getFieldDataType(partitionColNames[i]).get();
						Object partitionObject = restorePartitionValueFromFromType(partitionValue, type);
						partitionColValues.put(partitionColNames[i], partitionObject);
					}
					allPartitions.add(new HiveTablePartition(sd, partitionColValues));
				}
			} else {
				allPartitions.add(new HiveTablePartition(client.getTable(dbName, tableName).getSd(), null));
			}
		} catch (TException e) {
			throw new FlinkHiveException("Failed to collect all partitions from hive metaStore", e);
		}
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
}
