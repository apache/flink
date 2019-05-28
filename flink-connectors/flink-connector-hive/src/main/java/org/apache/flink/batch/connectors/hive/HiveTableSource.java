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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.TypeConverters;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A TableSource to read Hive tables.
 */
public class HiveTableSource implements BatchTableSource<BaseRow> {
	private static Logger logger = LoggerFactory.getLogger(HiveTableSource.class);

	private final TableSchema tableSchema;
	private final JobConf jobConf;
	private final String dbName;
	private final String tableName;
	private final Boolean isPartitionTable;
	private final String[] partitionColNames;
	private List<HiveTablePartition> allPartitions;

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
	}

	@Override
	public DataStream getBoundedStream(StreamExecutionEnvironment streamEnv) {
		initAllPartitions();
		return streamEnv.createInput(new HiveTableInputFormat(jobConf, isPartitionTable, partitionColNames,
									allPartitions, (BaseRowTypeInfo) getReturnType()));
	}

	@Override
	public TypeInformation getReturnType() {
		InternalType[] internalTypes = Arrays.asList(tableSchema.getFieldTypes()).stream()
											.map(t -> TypeConverters.createInternalTypeFromTypeInfo(t))
											.collect(Collectors.toList()).toArray(new InternalType[0]);
		return new BaseRowTypeInfo(internalTypes, tableSchema.getFieldNames());
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	private void initAllPartitions() {
		allPartitions = new ArrayList<>();
		// Please note that the following directly accesses Hive metastore, which is only a temporary workaround.
		// Ideally, we need to go thru Catalog API to get all info we need here, which requires some major
		// refactoring. We will postpone this until we merge Blink to Flink.
		IMetaStoreClient client = getMetastoreClient(jobConf);
		try {
			if (isPartitionTable) {
				List<org.apache.hadoop.hive.metastore.api.Partition> partitions =
						client.listPartitions(dbName, tableName, (short) -1);
				for (org.apache.hadoop.hive.metastore.api.Partition partition : partitions) {
					StorageDescriptor sd = partition.getSd();
					Map<String, Object> partitionColValues = new HashMap<>();
					for (int i = 0; i < partitionColNames.length; i++) {
						String partitionValue = partition.getValues().get(i);
						Class clazz = tableSchema.getFieldType(partitionColNames[i]).get().getTypeClass();
						Object partitionObject = HiveTableUtil.getActualObjectFromString(partitionValue, clazz);
						partitionColValues.put(partitionColNames[i], partitionObject);
					}
					allPartitions.add(new HiveTablePartition(sd, partitionColValues));
				}
			} else {
				allPartitions.add(new HiveTablePartition(client.getTable(dbName, tableName).getSd(), null));
			}
		} catch (Exception e) {
			logger.error("Failed to collect all partitions from hive metaStore", e);
			throw new FlinkHiveException("Failed to collect all partitions from hive metaStore", e);
		}
	}

	private static IMetaStoreClient getMetastoreClient(JobConf jobConf) {
		try {
			return RetryingMetaStoreClient.getProxy(
					new HiveConf(jobConf, HiveConf.class),
					null,
					null,
					HiveMetaStoreClient.class.getName(),
					true);
		} catch (MetaException e) {
			logger.error("Failed to collect all partitions from hive metaStore", e);
			throw new FlinkHiveException("Failed to collect all partitions from hive metaStore", e);
		}
	}
}
