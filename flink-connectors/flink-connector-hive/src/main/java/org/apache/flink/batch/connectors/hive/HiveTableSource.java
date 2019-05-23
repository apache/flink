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

import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.getActualObjectFromString;

/**
 * A TableSource to read Hive tables.
 */
public class HiveTableSource implements BatchTableSource<BaseRow> {
	private static Logger logger = LoggerFactory.getLogger(HiveTableSource.class);

	private TableSchema tableSchema;
	private String hiveRowTypeString;
	private JobConf jobConf;
	private String dbName;
	private String tableName;
	private Boolean isPartitionTable;
	private String[] partitionColNames;
	private List<HiveTablePartition> allPartitions;

	public HiveTableSource(TableSchema tableSchema,
						String hiveRowTypeString, // the string representations of original Hive types
						JobConf jobConf,
						String dbName,
						String tableName,
						String[] partitionColNames) {
		this.tableSchema = tableSchema;
		this.hiveRowTypeString = hiveRowTypeString;
		this.jobConf = jobConf;
		this.dbName = dbName;
		this.tableName = tableName;
		this.isPartitionTable = (null != partitionColNames && partitionColNames.length != 0);
		this.partitionColNames = partitionColNames;
	}

	@Override
	public DataStream getBoundedStream(StreamExecutionEnvironment streamEnv) {
		initAllPartitions();
		return streamEnv.createInput(
				new HiveTableInputFormat.Builder((BaseRowTypeInfo) getReturnType(), jobConf, dbName, tableName,
												isPartitionTable, partitionColNames, allPartitions).build());
	}

	@Override
	public TypeInformation getReturnType() {
		InternalType[] internalTypes = (InternalType[]) Arrays.asList(tableSchema.getFieldTypes()).stream()
											.map(t -> TypeConverters.createInternalTypeFromTypeInfo(t))
											.toArray();
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
						Object partitionObject = getActualObjectFromString(partitionValue, clazz);
						partitionColValues.put(partitionColNames[i], partitionObject);
					}
					allPartitions.add(new HiveTablePartition(sd, partitionColValues));
				}
			} else {
				allPartitions.add(new HiveTablePartition(client.getTable(dbName, tableName).getSd(), null));
			}
		} catch (Exception e) {
			logger.error("Failed to collect all partitions from hive metaStore", e);
			throw new RuntimeException("Failed to collect all partitions from hive metaStore", e);
		}
	}

	private static IMetaStoreClient getMetastoreClient(JobConf jobConf) {
		try {
			HiveConf hiveConf = new HiveConf();
			hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, jobConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
			return RetryingMetaStoreClient.getProxy(
					hiveConf,
					null,
					null,
					HiveMetaStoreClient.class.getName(),
					true);
		} catch (MetaException e) {
			logger.error("Failed to collect all partitions from hive metaStore", e);
			throw new RuntimeException("Failed to collect all partitions from hive metaStore", e);
		}
	}
}
