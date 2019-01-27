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

package org.apache.flink.streaming.connectors.hive;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.catalog.hive.FlinkHiveException;
import org.apache.flink.table.catalog.hive.HiveMetadataUtil;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.Partition;
import org.apache.flink.table.sources.PartitionableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.util.TableSchemaUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;

import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.DEFAULT_LIST_COLUMN_TYPES_SEPARATOR;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_COMPRESSED;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_INPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_LOCATION;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_NUM_BUCKETS;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_OUTPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_SERDE_LIBRARY;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Hive table source class use BaseRow as inner implementation.
 */
public class HiveTableSource extends PartitionableTableSource implements BatchTableSource<BaseRow> {
	private static Logger logger = LoggerFactory.getLogger(HiveTableSource.class);
	private RowTypeInfo rowTypeInfo;
	private String hiveRowTypeString;
	private JobConf jobConf;
	private TableStats tableStats;
	private String dbName;
	private String tableName;
	private Boolean isPartitionTable;
	private String[] partitionColNames;
	private boolean isFilterPushDown = false;
	private boolean isPartitionPruned = false;
	private final List<Partition> prunedPartitions;
	private List<Partition> allPartitions;

	@Override
	public List<Partition> getAllPartitions() {
		return allPartitions;
	}

	@Override
	public String[] getPartitionFieldNames() {
		if (!isPartitionTable) {
			return new String[0];
		} else {
			return partitionColNames;
		}
	}

	@Override
	public TypeInformation<?>[] getPartitionFieldTypes() {
		if (!isPartitionTable) {
			return new TypeInformation[0];
		} else {
			return Arrays.asList(partitionColNames).stream()
						.map(s -> rowTypeInfo.getTypeAt(s))
						.collect(Collectors.toList())
						.toArray(new TypeInformation[partitionColNames.length]);
		}
	}

	@Override
	public List<Partition> getPrunedPartitions() {
		//This function seems useless?
		return prunedPartitions;
	}

	@Override
	public boolean isPartitionPruned() {
		return isPartitionPruned;
	}

	@Override
	public boolean supportDropPartitionPredicate() {
		return true;
	}

	@Override
	public TableSource applyPrunedPartitionsAndPredicate(
			boolean partitionPruned, List<Partition> prunedPartitionList, List<Expression> predicates) {
		return new HiveTableSource(rowTypeInfo, hiveRowTypeString, jobConf, tableStats, dbName, tableName,
								partitionColNames, true, partitionPruned, allPartitions, prunedPartitionList);
	}

	@Override
	public boolean isFilterPushedDown() {
		return isFilterPushDown;
	}

	private void initAllPartitions() {
		allPartitions = new ArrayList<>();
		if (isPartitionTable) {
			HiveConf hiveConf = new HiveConf();
			hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, jobConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
			IMetaStoreClient client = null;
			try {
				client = RetryingMetaStoreClient.getProxy(hiveConf,
																		null,
																		null,
																		HiveMetaStoreClient.class.getName(),
																		true);
				List<org.apache.hadoop.hive.metastore.api.Partition> partitions = client.listPartitions(dbName, tableName, (short) -1);
				for (org.apache.hadoop.hive.metastore.api.Partition partition: partitions){
					StorageDescriptor sd = partition.getSd();
					Map<String, Object> partitionColValues = new HashMap<>();
					for (int i = 0; i < partitionColNames.length; i++) {
						partitionColValues.put(partitionColNames[i], partition.getValues().get(i));
					}
					allPartitions.add(
							new HiveTablePartition(sd.getInputFormat(),
												sd.getOutputFormat(),
												sd.getSerdeInfo().getSerializationLib(),
												sd.getLocation(),
												createPropertiesFromSdParameters(sd),
												partitionColValues));
				}
			} catch (Exception e) {
				throw new FlinkHiveException("Failed creating Hive metaStore client", e);
			} finally {
				if (null != client) {
					client.close();
				}
			}
		} else {
			// TODO: we should get StorageDescriptor from Hive Metastore somehow.
			StorageDescriptor sd = createStorageDescriptor(jobConf, rowTypeInfo);
			jobConf.setStrings(INPUT_DIR, sd.getLocation());
			Properties properties = createPropertiesFromSdParameters(sd);
			allPartitions.add(new HiveTablePartition(jobConf.get(HIVE_TABLE_INPUT_FORMAT),
													jobConf.get(HIVE_TABLE_OUTPUT_FORMAT),
													jobConf.get(HIVE_TABLE_SERDE_LIBRARY),
													jobConf.get(HIVE_TABLE_LOCATION),
													properties,
													null));
		}
	}

	public HiveTableSource(RowTypeInfo rowTypeInfo,
						String hiveRowTypeString, // the string representations of original Hive types
						JobConf jobConf,
						TableStats tableStats,
						String dbName,
						String tableName,
						String[] partitionColNames) {
		this.rowTypeInfo = rowTypeInfo;
		this.hiveRowTypeString = hiveRowTypeString;
		this.jobConf = jobConf;
		this.tableStats = tableStats;
		this.dbName = dbName;
		this.tableName = tableName;
		this.isPartitionTable = (null != partitionColNames && partitionColNames.length != 0);
		this.partitionColNames = partitionColNames;
		this.prunedPartitions = null;
		initAllPartitions();
	}

	public HiveTableSource(RowTypeInfo rowTypeInfo,
						String hiveRowTypeString,
						JobConf jobConf,
						TableStats tableStats,
						String dbName,
						String tableName,
						String[] partitionColNames,
						Boolean isFilterPushDown,
						Boolean isPartitionPruned,
						List<Partition> allPartitions,
						List<Partition> prunedPartitions) {
		this.rowTypeInfo = rowTypeInfo;
		this.hiveRowTypeString = hiveRowTypeString;
		this.jobConf = jobConf;
		this.tableStats = tableStats;
		this.dbName = dbName;
		this.tableName = tableName;
		this.isPartitionTable = (null != partitionColNames && partitionColNames.length != 0);
		this.partitionColNames = partitionColNames;
		this.isFilterPushDown = isFilterPushDown;
		this.isPartitionPruned = isPartitionPruned;
		if (null != prunedPartitions && prunedPartitions.size() != 0) {
			this.allPartitions = prunedPartitions;
		} else {
			this.allPartitions = allPartitions;
		}
		this.prunedPartitions = prunedPartitions;
	}

	@Override
	public DataStream<BaseRow> getBoundedStream(StreamExecutionEnvironment streamEnv) {
		try {
			List<Partition> partitionList;
			if (null == prunedPartitions || prunedPartitions.size() == 0){
				partitionList = allPartitions;
			} else {
				partitionList = prunedPartitions;
			}
			return streamEnv.createInput(
					new HiveTableInputFormat.Builder(rowTypeInfo, jobConf, dbName, tableName, isPartitionTable,
													partitionColNames, partitionList).build()).name(explainSource());
		} catch (Exception e){
			logger.error("Can not normally create hiveTableInputFormat !", e);
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataType getReturnType() {
		return TypeConverters.createInternalTypeFromTypeInfo(new BaseRowTypeInfo(
					rowTypeInfo.getFieldTypes(),
					rowTypeInfo.getFieldNames()));
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchemaUtil.fromDataType(getReturnType(), Option.empty());
	}

	@Override
	public String explainSource() {
		return "hive-table-source" + ": isPartitionPrune:" + String.valueOf(isPartitionPruned)
				+ " isFilterPushDown:" + String.valueOf(isFilterPushDown);
	}

	@Override
	public TableStats getTableStats() {
		return tableStats;
	}

	private static StorageDescriptor createStorageDescriptor(JobConf jobConf, RowTypeInfo rowTypeInfo) {
		StorageDescriptor storageDescriptor = new StorageDescriptor();
		storageDescriptor.setLocation(jobConf.get(HIVE_TABLE_LOCATION));
		storageDescriptor.setInputFormat(jobConf.get(HIVE_TABLE_INPUT_FORMAT));
		storageDescriptor.setOutputFormat(jobConf.get(HIVE_TABLE_OUTPUT_FORMAT));
		storageDescriptor.setCompressed(Boolean.parseBoolean(jobConf.get(HIVE_TABLE_COMPRESSED)));
		storageDescriptor.setNumBuckets(Integer.parseInt(jobConf.get(HIVE_TABLE_NUM_BUCKETS)));

		SerDeInfo serDeInfo = new SerDeInfo();
		serDeInfo.setSerializationLib(jobConf.get(HIVE_TABLE_SERDE_LIBRARY));
		Map<String, String> parameters = new HashMap<>();
		parameters.put(serdeConstants.SERIALIZATION_FORMAT, jobConf.get(HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT));
		serDeInfo.setParameters(parameters);
		List<FieldSchema> fieldSchemas = new ArrayList<>();
		for (int i = 0; i < rowTypeInfo.getArity(); i++) {
			String hiveType = HiveMetadataUtil.convert(TypeConverters.createInternalTypeFromTypeInfo(rowTypeInfo.getFieldTypes()[i]));
			if (null == hiveType) {
				logger.error("Now we don't support flink type of " + rowTypeInfo.getFieldTypes()[i]
							+ " converting from hive");
				throw new FlinkHiveException("Now we don't support flink's type of "
											+ rowTypeInfo.getFieldTypes()[i] + " converting from hive");
			}
			fieldSchemas.add(
					new FieldSchema(rowTypeInfo.getFieldNames()[i], hiveType, ""));
		}
		storageDescriptor.setCols(fieldSchemas);
		storageDescriptor.setSerdeInfo(serDeInfo);
		return storageDescriptor;
	}

	private Properties createPropertiesFromSdParameters(StorageDescriptor storageDescriptor) {
		SerDeInfo serDeInfo = storageDescriptor.getSerdeInfo();
		Map<String, String> parameters = serDeInfo.getParameters();
		Properties properties = new Properties();
		properties.setProperty(serdeConstants.SERIALIZATION_FORMAT,
							serDeInfo.getParameters().get(serdeConstants.SERIALIZATION_FORMAT));
		List<String> colTypes = new ArrayList<>();
		List<String> colNames = new ArrayList<>();
		List<FieldSchema> cols = storageDescriptor.getCols();
		for (FieldSchema col: cols){
			colTypes.add(col.getType());
			colNames.add(col.getName());
		}
		properties.setProperty(serdeConstants.LIST_COLUMNS, StringUtils.join(colNames, ","));
		properties.setProperty(serdeConstants.COLUMN_NAME_DELIMITER, ",");
		properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, StringUtils.join(colTypes, DEFAULT_LIST_COLUMN_TYPES_SEPARATOR));
		properties.setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
		properties.putAll(parameters);
		return properties;
	}
}
