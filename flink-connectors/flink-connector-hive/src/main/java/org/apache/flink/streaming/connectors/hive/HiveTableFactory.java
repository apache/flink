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
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.catalog.hive.FlinkHiveException;
import org.apache.flink.table.catalog.hive.HiveMetadataUtil;
import org.apache.flink.table.catalog.hive.config.HiveCatalogConfig;
import org.apache.flink.table.catalog.hive.config.HiveTableConfig;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.TableSourceParserFactory;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.util.TableProperties;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.DEFAULT_LIST_COLUMN_TYPES_SEPARATOR;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_DB_NAME;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_FIELD_NAMES;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_FIELD_TYPES;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_PARTITION_FIELDS;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_TABLE_NAME;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * Hive table factory provides for sql to register hive table.
 */
public class HiveTableFactory implements BatchTableSourceFactory<BaseRow>, TableSourceParserFactory {
	private static Logger logger = LoggerFactory.getLogger(HiveTableFactory.class);
	/**
	 * Add this method to make some optimize rule work normally, we should delete it later.
	 */
	@Override
	public TableSourceParser createParser(
			String tableName, RichTableSchema tableSchema, TableProperties properties) {
		return null;
	}

	@Override
	public BatchTableSource<BaseRow> createBatchTableSource(Map<String, String> props) {
		HiveConf hiveConf = new HiveConf();
		TableStats tableStats = null;
		for (Map.Entry<String, String> prop : props.entrySet()) {
			hiveConf.set(prop.getKey(), prop.getValue());
		}
		String[] fieldNames = props.get(HIVE_TABLE_FIELD_NAMES).split(",");
		String hiveRowTypeString = props.get(HIVE_TABLE_FIELD_TYPES);
		String[] hiveFieldTypes = hiveRowTypeString.split(DEFAULT_LIST_COLUMN_TYPES_SEPARATOR);
		InternalType[] colTypes = new InternalType[fieldNames.length];
		TypeInformation[] typeInformations = new TypeInformation[fieldNames.length];
		for (int i = 0; i < hiveFieldTypes.length; i++) {
			colTypes[i] = HiveMetadataUtil.convert(hiveFieldTypes[i]);
			typeInformations[i] = TypeConverters.createExternalTypeInfoFromDataType(colTypes[i]);
		}
		String hiveDbName = props.get(HIVE_TABLE_DB_NAME);
		String hiveTableName = props.get(HIVE_TABLE_TABLE_NAME);
		String partitionFields = props.get(HIVE_TABLE_PARTITION_FIELDS);
		String[] partitionColumns = new String[0];
		if (null != partitionFields && !partitionFields.isEmpty()) {
			partitionColumns = partitionFields.split(",");
		}
		hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, props.get(HiveConf.ConfVars.METASTOREURIS.varname));
		try {
			JobConf jobConf = new JobConf(hiveConf);
			return new HiveTableSource(new RowTypeInfo(typeInformations, fieldNames),
									hiveRowTypeString,
									jobConf,
									tableStats,
									hiveDbName,
									hiveTableName,
									partitionColumns);
		} catch (Exception e){
			logger.error("Error when create hive batch table source ...", e);
			throw new FlinkHiveException(e);
		}
	}

	@Override
	public Map<String, String> requiredContext() {
		HashMap<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, "HIVE");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CONNECTOR_PROPERTY_VERSION);
		properties.add(HIVE_TABLE_TYPE);

		// Hive catalog configs
		properties.add(HiveCatalogConfig.HIVE_METASTORE_USERNAME);

		// Hive table configs
		properties.add(HiveTableConfig.HIVE_TABLE_COMPRESSED);
		properties.add(HiveTableConfig.HIVE_TABLE_INPUT_FORMAT);
		properties.add(HiveTableConfig.HIVE_TABLE_LOCATION);
		properties.add(HiveTableConfig.HIVE_TABLE_NUM_BUCKETS);
		properties.add(HiveTableConfig.HIVE_TABLE_OUTPUT_FORMAT);
		properties.add(HiveTableConfig.HIVE_TABLE_SERDE_LIBRARY);
		properties.add(HiveTableConfig.HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT);
		properties.add(HiveTableConfig.HIVE_TABLE_FIELD_NAMES);
		properties.add(HiveTableConfig.HIVE_TABLE_FIELD_TYPES);

		// Hive table parameters
		properties.add(HiveTableConfig.HIVE_TABLE_PROPERTY_TRANSIENT_LASTDDLTIME);
		properties.add(HiveTableConfig.HIVE_TABLE_PROPERTY_LAST_MODIFIED_TIME);

		// Hive table stats
		properties.add(StatsSetupConst.NUM_FILES);
		properties.add(StatsSetupConst.NUM_PARTITIONS);
		properties.add(StatsSetupConst.TOTAL_SIZE);
		properties.add(StatsSetupConst.RAW_DATA_SIZE);
		properties.add(StatsSetupConst.ROW_COUNT);
		properties.add(StatsSetupConst.COLUMN_STATS_ACCURATE);

		properties.add(HiveTableConfig.HIVE_TABLE_DB_NAME);
		properties.add(HiveTableConfig.HIVE_TABLE_TABLE_NAME);
		properties.add(HiveTableConfig.HIVE_TABLE_PARTITION_FIELDS);
		properties.add(HiveConf.ConfVars.METASTOREURIS.varname);

		return properties;
	}
}
