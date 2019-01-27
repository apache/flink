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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.Column;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.BooleanType;
import org.apache.flink.table.api.types.ByteArrayType;
import org.apache.flink.table.api.types.ByteType;
import org.apache.flink.table.api.types.CharType;
import org.apache.flink.table.api.types.DateType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.FloatType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.config.HiveDbConfig;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.util.PropertiesUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.DEFAULT_LIST_COLUMN_TYPES_SEPARATOR;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_COMPRESSED;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_DB_NAME;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_FIELD_NAMES;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_FIELD_TYPES;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_INPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_LOCATION;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_NUM_BUCKETS;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_OUTPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_PARTITION_FIELDS;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_SERDE_LIBRARY;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_TABLE_NAME;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_TYPE;

/**
 * Utils for meta objects conversion between Flink and Hive.
 */
public class HiveMetadataUtil {
	private static final Logger LOG = LoggerFactory.getLogger(HiveMetadataUtil.class);

	/**
	 * The number of milliseconds in a day.
	 */
	private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	private static final String DECIMAL_TYPE_NAME_FORMAT = "decimal(%d,%d)";

	private HiveMetadataUtil() {
	}

	/**
	 * Create a Hive table from CatalogTable.
	 */
	public static Table createHiveTable(ObjectPath tablePath, CatalogTable table) {
		Properties prop = new Properties();
		prop.putAll(table.getProperties());

		// StorageDescriptor
		StorageDescriptor sd = new StorageDescriptor();
		sd.setInputFormat(prop.getProperty(HIVE_TABLE_INPUT_FORMAT));
		sd.setOutputFormat(prop.getProperty(HIVE_TABLE_OUTPUT_FORMAT));
		sd.setLocation(prop.getProperty(HIVE_TABLE_LOCATION));
		sd.setSerdeInfo(
			new SerDeInfo(
				null,
				prop.getProperty(HIVE_TABLE_SERDE_LIBRARY, LazySimpleSerDe.class.getName()),
				new HashMap<>())
		);
		sd.setCompressed(Boolean.valueOf(prop.getProperty(HIVE_TABLE_COMPRESSED)));
		sd.setParameters(new HashMap<>());
		sd.setNumBuckets(PropertiesUtil.getInt(prop, HIVE_TABLE_NUM_BUCKETS, -1));
		sd.setBucketCols(new ArrayList<>());

		List<FieldSchema> columns = createHiveColumns(table.getTableSchema());

		// Only add non-partitioned columns as Hive table column. Partition columns should be set as partition keys in Hive's table
		sd.setCols(columns.subList(0, columns.size() - table.getPartitionColumnNames().size()));
		sd.setSortCols(new ArrayList<>());

		// Partitions
		List<FieldSchema> partitionKeys = new ArrayList<>();
		if (table.isPartitioned()) {
			partitionKeys = columns.subList(columns.size() - table.getPartitionColumnNames().size(), columns.size());
		}

		Table hiveTable = new Table();
		hiveTable.setSd(sd);
		hiveTable.setPartitionKeys(partitionKeys);
		hiveTable.setTableType(prop.getProperty(HIVE_TABLE_TYPE));
		hiveTable.setDbName(tablePath.getDbName());
		hiveTable.setTableName(tablePath.getObjectName());
		hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

		// Create Hive table doesn't include TableStats
		hiveTable.setParameters(table.getProperties());

		return hiveTable;
	}

	/**
	 * Create Hive columns from Flink TableSchema.
	 */
	private static List<FieldSchema> createHiveColumns(TableSchema schema) {
		List<FieldSchema> columns = new ArrayList<>(schema.getColumns().length);
		for (Column column : schema.getColumns()) {
			FieldSchema fieldSchema = new FieldSchema(column.name(), convert(column.internalType()), null);
			columns.add(fieldSchema);
		}
		return columns;
	}

	/**
	 * Create Flink's TableSchema from Hive columns.
	 */
	public static TableSchema createTableSchema(List<FieldSchema> fieldSchemas, List<FieldSchema> partitionFields) {
		int colSize = fieldSchemas.size() + partitionFields.size();

		String[] colNames = new String[colSize];
		InternalType[] colTypes = new InternalType[colSize];

		for (int i = 0; i < fieldSchemas.size(); i++) {
			FieldSchema fs = fieldSchemas.get(i);

			colNames[i] = fs.getName();
			colTypes[i] = HiveMetadataUtil.convert(fs.getType());
		}
		for (int i = 0; i < colSize - fieldSchemas.size(); i++){
			FieldSchema fs = partitionFields.get(i);

			colNames[i + fieldSchemas.size()] = fs.getName();
			colTypes[i + fieldSchemas.size()] = HiveMetadataUtil.convert(fs.getType());
		}

		return new TableSchema(colNames, colTypes);
	}

	/**
	 * Create an CatalogTable from Hive table.
	 */
	public static CatalogTable createCatalogTable(Table hiveTable, TableSchema tableSchema, TableStats tableStats) {
		return new CatalogTable(
			"hive",
			tableSchema,
			getPropertiesFromHiveTable(hiveTable),
			new RichTableSchema(tableSchema.getFieldNames(), tableSchema.getFieldTypes()),
			tableStats,
			null,
			getPartitionCols(hiveTable),
			hiveTable.getPartitionKeysSize() != 0,
			null,
			null,
			-1L,
			(long) hiveTable.getCreateTime(),
			(long) hiveTable.getLastAccessTime(),
			false);
	}

	/**
	 * Create a map of Flink column stats from the given Hive column stats.
	 */
	public static Map<String, ColumnStats> createColumnStats(List<ColumnStatisticsObj> hiveColStats) {
		Map<String, ColumnStats> colStats = new HashMap<>();

		if (hiveColStats != null) {
			for (ColumnStatisticsObj colStatsObj : hiveColStats) {
				ColumnStats columnStats = createTableColumnStats(convert(colStatsObj.getColType()), colStatsObj.getStatsData());

				if (colStats != null) {
					colStats.put(colStatsObj.getColName(), columnStats);
				}
			}
		}

		return colStats;
	}

	/**
	 * Create a map of Flink column stats from the given Hive column stats.
	 */
	public static ColumnStatistics createColumnStats(Table hiveTable, Map<String, ColumnStats> colStats) {
		ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, hiveTable.getDbName(), hiveTable.getTableName());

		List<ColumnStatisticsObj> colStatsList = new ArrayList<>();

		for (FieldSchema field : hiveTable.getSd().getCols()) {
			String hiveColName = field.getName();
			String hiveColType = field.getType();

			if (colStats.containsKey(hiveColName)) {
				ColumnStats flinkColStat = colStats.get(field.getName());

				ColumnStatisticsData statsData = getColumnStatisticsData(convert(hiveColType), flinkColStat);
				ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj(hiveColName, hiveColType, statsData);
				colStatsList.add(columnStatisticsObj);
			}
		}

		return new ColumnStatistics(desc, colStatsList);
	}

	/**
	 * Convert Flink ColumnStats to Hive ColumnStatisticsData according to Hive column type.
	 * Note we currently assume that, in Flink, the max and min of ColumnStats will be same type as the Flink column type.
	 * For example, for SHORT and Long columns, the max and min of their ColumnStats should be of type SHORT and LONG.
	 */
	private static ColumnStatisticsData getColumnStatisticsData(InternalType colType, ColumnStats colStat) {
		if (colType.equals(CharType.INSTANCE)) {
			return ColumnStatisticsData.stringStats(
				new StringColumnStatsData(colStat.maxLen(), colStat.avgLen(), colStat.nullCount(), colStat.ndv()));
		} else if (colType instanceof DecimalType) {
			DecimalColumnStatsData decimalStats = new DecimalColumnStatsData(colStat.nullCount(), colStat.ndv());

			decimalStats.setHighValue(fromFlinkDecimal((Decimal) colStat.max()));
			decimalStats.setLowValue(fromFlinkDecimal((Decimal) colStat.min()));

			return ColumnStatisticsData.decimalStats(decimalStats);
		} else if (colType.equals(StringType.INSTANCE)) {
			return ColumnStatisticsData.stringStats(
				new StringColumnStatsData(colStat.maxLen(), colStat.avgLen(), colStat.nullCount(), colStat.ndv()));
		} else if (colType.equals(BooleanType.INSTANCE)) {
			BooleanColumnStatsData boolStats = new BooleanColumnStatsData(0, 0, colStat.nullCount());

			return ColumnStatisticsData.booleanStats(boolStats);
		} else if (colType.equals(ByteType.INSTANCE)) {
			LongColumnStatsData tinyint = new LongColumnStatsData(colStat.nullCount(), colStat.ndv());
			tinyint.setHighValue(((Byte) colStat.max()).longValue());
			tinyint.setLowValue(((Byte) colStat.min()).longValue());

			return ColumnStatisticsData.longStats(tinyint);
		} else if (colType.equals(ShortType.INSTANCE)) {
			LongColumnStatsData smallint = new LongColumnStatsData(colStat.nullCount(), colStat.ndv());
			smallint.setHighValue(((Short) colStat.max()).longValue());
			smallint.setLowValue(((Short) colStat.min()).longValue());

			return ColumnStatisticsData.longStats(smallint);
		} else if (colType.equals(IntType.INSTANCE)) {
			LongColumnStatsData integer = new LongColumnStatsData(colStat.nullCount(), colStat.ndv());
			integer.setHighValue(((Number) colStat.max()).longValue());
			integer.setLowValue(((Number) colStat.min()).longValue());

			return ColumnStatisticsData.longStats(integer);
		} else if (colType.equals(LongType.INSTANCE)) {
			LongColumnStatsData longStats = new LongColumnStatsData(colStat.nullCount(), colStat.ndv());
			longStats.setHighValue(((Long) colStat.max()).longValue());
			longStats.setLowValue(((Long) colStat.min()).longValue());

			return ColumnStatisticsData.longStats(longStats);
		} else if (colType instanceof TimestampType) {
			LongColumnStatsData timestampStats = new LongColumnStatsData(colStat.nullCount(), colStat.ndv());
			timestampStats.setHighValue(((Timestamp) colStat.max()).getTime());
			timestampStats.setLowValue(((Timestamp) colStat.min()).getTime());

			return ColumnStatisticsData.longStats(timestampStats);
		} else if (colType.equals(FloatType.INSTANCE)) {
			DoubleColumnStatsData floatStats = new DoubleColumnStatsData(colStat.nullCount(), colStat.ndv());
			floatStats.setHighValue((Float) colStat.max());
			floatStats.setLowValue((Float) colStat.min());

			return ColumnStatisticsData.doubleStats(floatStats);
		} else if (colType.equals(DoubleType.INSTANCE)) {
			DoubleColumnStatsData doubleStats = new DoubleColumnStatsData(colStat.nullCount(), colStat.ndv());
			doubleStats.setHighValue((Double) colStat.max());
			doubleStats.setLowValue((Double) colStat.min());

			return ColumnStatisticsData.doubleStats(doubleStats);
		} else if (colType.equals(DateType.DATE)) {
			DateColumnStatsData dateStats = new DateColumnStatsData(colStat.nullCount(), colStat.ndv());

			dateStats.setHighValue(new org.apache.hadoop.hive.metastore.api.Date(((Date) colStat.max()).getTime() / MILLIS_PER_DAY));
			dateStats.setLowValue(new org.apache.hadoop.hive.metastore.api.Date(((Date) colStat.min()).getTime() / MILLIS_PER_DAY));

			return ColumnStatisticsData.dateStats(dateStats);
		} else {
			LOG.warn("Flink does not support converting ColumnStats '{}' for Hive column type '{}' yet.", colStat, colType);
			return new ColumnStatisticsData();
		}
	}

	@VisibleForTesting
	protected static Decimal fromHiveDecimal(org.apache.hadoop.hive.metastore.api.Decimal hiveDecimal) {
		BigDecimal bigDecimal = new BigDecimal(new BigInteger(hiveDecimal.getUnscaled()), hiveDecimal.getScale());
		return Decimal.fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
	}

	@VisibleForTesting
	protected static org.apache.hadoop.hive.metastore.api.Decimal fromFlinkDecimal(Decimal flinkDecimal) {
		BigDecimal bigDecimal = flinkDecimal.toBigDecimal();
		return new org.apache.hadoop.hive.metastore.api.Decimal(
				ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray()), (short) bigDecimal.scale());
	}

	/**
	 * Create Flink ColumnStats from Hive ColumnStatisticsData.
	 * Note we currently assume that, in Flink, the max and min of ColumnStats will be same type as the Flink column type.
	 * For example, for SHORT and Long columns, the max and min of their ColumnStats should be of type SHORT and LONG.
	 */
	private static ColumnStats createTableColumnStats(InternalType colType, ColumnStatisticsData stats) {
		if (stats.isSetBinaryStats()) {
			BinaryColumnStatsData binaryStats = stats.getBinaryStats();
			return new ColumnStats(
				null,
				binaryStats.getNumNulls(),
				binaryStats.getAvgColLen(),
				(int) binaryStats.getMaxColLen(),
				null,
				null);
		} else if (stats.isSetBooleanStats()) {
			BooleanColumnStatsData booleanStats = stats.getBooleanStats();
			return new ColumnStats(
				null,
				booleanStats.getNumNulls(),
				null,
				null,
				null,
				null);
		} else if (stats.isSetDateStats()) {
			DateColumnStatsData dateStats = stats.getDateStats();
			return new ColumnStats(
				dateStats.getNumDVs(),
				dateStats.getNumNulls(),
				null,
				null,
				new Date(dateStats.getHighValue().getDaysSinceEpoch() * MILLIS_PER_DAY),
				new Date(dateStats.getLowValue().getDaysSinceEpoch() * MILLIS_PER_DAY));
		} else if (stats.isSetDecimalStats()) {
			DecimalColumnStatsData decimalStats = stats.getDecimalStats();

			return new ColumnStats(
				decimalStats.getNumDVs(),
				decimalStats.getNumNulls(),
				null,
				null,
				fromHiveDecimal(decimalStats.getHighValue()),
				fromHiveDecimal(decimalStats.getLowValue()));
		} else if (stats.isSetDoubleStats()) {
			if (colType.equals(FloatType.INSTANCE)) {
				DoubleColumnStatsData floatStats = stats.getDoubleStats();
				return new ColumnStats(
					floatStats.getNumDVs(),
					floatStats.getNumNulls(),
					null,
					null,
					(float) floatStats.getHighValue(),
					(float) floatStats.getLowValue());
			} else if (colType.equals(DoubleType.INSTANCE)) {
				DoubleColumnStatsData doubleStats = stats.getDoubleStats();
				return new ColumnStats(
					doubleStats.getNumDVs(),
					doubleStats.getNumNulls(),
					null,
					null,
					doubleStats.getHighValue(),
					doubleStats.getLowValue());
			} else {
				LOG.warn("Flink does not support converting ColumnStatisticsData '{}' for Hive column type '{}' yet.", stats, colType);
				return null;
			}
		} else if (stats.isSetLongStats()) {
			if (colType.equals(ByteType.INSTANCE)) {
				LongColumnStatsData tinyint = stats.getLongStats();
				return new ColumnStats(tinyint.getNumDVs(), tinyint.getNumNulls(), null, null, new Byte(((Long) tinyint.getHighValue()).byteValue()), new Byte(((Long) tinyint.getLowValue()).byteValue()));
			} else if (colType.equals(ShortType.INSTANCE)) {
				LongColumnStatsData smallint = stats.getLongStats();
				return new ColumnStats(smallint.getNumDVs(), smallint.getNumNulls(), null, null, new Short(((Long) smallint.getHighValue()).shortValue()), new Short(((Long) smallint.getLowValue()).shortValue()));
			} else if (colType.equals(IntType.INSTANCE)) {
				LongColumnStatsData integer = stats.getLongStats();
				return new ColumnStats(integer.getNumDVs(), integer.getNumNulls(), null, null, new Integer(((Long) integer.getHighValue()).intValue()), new Integer(((Long) integer.getLowValue()).intValue()));
			} else if (colType.equals(LongType.INSTANCE)) {
				LongColumnStatsData longStats = stats.getLongStats();
				return new ColumnStats(longStats.getNumDVs(), longStats.getNumNulls(), null, null, new Long(((Long) longStats.getHighValue()).longValue()), new Long(((Long) longStats.getLowValue()).longValue()));
			} else if (colType instanceof TimestampType) {
				LongColumnStatsData timestampStats = stats.getLongStats();
				return new ColumnStats(timestampStats.getNumDVs(), timestampStats.getNumNulls(), null, null, new Timestamp(timestampStats.getHighValue()), new Timestamp(timestampStats.getLowValue()));
			} else {
				LOG.warn("Flink does not support converting ColumnStatisticsData '{}' for Hive column type '{}' yet.", stats, colType);
				return null;
			}
		} else if (stats.isSetStringStats()) {
			StringColumnStatsData stringStats = stats.getStringStats();
			return new ColumnStats(
				stringStats.getNumDVs(),
				stringStats.getNumNulls(),
				stringStats.getAvgColLen(),
				(int) stringStats.getMaxColLen(),
				null,
				null);
		} else {
			LOG.warn("Flink does not support converting ColumnStatisticsData '{}' for Hive column type '{}' yet.", stats, colType);
			return null;
		}
	}

	private static LinkedHashSet<String> getPartitionCols(Table hiveTable) {
		return hiveTable.getPartitionKeys().stream()
			.map(fs -> fs.getName())
			.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	private static Map<String, String> getPropertiesFromHiveTable(Table table) {
		Map<String, String> prop = new HashMap<>(table.getParameters());

		prop.put(HIVE_TABLE_TYPE, table.getTableType());

		StorageDescriptor sd = table.getSd();
		prop.put(HIVE_TABLE_LOCATION, sd.getLocation());
		prop.put(HIVE_TABLE_SERDE_LIBRARY, sd.getSerdeInfo().getSerializationLib());
		prop.put(HIVE_TABLE_INPUT_FORMAT, sd.getInputFormat());
		prop.put(HIVE_TABLE_OUTPUT_FORMAT, sd.getOutputFormat());
		prop.put(HIVE_TABLE_COMPRESSED, String.valueOf(sd.isCompressed()));
		prop.put(HIVE_TABLE_NUM_BUCKETS, String.valueOf(sd.getNumBuckets()));
		prop.put(HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT,
				String.valueOf(sd.getSerdeInfo().getParameters().get(serdeConstants.SERIALIZATION_FORMAT)));

		prop.putAll(table.getParameters());

		List<FieldSchema> fieldSchemas = new ArrayList<>(sd.getCols());
		fieldSchemas.addAll(table.getPartitionKeys());
		String[] colNames = new String[fieldSchemas.size()];
		String[] hiveTypes = new String[fieldSchemas.size()];
		for (int i = 0; i < fieldSchemas.size(); i++) {
			colNames[i] = fieldSchemas.get(i).getName();
			hiveTypes[i] = fieldSchemas.get(i).getType();
		}
		prop.put(HIVE_TABLE_FIELD_NAMES, StringUtils.join(colNames, ","));
		prop.put(HIVE_TABLE_FIELD_TYPES, StringUtils.join(hiveTypes, DEFAULT_LIST_COLUMN_TYPES_SEPARATOR));
		prop.put(HIVE_TABLE_DB_NAME, table.getDbName());
		prop.put(HIVE_TABLE_TABLE_NAME, table.getTableName());
		prop.put(HIVE_TABLE_PARTITION_FIELDS, String.valueOf(
				StringUtils.join(
						table.getPartitionKeys()
							.stream()
							.map(key -> key.getName().toLowerCase())
							.collect(Collectors.toList()), ",")));

		return prop;
	}

	/**
	 * Create a Hive database from CatalogDatabase.
	 */
	public static Database createHiveDatabase(String dbName, CatalogDatabase catalogDb) {
		Map<String, String> prop = catalogDb.getProperties();

		return new Database(
			dbName,
			prop.get(HiveDbConfig.HIVE_DB_DESCRIPTION),
			prop.get(HiveDbConfig.HIVE_DB_LOCATION_URI),
			catalogDb.getProperties());
	}

	/**
	 * Create a Hive database from CatalogDatabase.
	 */
	public static CatalogDatabase createCatalogDatabase(Database hiveDb) {
		Map<String, String> prop = new HashMap<>(hiveDb.getParameters());

		prop.put(HiveDbConfig.HIVE_DB_LOCATION_URI, hiveDb.getLocationUri());
		prop.put(HiveDbConfig.HIVE_DB_DESCRIPTION, hiveDb.getDescription());
		prop.put(HiveDbConfig.HIVE_DB_OWNER_NAME, hiveDb.getOwnerName());

		return new CatalogDatabase(prop);
	}

	/**
	 * Create a CatalogPartition from the given PartitionSpec and Hive partition.
	 */
	public static CatalogPartition createCatalogPartition(CatalogPartition.PartitionSpec spec, Partition partition) {
		Map<String, String> prop = new HashMap<>(partition.getParameters());

		StorageDescriptor sd = partition.getSd();
		prop.put(HIVE_TABLE_LOCATION, sd.getLocation());
		prop.put(HIVE_TABLE_SERDE_LIBRARY, sd.getSerdeInfo().getSerializationLib());
		prop.put(HIVE_TABLE_INPUT_FORMAT, sd.getInputFormat());
		prop.put(HIVE_TABLE_OUTPUT_FORMAT, sd.getOutputFormat());
		prop.put(HIVE_TABLE_COMPRESSED, String.valueOf(sd.isCompressed()));
		prop.put(HIVE_TABLE_NUM_BUCKETS, String.valueOf(sd.getNumBuckets()));

		prop.putAll(partition.getParameters());

		return new CatalogPartition(spec, prop);
	}

	/**
	 * Create Flink PartitionSpec from Hive partition name string.
	 * Example of Hive partition name string - "name=bob/year=2019"
	 */
	public static CatalogPartition.PartitionSpec createPartitionSpec(String hivePartitionName) {
		return CatalogPartition.fromStrings(Arrays.asList(hivePartitionName.split("/")));
	}

	/**
	 * Create a Hive partition from the given Hive table and CatalogPartition.
	 */
	public static Partition createHivePartition(Table hiveTable, CatalogPartition cp) {
		Partition partition = new Partition();

		partition.setValues(cp.getPartitionSpec().getOrderedValues(getPartitionKeys(hiveTable.getPartitionKeys())));
		partition.setDbName(hiveTable.getDbName());
		partition.setTableName(hiveTable.getTableName());
		partition.setCreateTime((int) (System.currentTimeMillis() / 1000));
		partition.setParameters(cp.getProperties());
		partition.setSd(hiveTable.getSd().deepCopy());

		String location = cp.getProperties().get(HIVE_TABLE_LOCATION);
		partition.getSd().setLocation(location != null ? location : null);

		return partition;
	}

	/**
	 * Get Hive table partition keys.
	 */
	public static List<String> getPartitionKeys(List<FieldSchema> fieldSchemas) {
		List<String> partitionKeys = new ArrayList<>(fieldSchemas.size());

		for (FieldSchema fs : fieldSchemas) {
			partitionKeys.add(fs.getName());
		}

		return partitionKeys;
	}

	/**
	 * Convert a hive type to Flink internal type.
	 * Note that even though serdeConstants.DATETIME_TYPE_NAME exists, Hive hasn't officially support DATETIME type yet.
	 */
	public static InternalType convert(String hiveType) {
		// Note: Any type match changes should be updated in documentation of data type mapping at /dev/table/catalog.md

		// First, handle types that have parameters such as CHAR(5), DECIMAL(6, 2), etc
		if (isVarcharOrCharType(hiveType)) {
			// For CHAR(p) and VARCHAR(p) types, map them to String for now because Flink doesn't yet support them.
			return StringType.INSTANCE;
		} else if (isDecimalType(hiveType)) {
			return DecimalType.of(hiveType);
		}

		switch (hiveType) {
			case serdeConstants.STRING_TYPE_NAME:
				return StringType.INSTANCE;
			case serdeConstants.BOOLEAN_TYPE_NAME:
				return BooleanType.INSTANCE;
			case serdeConstants.TINYINT_TYPE_NAME:
				return ByteType.INSTANCE;
			case serdeConstants.SMALLINT_TYPE_NAME:
				return ShortType.INSTANCE;
			case serdeConstants.INT_TYPE_NAME:
				return IntType.INSTANCE;
			case serdeConstants.BIGINT_TYPE_NAME:
				return LongType.INSTANCE;
			case serdeConstants.FLOAT_TYPE_NAME:
				return FloatType.INSTANCE;
			case serdeConstants.DOUBLE_TYPE_NAME:
				return DoubleType.INSTANCE;
			case serdeConstants.DATE_TYPE_NAME:
				return DateType.DATE;
			case serdeConstants.TIMESTAMP_TYPE_NAME:
				return TimestampType.TIMESTAMP;
			case serdeConstants.BINARY_TYPE_NAME:
				return ByteArrayType.INSTANCE;
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive's type %s yet.", hiveType));
		}
	}

	/**
	 * Check if a hive type in string is VARCHAR(p) or CHAR(p).
	 */
	private static boolean isVarcharOrCharType(String hiveType) {
		return hiveType.toLowerCase().startsWith(serdeConstants.CHAR_TYPE_NAME) ||
			hiveType.toLowerCase().startsWith(serdeConstants.VARCHAR_TYPE_NAME);
	}

	/**
	 * Check if a hive type in string is DECIMAL(X, Y).
	 */
	private static boolean isDecimalType(String hiveType) {
		return hiveType.toLowerCase().startsWith(serdeConstants.DECIMAL_TYPE_NAME);
	}

	/**
	 * Convert Flink's internal type to String for hive.
	 */
	public static String convert(InternalType internalType) {
		if (internalType.equals(BooleanType.INSTANCE)) {
			return serdeConstants.BOOLEAN_TYPE_NAME;
		} else if (internalType.equals(ByteType.INSTANCE)) {
			return serdeConstants.TINYINT_TYPE_NAME;
		} else if (internalType.equals(ShortType.INSTANCE)) {
			return serdeConstants.SMALLINT_TYPE_NAME;
		} else if (internalType.equals(IntType.INSTANCE)) {
			return serdeConstants.INT_TYPE_NAME;
		} else if (internalType.equals(LongType.INSTANCE)) {
			return serdeConstants.BIGINT_TYPE_NAME;
		} else if (internalType.equals(FloatType.INSTANCE)) {
			return serdeConstants.FLOAT_TYPE_NAME;
		} else if (internalType.equals(DoubleType.INSTANCE)) {
			return serdeConstants.DOUBLE_TYPE_NAME;
		} else if (internalType.equals(StringType.INSTANCE)) {
			return serdeConstants.STRING_TYPE_NAME;
		} else if (internalType.equals(CharType.INSTANCE)) {
			return serdeConstants.CHAR_TYPE_NAME + "(1)";
		} else if (internalType.equals(DateType.DATE)) {
			return serdeConstants.DATE_TYPE_NAME;
		} else if (internalType instanceof TimestampType) {
			return serdeConstants.TIMESTAMP_TYPE_NAME;
		} else if (internalType instanceof DecimalType) {
			return String.format(DECIMAL_TYPE_NAME_FORMAT, ((DecimalType) internalType).precision(), ((DecimalType) internalType).scale());
		} else if (internalType.equals(ByteArrayType.INSTANCE)) {
			return serdeConstants.BINARY_TYPE_NAME;
		} else {
			throw new UnsupportedOperationException(
				String.format("Flink's hive metadata integration doesn't support Flink type %s yet.",
					internalType.toString()));
		}
	}
}
