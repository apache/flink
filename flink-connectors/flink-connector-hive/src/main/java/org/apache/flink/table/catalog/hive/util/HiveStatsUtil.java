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

package org.apache.flink.table.catalog.hive.util;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utils for stats of HiveCatalog.
 */
public class HiveStatsUtil {
	private static final Logger LOG = LoggerFactory.getLogger(HiveStatsUtil.class);

	private static final int DEFAULT_UNKNOWN_STATS_VALUE = -1;

	private HiveStatsUtil() {}

	/**
	 * Create a map of Flink column stats from the given Hive column stats.
	 */
	public static Map<String, CatalogColumnStatisticsDataBase> createCatalogColumnStats(@Nonnull List<ColumnStatisticsObj> hiveColStats, String hiveVersion) {
		checkNotNull(hiveColStats, "hiveColStats can not be null");
		Map<String, CatalogColumnStatisticsDataBase> colStats = new HashMap<>();
		for (ColumnStatisticsObj colStatsObj : hiveColStats) {
			CatalogColumnStatisticsDataBase columnStats = createTableColumnStats(
					HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(colStatsObj.getColType())),
					colStatsObj.getStatsData(), hiveVersion);
			colStats.put(colStatsObj.getColName(), columnStats);
		}

		return colStats;
	}

	/**
	 * Create columnStatistics from the given Hive column stats of a hive table.
	 */
	public static ColumnStatistics createTableColumnStats(
			Table hiveTable,
			Map<String, CatalogColumnStatisticsDataBase> colStats,
			String hiveVersion) {
		ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, hiveTable.getDbName(), hiveTable.getTableName());
		return createHiveColumnStatistics(colStats, hiveTable.getSd(), desc, hiveVersion);
	}

	/**
	 * Create columnStatistics from the given Hive column stats of a hive partition.
	 */
	public static ColumnStatistics createPartitionColumnStats(
			Partition hivePartition,
			String partName,
			Map<String, CatalogColumnStatisticsDataBase> colStats,
			String hiveVersion) {
		ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, hivePartition.getDbName(), hivePartition.getTableName());
		desc.setPartName(partName);
		return createHiveColumnStatistics(colStats, hivePartition.getSd(), desc, hiveVersion);
	}

	private static ColumnStatistics createHiveColumnStatistics(
			Map<String, CatalogColumnStatisticsDataBase> colStats,
			StorageDescriptor sd,
			ColumnStatisticsDesc desc,
			String hiveVersion) {
		List<ColumnStatisticsObj> colStatsList = new ArrayList<>();

		for (FieldSchema field : sd.getCols()) {
			String hiveColName = field.getName();
			String hiveColType = field.getType();
			CatalogColumnStatisticsDataBase flinkColStat = colStats.get(field.getName());
			if (null != flinkColStat) {
				ColumnStatisticsData statsData = getColumnStatisticsData(
						HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(hiveColType)),
						flinkColStat,
						hiveVersion);
				ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj(hiveColName, hiveColType, statsData);
				colStatsList.add(columnStatisticsObj);
			}
		}

		return new ColumnStatistics(desc, colStatsList);
	}

	/**
	 * Create Flink ColumnStats from Hive ColumnStatisticsData.
	 */
	private static CatalogColumnStatisticsDataBase createTableColumnStats(DataType colType, ColumnStatisticsData stats, String hiveVersion) {
		HiveShim hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
		if (stats.isSetBinaryStats()) {
			BinaryColumnStatsData binaryStats = stats.getBinaryStats();
			return new CatalogColumnStatisticsDataBinary(
					binaryStats.isSetMaxColLen() ? binaryStats.getMaxColLen() : null,
					binaryStats.isSetAvgColLen() ? binaryStats.getAvgColLen() : null,
					binaryStats.isSetNumNulls() ? binaryStats.getNumNulls() : null);
		} else if (stats.isSetBooleanStats()) {
			BooleanColumnStatsData booleanStats = stats.getBooleanStats();
			return new CatalogColumnStatisticsDataBoolean(
					booleanStats.isSetNumTrues() ? booleanStats.getNumTrues() : null,
					booleanStats.isSetNumFalses() ? booleanStats.getNumFalses() : null,
					booleanStats.isSetNumNulls() ? booleanStats.getNumNulls() : null);
		} else if (hiveShim.isDateStats(stats)) {
			return hiveShim.toFlinkDateColStats(stats);
		} else if (stats.isSetDoubleStats()) {
				DoubleColumnStatsData doubleStats = stats.getDoubleStats();
				return new CatalogColumnStatisticsDataDouble(
						doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null,
						doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null,
						doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null,
						doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null);
		} else if (stats.isSetLongStats()) {
				LongColumnStatsData longColStats = stats.getLongStats();
				return new CatalogColumnStatisticsDataLong(
						longColStats.isSetLowValue() ? longColStats.getLowValue() : null,
						longColStats.isSetHighValue() ? longColStats.getHighValue() : null,
						longColStats.isSetNumDVs() ? longColStats.getNumDVs() : null,
						longColStats.isSetNumNulls() ? longColStats.getNumNulls() : null);
		} else if (stats.isSetStringStats()) {
			StringColumnStatsData stringStats = stats.getStringStats();
			return new CatalogColumnStatisticsDataString(
					stringStats.isSetMaxColLen() ? stringStats.getMaxColLen() : null,
					stringStats.isSetAvgColLen() ? stringStats.getAvgColLen() : null,
					stringStats.isSetNumDVs() ? stringStats.getNumDVs() : null,
					stringStats.isSetNumDVs() ? stringStats.getNumNulls() : null);
		} else {
			LOG.warn("Flink does not support converting ColumnStatisticsData '{}' for Hive column type '{}' yet.", stats, colType);
			return null;
		}
	}

	/**
	 * Convert Flink ColumnStats to Hive ColumnStatisticsData according to Hive column type.
	 * Note we currently assume that, in Flink, the max and min of ColumnStats will be same type as the Flink column type.
	 * For example, for SHORT and Long columns, the max and min of their ColumnStats should be of type SHORT and LONG.
	 */
	private static ColumnStatisticsData getColumnStatisticsData(DataType colType, CatalogColumnStatisticsDataBase colStat,
			String hiveVersion) {
		LogicalTypeRoot type = colType.getLogicalType().getTypeRoot();
		if (type.equals(LogicalTypeRoot.CHAR)
		|| type.equals(LogicalTypeRoot.VARCHAR)) {
			if (colStat instanceof CatalogColumnStatisticsDataString) {
				CatalogColumnStatisticsDataString stringColStat = (CatalogColumnStatisticsDataString) colStat;
				StringColumnStatsData hiveStringColumnStats = new StringColumnStatsData();
				hiveStringColumnStats.clear();
				if (null != stringColStat.getMaxLength()) {
					hiveStringColumnStats.setMaxColLen(stringColStat.getMaxLength());
				}
				if (null != stringColStat.getAvgLength()) {
					hiveStringColumnStats.setAvgColLen(stringColStat.getAvgLength());
				}
				if (null != stringColStat.getNullCount()) {
					hiveStringColumnStats.setNumNulls(stringColStat.getNullCount());
				}
				if (null != stringColStat.getNdv()) {
					hiveStringColumnStats.setNumDVs(stringColStat.getNdv());
				}
				return ColumnStatisticsData.stringStats(hiveStringColumnStats);
			}
		} else if (type.equals(LogicalTypeRoot.BOOLEAN)) {
			if (colStat instanceof CatalogColumnStatisticsDataBoolean) {
				CatalogColumnStatisticsDataBoolean booleanColStat = (CatalogColumnStatisticsDataBoolean) colStat;
				BooleanColumnStatsData hiveBoolStats = new BooleanColumnStatsData();
				hiveBoolStats.clear();
				if (null != booleanColStat.getTrueCount()) {
					hiveBoolStats.setNumTrues(booleanColStat.getTrueCount());
				}
				if (null != booleanColStat.getFalseCount()) {
					hiveBoolStats.setNumFalses(booleanColStat.getFalseCount());
				}
				if (null != booleanColStat.getNullCount()) {
					hiveBoolStats.setNumNulls(booleanColStat.getNullCount());
				}
				return ColumnStatisticsData.booleanStats(hiveBoolStats);
			}
		} else if (type.equals(LogicalTypeRoot.TINYINT)
				|| type.equals(LogicalTypeRoot.SMALLINT)
				|| type.equals(LogicalTypeRoot.INTEGER)
				|| type.equals(LogicalTypeRoot.BIGINT)
				|| type.equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
				|| type.equals(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)
				|| type.equals(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE)) {
			if (colStat instanceof CatalogColumnStatisticsDataLong) {
				CatalogColumnStatisticsDataLong longColStat = (CatalogColumnStatisticsDataLong) colStat;
				LongColumnStatsData hiveLongColStats = new LongColumnStatsData();
				hiveLongColStats.clear();
				if (null != longColStat.getMax()) {
					hiveLongColStats.setHighValue(longColStat.getMax());
				}
				if (null != longColStat.getMin()) {
					hiveLongColStats.setLowValue(longColStat.getMin());
				}
				if (null != longColStat.getNdv()) {
					hiveLongColStats.setNumDVs(longColStat.getNdv());
				}
				if (null != longColStat.getNullCount()) {
					hiveLongColStats.setNumNulls(longColStat.getNullCount());
				}
				return ColumnStatisticsData.longStats(hiveLongColStats);
			}
		} else if (type.equals(LogicalTypeRoot.FLOAT)
				|| type.equals(LogicalTypeRoot.DOUBLE)) {
			if (colStat instanceof CatalogColumnStatisticsDataDouble) {
				CatalogColumnStatisticsDataDouble doubleColumnStatsData = (CatalogColumnStatisticsDataDouble) colStat;
				DoubleColumnStatsData hiveFloatStats = new DoubleColumnStatsData();
				hiveFloatStats.clear();
				if (null != doubleColumnStatsData.getMax()) {
					hiveFloatStats.setHighValue(doubleColumnStatsData.getMax());
				}
				if (null != doubleColumnStatsData.getMin()) {
					hiveFloatStats.setLowValue(doubleColumnStatsData.getMin());
				}
				if (null != doubleColumnStatsData.getNullCount()) {
					hiveFloatStats.setNumNulls(doubleColumnStatsData.getNullCount());
				}
				if (null != doubleColumnStatsData.getNdv()) {
					hiveFloatStats.setNumDVs(doubleColumnStatsData.getNdv());
				}
				return ColumnStatisticsData.doubleStats(hiveFloatStats);
			}
		} else if (type.equals(LogicalTypeRoot.DATE)) {
			if (colStat instanceof CatalogColumnStatisticsDataDate) {
				HiveShim hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
				return hiveShim.toHiveDateColStats((CatalogColumnStatisticsDataDate) colStat);
			}
		} else if (type.equals(LogicalTypeRoot.VARBINARY)
				|| type.equals(LogicalTypeRoot.BINARY)) {
			if (colStat instanceof CatalogColumnStatisticsDataBinary) {
				CatalogColumnStatisticsDataBinary binaryColumnStatsData = (CatalogColumnStatisticsDataBinary) colStat;
				BinaryColumnStatsData hiveBinaryColumnStats = new BinaryColumnStatsData();
				hiveBinaryColumnStats.clear();
				if (null != binaryColumnStatsData.getMaxLength()) {
					hiveBinaryColumnStats.setMaxColLen(binaryColumnStatsData.getMaxLength());
				}
				if (null != binaryColumnStatsData.getAvgLength()) {
					hiveBinaryColumnStats.setAvgColLen(binaryColumnStatsData.getAvgLength());
				}
				if (null != binaryColumnStatsData.getNullCount()) {
					hiveBinaryColumnStats.setNumNulls(binaryColumnStatsData.getNullCount());
				}
				return ColumnStatisticsData.binaryStats(hiveBinaryColumnStats);
			}
		}
		throw new CatalogException(String.format("Flink does not support converting ColumnStats '%s' for Hive column " +
												"type '%s' yet", colStat, colType));
	}

	public static int parsePositiveIntStat(Map<String, String> parameters, String key) {
		String value = parameters.get(key);
		if (value == null) {
			return DEFAULT_UNKNOWN_STATS_VALUE;
		} else {
			int v = Integer.parseInt(value);
			return v > 0 ? v : DEFAULT_UNKNOWN_STATS_VALUE;
		}
	}

	public static long parsePositiveLongStat(Map<String, String> parameters, String key) {
		String value = parameters.get(key);
		if (value == null) {
			return DEFAULT_UNKNOWN_STATS_VALUE;
		} else {
			long v = Long.parseLong(value);
			return v > 0 ? v : DEFAULT_UNKNOWN_STATS_VALUE;
		}
	}
}
