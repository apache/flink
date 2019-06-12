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

import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utils to for HiveCatalog.
 */
public class HiveCatalogUtil {
	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogUtil.class);

	private HiveCatalogUtil() {}

	/**
	 * Create a map of Flink column stats from the given Hive column stats.
	 */
	public static Map<String, CatalogColumnStatisticsDataBase> createCatalogColumnStats(List<ColumnStatisticsObj> hiveColStats) {
		Map<String, CatalogColumnStatisticsDataBase> colStats = new HashMap<>();

		if (hiveColStats != null) {
			for (ColumnStatisticsObj colStatsObj : hiveColStats) {
				CatalogColumnStatisticsDataBase columnStats =
						createTableColumnStats(
								HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(colStatsObj.getColType())),
								colStatsObj.getStatsData());

				if (colStats != null) {
					colStats.put(colStatsObj.getColName(), columnStats);
				}
			}
		}

		return colStats;
	}

	/**
	 * Create columnStatistics from the given Hive column stats of a hive table.
	 */
	public static ColumnStatistics createTableColumnStats(
			Table hiveTable,
			Map<String, CatalogColumnStatisticsDataBase> colStats) {
		ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, hiveTable.getDbName(), hiveTable.getTableName());
		return createColumnStatistics(colStats, hiveTable.getSd(), desc);
	}

	/**
	 * Create columnStatistics from the given Hive column stats of a hive partition.
	 */
	public static ColumnStatistics createPartitionColumnStats(
			Partition hivePartition,
			String partName,
			Map<String, CatalogColumnStatisticsDataBase> colStats) {
		ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, hivePartition.getDbName(), hivePartition.getTableName());
		desc.setPartName(partName);
		return createColumnStatistics(colStats, hivePartition.getSd(), desc);
	}

	private static ColumnStatistics createColumnStatistics(
			Map<String, CatalogColumnStatisticsDataBase> colStats,
			StorageDescriptor sd,
			ColumnStatisticsDesc desc) {
		List<ColumnStatisticsObj> colStatsList = new ArrayList<>();

		for (FieldSchema field : sd.getCols()) {
			String hiveColName = field.getName();
			String hiveColType = field.getType();

			if (colStats.containsKey(hiveColName)) {
				CatalogColumnStatisticsDataBase flinkColStat = colStats.get(field.getName());
				ColumnStatisticsData statsData =
						getColumnStatisticsData(HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(hiveColType)), flinkColStat);
				ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj(hiveColName, hiveColType, statsData);
				colStatsList.add(columnStatisticsObj);
			}
		}

		return new ColumnStatistics(desc, colStatsList);
	}

	/**
	 * Create Flink ColumnStats from Hive ColumnStatisticsData.
	 */
	private static CatalogColumnStatisticsDataBase createTableColumnStats(DataType colType, ColumnStatisticsData stats) {
		if (stats.isSetBinaryStats()) {
			BinaryColumnStatsData binaryStats = stats.getBinaryStats();
			return new CatalogColumnStatisticsDataBinary(
					binaryStats.getMaxColLen(),
					binaryStats.getAvgColLen(),
					binaryStats.getNumNulls());
		} else if (stats.isSetBooleanStats()) {
			BooleanColumnStatsData booleanStats = stats.getBooleanStats();
			return new CatalogColumnStatisticsDataBoolean(
					booleanStats.getNumFalses(),
					booleanStats.getNumFalses(),
					booleanStats.getNumNulls());
		} else if (stats.isSetDateStats()) {
			DateColumnStatsData dateStats = stats.getDateStats();
			return new CatalogColumnStatisticsDataDate(
					new org.apache.flink.table.catalog.stats.Date(dateStats.getLowValue().getDaysSinceEpoch()),
					new org.apache.flink.table.catalog.stats.Date(dateStats.getHighValue().getDaysSinceEpoch()),
					dateStats.getNumDVs(),
					dateStats.getNumNulls());
		} else if (stats.isSetDoubleStats()) {
				DoubleColumnStatsData doubleStats = stats.getDoubleStats();
				return new CatalogColumnStatisticsDataDouble(
						doubleStats.getLowValue(), doubleStats.getHighValue(),
						doubleStats.getNumDVs(), doubleStats.getNumNulls());
		} else if (stats.isSetLongStats()) {
				LongColumnStatsData longColStats = stats.getLongStats();
				return new CatalogColumnStatisticsDataLong(
						longColStats.getLowValue(), longColStats.getHighValue(),
						longColStats.getNumDVs(), longColStats.getNumNulls());
		} else if (stats.isSetStringStats()) {
			StringColumnStatsData stringStats = stats.getStringStats();
			return new CatalogColumnStatisticsDataString(
					stringStats.getMaxColLen(), stringStats.getAvgColLen(),
					stringStats.getNumDVs(), stringStats.getNumNulls());
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
	private static ColumnStatisticsData getColumnStatisticsData(DataType colType, CatalogColumnStatisticsDataBase colStat) {
		LogicalType colLogicalType = colType.getLogicalType();
		if (colLogicalType instanceof CharType || colLogicalType instanceof VarCharType) {
			if (colStat instanceof CatalogColumnStatisticsDataString) {
				CatalogColumnStatisticsDataString stringColStat = (CatalogColumnStatisticsDataString) colStat;
				return ColumnStatisticsData.stringStats(new StringColumnStatsData(stringColStat.getMaxLength(), stringColStat.getAvgLength(), stringColStat.getNullCount(), stringColStat.getNdv()));
			}
		} else if (colLogicalType instanceof BooleanType) {
			if (colStat instanceof CatalogColumnStatisticsDataBoolean) {
				CatalogColumnStatisticsDataBoolean booleanColStat = (CatalogColumnStatisticsDataBoolean) colStat;
				BooleanColumnStatsData boolStats = new BooleanColumnStatsData(
						booleanColStat.getTrueCount(),
						booleanColStat.getFalseCount(),
						booleanColStat.getNullCount());
				return ColumnStatisticsData.booleanStats(boolStats);
			}
		} else if (colLogicalType instanceof TinyIntType || colLogicalType instanceof SmallIntType ||
				colLogicalType instanceof IntType || colLogicalType instanceof BigIntType ||
				colLogicalType instanceof TimestampType) {
			if (colStat instanceof CatalogColumnStatisticsDataLong) {
				CatalogColumnStatisticsDataLong longColStat = (CatalogColumnStatisticsDataLong) colStat;
				LongColumnStatsData longColumnStatsData = new LongColumnStatsData(longColStat.getNullCount(), longColStat.getNdv());
				longColumnStatsData.setHighValue(longColStat.getMax());
				longColumnStatsData.setLowValue(longColStat.getMin());
				return ColumnStatisticsData.longStats(longColumnStatsData);
			}
		} else if (colLogicalType instanceof FloatType || colLogicalType instanceof DoubleType) {
			if (colStat instanceof CatalogColumnStatisticsDataDouble) {
				CatalogColumnStatisticsDataDouble doubleColumnStatsData = (CatalogColumnStatisticsDataDouble) colStat;
				DoubleColumnStatsData floatStats = new DoubleColumnStatsData(doubleColumnStatsData.getNullCount(), doubleColumnStatsData.getNdv());
				floatStats.setHighValue(doubleColumnStatsData.getMax());
				floatStats.setLowValue(doubleColumnStatsData.getMin());
				return ColumnStatisticsData.doubleStats(floatStats);
			}
		} else if (colLogicalType instanceof DateType) {
			if (colStat instanceof CatalogColumnStatisticsDataDate) {
				CatalogColumnStatisticsDataDate dateColumnStatsData = (CatalogColumnStatisticsDataDate) colStat;
				DateColumnStatsData dateStats = new DateColumnStatsData(dateColumnStatsData.getNullCount(), dateColumnStatsData.getNdv());
				dateStats.setHighValue(new org.apache.hadoop.hive.metastore.api.Date(dateColumnStatsData.getMax().getDaysSinceEpoch()));
				dateStats.setLowValue(new org.apache.hadoop.hive.metastore.api.Date(dateColumnStatsData.getMin().getDaysSinceEpoch()));
				return ColumnStatisticsData.dateStats(dateStats);
			}
		}
		LOG.warn("Flink does not support converting ColumnStats '{}' for Hive column type '{}' yet.", colStat, colType);
		return new ColumnStatisticsData();
	}
}
