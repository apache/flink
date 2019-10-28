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

package org.apache.flink.table.catalog.hive.client;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.functions.hive.FlinkHiveUDFException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.thrift.TException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Shim for Hive version 1.2.0.
 */
public class HiveShimV120 extends HiveShimV111 {

	@Override
	public IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf) {
		try {
			Method method = RetryingMetaStoreClient.class.getMethod("getProxy", HiveConf.class);
			// getProxy is a static method
			return (IMetaStoreClient) method.invoke(null, (hiveConf));
		} catch (Exception ex) {
			throw new CatalogException("Failed to create Hive Metastore client", ex);
		}
	}

	@Override
	public void alterTable(IMetaStoreClient client, String databaseName, String tableName, Table table) throws InvalidOperationException, MetaException, TException {
		// For Hive-1.2.x, we need to tell HMS not to update stats. Otherwise, the stats we put in the table
		// parameters can be overridden. The extra config we add here will be removed by HMS after it's used.
		// Don't use StatsSetupConst.DO_NOT_UPDATE_STATS because it wasn't defined in Hive 1.1.x.
		table.getParameters().put("DO_NOT_UPDATE_STATS", "true");
		client.alter_table(databaseName, tableName, table);
	}

	@Override
	public ObjectInspector getObjectInspectorForConstant(PrimitiveTypeInfo primitiveTypeInfo, Object value) {
		String className;
		switch (primitiveTypeInfo.getPrimitiveCategory()) {
			case BOOLEAN:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBooleanObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case BYTE:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantByteObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case SHORT:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantShortObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case INT:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantIntObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case LONG:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantLongObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case FLOAT:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantFloatObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DOUBLE:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantDoubleObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case STRING:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case CHAR:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantHiveCharObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case VARCHAR:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantHiveVarcharObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DATE:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantDateObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case TIMESTAMP:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantTimestampObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DECIMAL:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantHiveDecimalObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case BINARY:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBinaryObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case UNKNOWN:
			case VOID:
				// If type is null, we use the Java Constant String to replace
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value.toString());
			default:
				throw new FlinkHiveUDFException(
					String.format("Cannot find ConstantObjectInspector for %s", primitiveTypeInfo));
		}
	}

	@Override
	public ColumnStatisticsData toHiveDateColStats(CatalogColumnStatisticsDataDate flinkDateColStats) {
		try {
			Class dateStatsClz = Class.forName("org.apache.hadoop.hive.metastore.api.DateColumnStatsData");
			Object dateStats = dateStatsClz.getDeclaredConstructor(long.class, long.class)
					.newInstance(flinkDateColStats.getNullCount(), flinkDateColStats.getNdv());
			Class hmsDateClz = Class.forName("org.apache.hadoop.hive.metastore.api.Date");
			Method setHigh = dateStatsClz.getDeclaredMethod("setHighValue", hmsDateClz);
			Method setLow = dateStatsClz.getDeclaredMethod("setLowValue", hmsDateClz);
			Constructor hmsDateConstructor = hmsDateClz.getConstructor(long.class);
			setHigh.invoke(dateStats, hmsDateConstructor.newInstance(flinkDateColStats.getMax().getDaysSinceEpoch()));
			setLow.invoke(dateStats, hmsDateConstructor.newInstance(flinkDateColStats.getMin().getDaysSinceEpoch()));
			Class colStatsClz = ColumnStatisticsData.class;
			return (ColumnStatisticsData) colStatsClz.getDeclaredMethod("dateStats", dateStatsClz).invoke(null, dateStats);
		} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new CatalogException("Failed to create Hive statistics for date column", e);
		}
	}

	@Override
	public boolean isDateStats(ColumnStatisticsData colStatsData) {
		try {
			Method method = ColumnStatisticsData.class.getDeclaredMethod("isSetDateStats");
			return (boolean) method.invoke(colStatsData);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new CatalogException("Failed to decide whether ColumnStatisticsData is for DATE column", e);
		}
	}

	@Override
	public CatalogColumnStatisticsDataDate toFlinkDateColStats(ColumnStatisticsData hiveDateColStats) {
		try {
			Object dateStats = ColumnStatisticsData.class.getDeclaredMethod("getDateStats").invoke(hiveDateColStats);
			Class dateStatsClz = dateStats.getClass();
			long numDV = (long) dateStatsClz.getMethod("getNumDVs").invoke(dateStats);
			long numNull = (long) dateStatsClz.getMethod("getNumNulls").invoke(dateStats);
			Object hmsHighDate = dateStatsClz.getMethod("getHighValue").invoke(dateStats);
			Object hmsLowDate = dateStatsClz.getMethod("getLowValue").invoke(dateStats);
			Class hmsDateClz = hmsHighDate.getClass();
			Method hmsDateDays = hmsDateClz.getMethod("getDaysSinceEpoch");
			long highDateDays = (long) hmsDateDays.invoke(hmsHighDate);
			long lowDateDays = (long) hmsDateDays.invoke(hmsLowDate);
			return new CatalogColumnStatisticsDataDate(new Date(lowDateDays), new Date(highDateDays), numDV, numNull);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new CatalogException("Failed to create Flink statistics for date column", e);
		}
	}
}
