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

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.Date;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.thrift.TException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
	public ColumnStatisticsData toHiveDateColStats(CatalogColumnStatisticsDataDate flinkDateColStats) {
		try {
			Class dateStatsClz = Class.forName("org.apache.hadoop.hive.metastore.api.DateColumnStatsData");
			Object dateStats = dateStatsClz.getDeclaredConstructor().newInstance();
			dateStatsClz.getMethod("clear").invoke(dateStats);
			if (null != flinkDateColStats.getNdv()) {
				dateStatsClz.getMethod("setNumDVs", long.class).invoke(dateStats, flinkDateColStats.getNdv());
			}
			if (null != flinkDateColStats.getNullCount()) {
				dateStatsClz.getMethod("setNumNulls", long.class).invoke(dateStats, flinkDateColStats.getNullCount());
			}
			Class hmsDateClz = Class.forName("org.apache.hadoop.hive.metastore.api.Date");
			Constructor hmsDateConstructor = hmsDateClz.getConstructor(long.class);
			if (null != flinkDateColStats.getMax()) {
				Method setHigh = dateStatsClz.getDeclaredMethod("setHighValue", hmsDateClz);
				setHigh.invoke(dateStats,
							hmsDateConstructor.newInstance(flinkDateColStats.getMax().getDaysSinceEpoch()));
			}
			if (null != flinkDateColStats.getMin()) {
				Method setLow = dateStatsClz.getDeclaredMethod("setLowValue", hmsDateClz);
				setLow.invoke(dateStats,
							hmsDateConstructor.newInstance(flinkDateColStats.getMin().getDaysSinceEpoch()));
			}
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
			boolean isSetNumDv = (boolean) dateStatsClz.getMethod("isSetNumDVs").invoke(dateStats);
			boolean isSetNumNull = (boolean) dateStatsClz.getMethod("isSetNumNulls").invoke(dateStats);
			boolean isSetHighValue = (boolean) dateStatsClz.getMethod("isSetHighValue").invoke(dateStats);
			boolean isSetLowValue = (boolean) dateStatsClz.getMethod("isSetLowValue").invoke(dateStats);
			Long numDV = isSetNumDv ? (Long) dateStatsClz.getMethod("getNumDVs").invoke(dateStats) : null;
			Long numNull = isSetNumNull ? (Long) dateStatsClz.getMethod("getNumNulls").invoke(dateStats) : null;
			Object hmsHighDate = dateStatsClz.getMethod("getHighValue").invoke(dateStats);
			Object hmsLowDate = dateStatsClz.getMethod("getLowValue").invoke(dateStats);
			Class hmsDateClz = hmsHighDate.getClass();
			Method hmsDateDays = hmsDateClz.getMethod("getDaysSinceEpoch");
			Date highDateDays = isSetHighValue ? new Date((Long) hmsDateDays.invoke(hmsHighDate)) : null;
			Date lowDateDays = isSetLowValue ? new Date((Long) hmsDateDays.invoke(hmsLowDate)) : null;
			return new CatalogColumnStatisticsDataDate(lowDateDays, highDateDays, numDV, numNull);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new CatalogException("Failed to create Flink statistics for date column", e);
		}
	}

	@Override
	public Set<String> listBuiltInFunctions() {
		try {
			Method method = FunctionRegistry.class.getMethod("getFunctionNames");
			// getFunctionNames is a static method
			Set<String> names = (Set<String>) method.invoke(null);

			return names.stream()
				.filter(n -> isBuiltInFunctionInfo(getFunctionInfo(n).get()))
				.collect(Collectors.toSet());
		} catch (Exception ex) {
			throw new CatalogException("Failed to invoke FunctionRegistry.getFunctionNames()", ex);
		}
	}

	@Override
	public Optional<FunctionInfo> getBuiltInFunctionInfo(String name) {
		Optional<FunctionInfo> functionInfo = getFunctionInfo(name);

		if (functionInfo.isPresent() && isBuiltInFunctionInfo(functionInfo.get())) {
			return functionInfo;
		} else {
			return Optional.empty();
		}
	}

	private Optional<FunctionInfo> getFunctionInfo(String name) {
		try {
			return Optional.of(FunctionRegistry.getFunctionInfo(name));
		} catch (SemanticException e) {
			throw new FlinkHiveException(
				String.format("Failed getting function info for %s", name), e);
		} catch (NullPointerException e) {
			return Optional.empty();
		}
	}

	private boolean isBuiltInFunctionInfo(FunctionInfo info) {
		try {
			Method method = FunctionInfo.class.getMethod("isBuiltIn", null);
			return (boolean) method.invoke(info);
		} catch (Exception ex) {
			throw new CatalogException("Failed to invoke FunctionInfo.isBuiltIn()", ex);
		}
	}
}
