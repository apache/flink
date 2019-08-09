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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.thrift.TException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Shim for Hive version 1.x.
 */
public class HiveShimV1 implements HiveShim {

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
	// 1.x client doesn't support filtering tables by type, so here we need to get all tables and filter by ourselves
	public List<String> getViews(IMetaStoreClient client, String databaseName) throws UnknownDBException, TException {
		// We don't have to use reflection here because client.getAllTables(String) is supposed to be there for
		// all versions.
		List<String> tableNames = client.getAllTables(databaseName);
		List<String> views = new ArrayList<>();
		for (String name : tableNames) {
			Table table = client.getTable(databaseName, name);
			String viewDef = table.getViewOriginalText();
			if (viewDef != null && !viewDef.isEmpty()) {
				views.add(table.getTableName());
			}
		}
		return views;
	}

	@Override
	public Function getFunction(IMetaStoreClient client, String dbName, String functionName) throws NoSuchObjectException, TException {
		try {
			// hive-1.x doesn't throw NoSuchObjectException if function doesn't exist, instead it throws a MetaException
			return client.getFunction(dbName, functionName);
		} catch (MetaException e) {
			// need to check the cause and message of this MetaException to decide whether it should actually be a NoSuchObjectException
			if (e.getCause() instanceof NoSuchObjectException) {
				throw (NoSuchObjectException) e.getCause();
			}
			if (e.getMessage().startsWith(NoSuchObjectException.class.getSimpleName())) {
				throw new NoSuchObjectException(e.getMessage());
			}
			throw e;
		}
	}

	@Override
	public boolean moveToTrash(FileSystem fs, Path path, Configuration conf, boolean purge) throws IOException {
		try {
			Method method = FileUtils.class.getDeclaredMethod("moveToTrash", FileSystem.class, Path.class, Configuration.class);
			return (boolean) method.invoke(null, fs, path, conf);
		} catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
			throw new IOException("Failed to move " + path + " to trash", e);
		}
	}

	@Override
	public void alterTable(IMetaStoreClient client, String databaseName, String tableName, Table table) throws InvalidOperationException, MetaException, TException {
		// For Hive-1.2.1, we need to tell HMS not to update stats. Otherwise, the stats we put in the table
		// parameters can be overridden. The extra config we add here will be removed by HMS after it's used.
		table.getParameters().put(StatsSetupConst.DO_NOT_UPDATE_STATS, "true");
		client.alter_table(databaseName, tableName, table);
	}

	@Override
	public SimpleGenericUDAFParameterInfo createUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct, boolean allColumns) {
		try {
			Constructor constructor = SimpleGenericUDAFParameterInfo.class.getConstructor(ObjectInspector[].class,
					boolean.class, boolean.class);
			return (SimpleGenericUDAFParameterInfo) constructor.newInstance(params, distinct, allColumns);
		} catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			throw new CatalogException("Failed to create SimpleGenericUDAFParameterInfo", e);
		}
	}
}
