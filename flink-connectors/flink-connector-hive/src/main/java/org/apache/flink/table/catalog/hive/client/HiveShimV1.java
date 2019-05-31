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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

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
			if (e.getCause() instanceof NoSuchObjectException) {
				throw (NoSuchObjectException) e.getCause();
			}
			throw e;
		}
	}
}
