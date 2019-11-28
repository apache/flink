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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shim for Hive version 3.1.0.
 */
public class HiveShimV310 extends HiveShimV235 {

	@Override
	public IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf) {
		try {
			Method method = RetryingMetaStoreClient.class.getMethod("getProxy", Configuration.class, Boolean.TYPE);
			// getProxy is a static method
			return (IMetaStoreClient) method.invoke(null, hiveConf, true);
		} catch (Exception ex) {
			throw new CatalogException("Failed to create Hive Metastore client", ex);
		}
	}

	@Override
	public Class<?> getMetaStoreUtilsClass() {
		try {
			return Class.forName("org.apache.hadoop.hive.metastore.utils.MetaStoreUtils");
		} catch (ClassNotFoundException e) {
			throw new CatalogException("Failed to find class MetaStoreUtils", e);
		}
	}

	@Override
	public Class<?> getHiveMetaStoreUtilsClass() {
		try {
			return Class.forName("org.apache.hadoop.hive.metastore.HiveMetaStoreUtils");
		} catch (ClassNotFoundException e) {
			throw new CatalogException("Failed to find class HiveMetaStoreUtils", e);
		}
	}

	@Override
	public Class<?> getDateDataTypeClass() {
		try {
			return Class.forName("org.apache.hadoop.hive.common.type.Date");
		} catch (ClassNotFoundException e) {
			throw new CatalogException("Failed to find class org.apache.hadoop.hive.common.type.Date", e);
		}
	}

	@Override
	public Class<?> getTimestampDataTypeClass() {
		try {
			return Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
		} catch (ClassNotFoundException e) {
			throw new CatalogException("Failed to find class org.apache.hadoop.hive.common.type.Timestamp", e);
		}
	}

	@Override
	public FileStatus[] getFileStatusRecurse(Path path, int level, FileSystem fs) throws IOException {
		try {
			Method method = HiveStatsUtils.class.getMethod("getFileStatusRecurse", Path.class, Integer.TYPE, FileSystem.class);
			// getFileStatusRecurse is a static method
			List<FileStatus> results = (List<FileStatus>) method.invoke(null, path, level, fs);
			return results.toArray(new FileStatus[0]);
		} catch (Exception ex) {
			throw new CatalogException("Failed to invoke HiveStatsUtils.getFileStatusRecurse()", ex);
		}
	}

	@Override
	public void makeSpecFromName(Map<String, String> partSpec, Path currPath) {
		try {
			Method method = Warehouse.class.getMethod("makeSpecFromName", Map.class, Path.class, Set.class);
			// makeSpecFromName is a static method
			method.invoke(null, partSpec, currPath, null);
		} catch (Exception ex) {
			throw new CatalogException("Failed to invoke Warehouse.makeSpecFromName()", ex);
		}
	}

	@Override
	public Set<String> getNotNullColumns(IMetaStoreClient client, Configuration conf, String dbName, String tableName) {
		try {
			// HMS catalog (https://issues.apache.org/jira/browse/HIVE-18685) is an on-going feature and we currently
			// just get the default catalog.
			String hiveDefaultCatalog = (String) HiveReflectionUtils.invokeMethod(getMetaStoreUtilsClass(), null,
					"getDefaultCatalog", new Class[]{Configuration.class}, new Object[]{conf});
			Class requestClz = Class.forName("org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest");
			Object request = requestClz.getDeclaredConstructor(String.class, String.class, String.class)
					.newInstance(hiveDefaultCatalog, dbName, tableName);
			List<?> constraints = (List<?>) HiveReflectionUtils.invokeMethod(client.getClass(), client,
					"getNotNullConstraints", new Class[]{requestClz}, new Object[]{request});
			Class constraintClz = Class.forName("org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint");
			Method colNameMethod = constraintClz.getDeclaredMethod("getColumn_name");
			Method isRelyMethod = constraintClz.getDeclaredMethod("isRely_cstr");
			Set<String> res = new HashSet<>();
			for (Object constraint : constraints) {
				if ((boolean) isRelyMethod.invoke(constraint)) {
					res.add((String) colNameMethod.invoke(constraint));
				}
			}
			return res;
		} catch (Exception e) {
			throw new CatalogException("Failed to get NOT NULL constraints", e);
		}
	}
}
