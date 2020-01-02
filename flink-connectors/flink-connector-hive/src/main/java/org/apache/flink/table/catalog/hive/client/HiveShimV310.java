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
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Shim for Hive version 3.1.0.
 */
public class HiveShimV310 extends HiveShimV235 {

	// timestamp classes
	private static Class hiveTimestampClz;
	private static Constructor hiveTimestampConstructor;
	private static Field hiveTimestampLocalDateTime;
	private static Constructor timestampWritableConstructor;

	// date classes
	private static Class hiveDateClz;
	private static Constructor hiveDateConstructor;
	private static Field hiveDateLocalDate;
	private static Constructor dateWritableConstructor;

	private static boolean hiveClassesInited;

	private static void initDateTimeClasses() {
		if (!hiveClassesInited) {
			synchronized (HiveShimV310.class) {
				if (!hiveClassesInited) {
					try {
						hiveTimestampClz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
						hiveTimestampConstructor = hiveTimestampClz.getDeclaredConstructor(LocalDateTime.class);
						hiveTimestampConstructor.setAccessible(true);
						hiveTimestampLocalDateTime = hiveTimestampClz.getDeclaredField("localDateTime");
						hiveTimestampLocalDateTime.setAccessible(true);
						timestampWritableConstructor = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritableV2")
								.getDeclaredConstructor(hiveTimestampClz);

						hiveDateClz = Class.forName("org.apache.hadoop.hive.common.type.Date");
						hiveDateConstructor = hiveDateClz.getDeclaredConstructor(LocalDate.class);
						hiveDateConstructor.setAccessible(true);
						hiveDateLocalDate = hiveDateClz.getDeclaredField("localDate");
						hiveDateLocalDate.setAccessible(true);
						dateWritableConstructor = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritableV2")
								.getDeclaredConstructor(hiveDateClz);
					} catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException e) {
						throw new FlinkHiveException("Failed to get Hive timestamp class and constructor", e);
					}
					hiveClassesInited = true;
				}
			}
		}
	}

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
		initDateTimeClasses();
		return hiveDateClz;
	}

	@Override
	public Class<?> getTimestampDataTypeClass() {
		initDateTimeClasses();
		return hiveTimestampClz;
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

	@Override
	public Object toHiveTimestamp(Object flinkTimestamp) {
		ensureSupportedFlinkTimestamp(flinkTimestamp);
		initDateTimeClasses();
		if (flinkTimestamp instanceof Timestamp) {
			flinkTimestamp = ((Timestamp) flinkTimestamp).toLocalDateTime();
		}
		try {
			return hiveTimestampConstructor.newInstance(flinkTimestamp);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new FlinkHiveException("Failed to convert to Hive timestamp", e);
		}
	}

	@Override
	public LocalDateTime toFlinkTimestamp(Object hiveTimestamp) {
		initDateTimeClasses();
		Preconditions.checkArgument(hiveTimestampClz.isAssignableFrom(hiveTimestamp.getClass()),
				"Expecting Hive timestamp to be an instance of %s, but actually got %s",
				hiveTimestampClz.getName(), hiveTimestamp.getClass().getName());
		try {
			return (LocalDateTime) hiveTimestampLocalDateTime.get(hiveTimestamp);
		} catch (IllegalAccessException e) {
			throw new FlinkHiveException("Failed to convert to Flink timestamp", e);
		}
	}

	@Override
	public Object toHiveDate(Object flinkDate) {
		ensureSupportedFlinkDate(flinkDate);
		initDateTimeClasses();
		if (flinkDate instanceof Date) {
			flinkDate = ((Date) flinkDate).toLocalDate();
		}
		try {
			return hiveDateConstructor.newInstance(flinkDate);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new FlinkHiveException("Failed to convert to Hive date", e);
		}
	}

	@Override
	public LocalDate toFlinkDate(Object hiveDate) {
		initDateTimeClasses();
		Preconditions.checkArgument(hiveDateClz.isAssignableFrom(hiveDate.getClass()),
				"Expecting Hive date to be an instance of %s, but actually got %s",
				hiveDateClz.getName(), hiveDate.getClass().getName());
		try {
			return (LocalDate) hiveDateLocalDate.get(hiveDate);
		} catch (IllegalAccessException e) {
			throw new FlinkHiveException("Failed to convert to Flink date", e);
		}
	}

	@Override
	public Writable hivePrimitiveToWritable(Object value) {
		if (value == null) {
			return null;
		}
		Optional<Writable> optional = javaToWritable(value);
		if (optional.isPresent()) {
			return optional.get();
		}
		try {
			if (getDateDataTypeClass().isInstance(value)) {
				return (Writable) dateWritableConstructor.newInstance(value);
			}
			if (getTimestampDataTypeClass().isInstance(value)) {
				return (Writable) timestampWritableConstructor.newInstance(value);
			}
		} catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
			throw new FlinkHiveException("Failed to create writable objects", e);
		}
		throw new FlinkHiveException("Unsupported primitive java value of class " + value.getClass().getName());
	}
}
