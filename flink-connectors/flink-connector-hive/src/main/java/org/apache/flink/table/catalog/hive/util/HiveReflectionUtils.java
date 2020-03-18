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
import org.apache.flink.table.functions.hive.FlinkHiveUDFException;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utilities for accessing Hive class or methods via Java reflection.
 *
 * <p>They are put here not for code sharing. Rather, this is a boiler place for managing similar code that involves
 * reflection. (In fact, they could be just private method in their respective calling class.)
 *
 * <p>Relevant Hive methods cannot be called directly because shimming is required to support different, possibly
 * incompatible Hive versions.
 */
public class HiveReflectionUtils {

	public static Properties getTableMetadata(HiveShim hiveShim, Table table) {
		try {
			Method method = hiveShim.getMetaStoreUtilsClass().getMethod("getTableMetadata", Table.class);
			return (Properties) method.invoke(null, table);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new CatalogException("Failed to invoke MetaStoreUtils.getTableMetadata()", e);
		}
	}

	public static List<String> getPvals(HiveShim hiveShim, List<FieldSchema> partCols, Map<String, String> partSpec) {
		try {
			Method method = hiveShim.getMetaStoreUtilsClass().getMethod("getPvals", List.class, Map.class);
			return (List<String>) method.invoke(null, partCols, partSpec);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new CatalogException("Failed to invoke MetaStoreUtils.getFieldsFromDeserializer", e);
		}
	}

	public static ObjectInspector createConstantObjectInspector(String className, Object value) {
		try {
			Constructor<?>  method = Class.forName(className).getDeclaredConstructor(value.getClass());
			method.setAccessible(true);
			return (ObjectInspector) method.newInstance(value);
		} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
				| InvocationTargetException e) {
			throw new FlinkHiveUDFException("Failed to instantiate java constant object inspector", e);
		}
	}

	public static Object invokeMethod(Class clz, Object obj, String methodName, Class[] argClz, Object[] args)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method method;
		try {
			method = clz.getDeclaredMethod(methodName, argClz);
		} catch (NoSuchMethodException e) {
			method = clz.getMethod(methodName, argClz);
		}
		return method.invoke(obj, args);
	}

}
