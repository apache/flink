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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Utils related to Hive Catalog for Hive dialect. In Hive dialect, the classloader of the Hive's
 * catalog got in Hive dialect is FlinkUserClassloader. But the classloader of the class HiveCatalog
 * is Flink's ComponentClassLoader. The classloader is not same, so we can't cast the Hive's catalog
 * got to HiveCatalog directly which cause us can only call these methods in HiveCatalog via
 * reflection.
 *
 * <p>It's a hack logic, we can remove the hack logic after we split the Hive connector into two
 * jars, one of which is only for source/sink, and the other one is only for hive dialect. Then, the
 * class loader will be same.
 */
public class HiveCatalogUtils {

    public static boolean isHiveCatalog(Catalog catalog) {
        // we can't use catalog instanceof HiveCatalog directly
        // as the classloaders for catalog and HiveCatalog are different
        return catalog instanceof HiveCatalog
                || catalog.getClass().getName().equals(HiveCatalog.class.getName());
    }

    public static HiveConf getHiveConf(Catalog catalog) {
        try {
            return (HiveConf)
                    HiveReflectionUtils.invokeMethod(
                            catalog.getClass(), catalog, "getHiveConf", null, null);
        } catch (Exception e) {
            throw new TableException(
                    String.format("Fail to get HiveConf via catalog: %s.", catalog.getClass()), e);
        }
    }

    public static String getHiveVersion(Catalog catalog) {
        try {
            return (String)
                    HiveReflectionUtils.invokeMethod(
                            catalog.getClass(), catalog, "getHiveVersion", null, null);
        } catch (Exception e) {
            throw new TableException(
                    String.format("Fail to get Hive version via catalog: %s.", catalog.getClass()),
                    e);
        }
    }

    public static Table getTable(Catalog catalog, ObjectPath tablePath) {
        try {
            return (Table)
                    HiveReflectionUtils.invokeMethod(
                            catalog.getClass(),
                            catalog,
                            "getHiveTable",
                            new Class[] {ObjectPath.class},
                            new Object[] {tablePath});
        } catch (Exception e) {
            throw new TableException(
                    String.format("Fail to get Table via catalog: %s.", catalog.getClass()), e);
        }
    }
}
