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

import org.apache.hadoop.hive.conf.HiveConf;

import java.lang.reflect.Method;

/** Utils for HiveCatalog. */
public class HiveCatalogUtils {

    public static HiveConf getHiveConf(Catalog catalog) {
        try {
            Method method = catalog.getClass().getMethod("getHiveConf");
            return (HiveConf) method.invoke(catalog);
        } catch (Exception e) {
            throw new TableException("Fail to get Hive catalog.", e);
        }
    }

    public static String getHiveVersion(Catalog catalog) {
        try {
            Method method = catalog.getClass().getMethod("getHiveVersion");
            return (String) method.invoke(catalog);
        } catch (Exception e) {
            throw new TableException("Fail to get Hive catalog.", e);
        }
    }
}
