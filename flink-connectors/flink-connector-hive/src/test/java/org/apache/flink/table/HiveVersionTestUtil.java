/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table;

import org.apache.flink.table.catalog.hive.client.HiveShimLoader;

/** Util for testing different Hive versions. */
public class HiveVersionTestUtil {
    public static final boolean HIVE_120_OR_LATER =
            HiveShimLoader.getHiveVersion().compareTo(HiveShimLoader.HIVE_VERSION_V1_2_0) >= 0;
    public static final boolean HIVE_110_OR_LATER =
            HiveShimLoader.getHiveVersion().compareTo(HiveShimLoader.HIVE_VERSION_V1_1_0) >= 0;
    public static final boolean HIVE_310_OR_LATER =
            HiveShimLoader.getHiveVersion().compareTo(HiveShimLoader.HIVE_VERSION_V3_1_0) >= 0;
    public static final boolean HIVE_210_OR_LATER =
            HiveShimLoader.getHiveVersion().compareTo(HiveShimLoader.HIVE_VERSION_V2_1_0) >= 0;
    public static final boolean HIVE_220_OR_LATER =
            HiveShimLoader.getHiveVersion().compareTo(HiveShimLoader.HIVE_VERSION_V2_2_0) >= 0;
}
