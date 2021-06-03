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

package org.apache.flink.table.planner.delegation.hive;

/** Some constants needed by the hive parser. */
public class HiveParserConstants {

    public static final String INTERVAL_YEAR_MONTH_TYPE_NAME = "interval_year_month";
    public static final String INTERVAL_DAY_TIME_TYPE_NAME = "interval_day_time";

    /* Constants for Druid storage handler */
    public static final String DRUID_HIVE_STORAGE_HANDLER_ID =
            "org.apache.hadoop.hive.druid.DruidStorageHandler";
}
