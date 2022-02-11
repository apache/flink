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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;

import org.apache.hadoop.hive.conf.HiveConf;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Options in {@link HiveConf}. Options are start with {@link
 * CatalogPropertiesUtil#FLINK_PROPERTY_PREFIX}.
 */
public class HiveConfOptions {

    public static final ConfigOption<Duration> LOCK_CHECK_MAX_SLEEP =
            key("flink.hive.lock-check-max-sleep")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(8))
                    .withDescription("The maximum sleep time when retrying to check the lock.");

    public static final ConfigOption<Duration> LOCK_ACQUIRE_TIMEOUT =
            key("flink.hive.lock-acquire-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(8))
                    .withDescription("The maximum time to wait for acquiring the lock.");
}
