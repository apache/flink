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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

class TtlTestConfig {
    private static final ConfigOption<Integer> UPDATE_GENERATOR_SRC_KEYSPACE =
            ConfigOptions.key("update_generator_source.keyspace").defaultValue(100);

    private static final ConfigOption<Long> UPDATE_GENERATOR_SRC_SLEEP_TIME =
            ConfigOptions.key("update_generator_source.sleep_time").defaultValue(0L);

    private static final ConfigOption<Long> UPDATE_GENERATOR_SRC_SLEEP_AFTER_ELEMENTS =
            ConfigOptions.key("update_generator_source.sleep_after_elements").defaultValue(0L);

    private static final ConfigOption<Long> STATE_TTL_VERIFIER_TTL_MILLI =
            ConfigOptions.key("state_ttl_verifier.ttl_milli").defaultValue(1000L);

    private static final ConfigOption<Long> REPORT_STAT_AFTER_UPDATES_NUM =
            ConfigOptions.key("report_stat.after_updates_num").defaultValue(200L);

    final int keySpace;
    final long sleepAfterElements;
    final long sleepTime;
    final Time ttl;
    final long reportStatAfterUpdatesNum;

    private TtlTestConfig(
            int keySpace,
            long sleepAfterElements,
            long sleepTime,
            Time ttl,
            long reportStatAfterUpdatesNum) {
        this.keySpace = keySpace;
        this.sleepAfterElements = sleepAfterElements;
        this.sleepTime = sleepTime;
        this.ttl = ttl;
        this.reportStatAfterUpdatesNum = reportStatAfterUpdatesNum;
    }

    static TtlTestConfig fromArgs(ParameterTool pt) {
        int keySpace =
                pt.getInt(
                        UPDATE_GENERATOR_SRC_KEYSPACE.key(),
                        UPDATE_GENERATOR_SRC_KEYSPACE.defaultValue());
        long sleepAfterElements =
                pt.getLong(
                        UPDATE_GENERATOR_SRC_SLEEP_AFTER_ELEMENTS.key(),
                        UPDATE_GENERATOR_SRC_SLEEP_AFTER_ELEMENTS.defaultValue());
        long sleepTime =
                pt.getLong(
                        UPDATE_GENERATOR_SRC_SLEEP_TIME.key(),
                        UPDATE_GENERATOR_SRC_SLEEP_TIME.defaultValue());
        Time ttl =
                Time.milliseconds(
                        pt.getLong(
                                STATE_TTL_VERIFIER_TTL_MILLI.key(),
                                STATE_TTL_VERIFIER_TTL_MILLI.defaultValue()));
        long reportStatAfterUpdatesNum =
                pt.getLong(
                        REPORT_STAT_AFTER_UPDATES_NUM.key(),
                        REPORT_STAT_AFTER_UPDATES_NUM.defaultValue());
        return new TtlTestConfig(
                keySpace, sleepAfterElements, sleepTime, ttl, reportStatAfterUpdatesNum);
    }
}
