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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds {@link org.apache.flink.configuration.ConfigOption}s used by table module for
 * materialized table.
 */
@PublicEvolving
public class MaterializedTableConfigOptions {

    private MaterializedTableConfigOptions() {}

    public static final String PARTITION_FIELDS = "partition.fields";
    public static final String DATE_FORMATTER = "date-formatter";

    public static final String SCHEDULE_TIME_DATE_FORMATTER_DEFAULT = "yyyy-MM-dd HH:mm:ss";

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Duration> MATERIALIZED_TABLE_FRESHNESS_THRESHOLD =
            key("materialized-table.refresh-mode.freshness-threshold")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription(
                            "Specifies a time threshold for determining the materialized table refresh mode."
                                    + " If the materialized table defined FRESHNESS is below this threshold, it run in continuous mode."
                                    + " Otherwise, it switches to full refresh mode.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<String> PARTITION_FIELDS_DATE_FORMATTER =
            key(String.format("%s.#.%s", PARTITION_FIELDS, DATE_FORMATTER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies the time partition formatter for the partitioned materialized table, where '#' denotes a string-based partition field name."
                                    + " This serves as a hint to the framework regarding which partition to refresh in full refresh mode.");
}
