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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/** This class holds configuration constants used by hive connector. */
public class HiveOptions {

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER =
            key("table.exec.hive.fallback-mapred-reader")
                    .defaultValue(false)
                    .withDescription(
                            "If it is false, using flink native vectorized reader to read orc files; "
                                    + "If it is true, using hadoop mapred record reader to read orc files.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM =
            key("table.exec.hive.infer-source-parallelism")
                    .defaultValue(true)
                    .withDescription(
                            "If is false, parallelism of source are set by config.\n"
                                    + "If is true, source parallelism is inferred according to splits number.\n");

    public static final ConfigOption<Integer> TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX =
            key("table.exec.hive.infer-source-parallelism.max")
                    .defaultValue(1000)
                    .withDescription("Sets max infer parallelism for source operator.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER =
            key("table.exec.hive.fallback-mapred-writer")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If it is false, using flink native writer to write parquet and orc files; "
                                    + "If it is true, using hadoop mapred record writer to write parquet and orc files.");
}
