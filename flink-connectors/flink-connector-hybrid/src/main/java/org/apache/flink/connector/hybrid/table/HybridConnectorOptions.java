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

package org.apache.flink.connector.hybrid.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Hybrid source options. */
public class HybridConnectorOptions {

    public static final String SOURCE_IDENTIFIER_REGEX = "[A-Za-z0-9_]+";
    public static final String SOURCE_IDENTIFIER_DELIMITER = ".";

    public static final ConfigOption<String> SOURCE_IDENTIFIERS =
            ConfigOptions.key("source-identifiers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Use comma delimiter and identifier indicate child sources that need to be concatenated. "
                                    + "e.g. source-identifiers='historical,realtime'");

    public static final ConfigOption<Boolean> OPTIONAL_SWITCHED_START_POSITION_ENABLED =
            ConfigOptions.key("switched-start-position-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable switched start position, default is false for using fixed start position. "
                                    + "If it is true, then hybrid source will call the previous source SplitEnumerator#getEndTimestamp "
                                    + "to get end timestamp and pass to next unbounded streaming source. ");

    private HybridConnectorOptions() {}
}
