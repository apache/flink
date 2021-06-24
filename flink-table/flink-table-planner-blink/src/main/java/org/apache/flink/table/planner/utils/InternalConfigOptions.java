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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds internal configuration constants used by Flink's table module.
 *
 * <p>NOTE: All option keys in this class must start with "__" and end up with "__", and all options
 * shouldn't expose to users, all options should erase after plan finished.
 */
@Internal
public final class InternalConfigOptions {

    public static final ConfigOption<Long> TABLE_QUERY_START_EPOCH_TIME =
            key("__table.query-start.epoch-time__")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The config used to save the epoch time at query start, this config will be"
                                    + " used by some temporal functions like CURRENT_TIMESTAMP in batch job to make sure"
                                    + " these temporal functions has query-start semantics.");

    public static final ConfigOption<Long> TABLE_QUERY_START_LOCAL_TIME =
            key("__table.query-start.local-time__")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The config used to save the local timestamp at query start, the timestamp value is stored"
                                    + " as UTC+0 milliseconds since epoch for simplification, this config will be used by"
                                    + " some temporal functions like LOCAL_TIMESTAMP in batch job to make sure these"
                                    + " temporal functions has query-start semantics.");
}
