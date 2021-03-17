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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** HBaseSinkOptions contains configuration options used for building an {@link HBaseSink}. */
public class HBaseSinkOptions {

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table in which the sink will store data.");

    public static final ConfigOption<Integer> QUEUE_LIMIT =
            ConfigOptions.key("queue.limit")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("The maximum buffer size before sending data to HBase");

    public static final ConfigOption<Integer> MAX_LATENCY =
            ConfigOptions.key("buffer.timeout.ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The maximum time an element stays in the queue before being flushed.");
}
