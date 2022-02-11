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

package org.apache.flink.connector.base.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Optional Options for {@link AsyncDynamicTableSinkFactory} representing fields of {@link
 * org.apache.flink.connector.base.sink.AsyncSinkBase}.
 */
@PublicEvolving
public class AsyncSinkConnectorOptions {

    public static final ConfigOption<Integer> MAX_BATCH_SIZE =
            ConfigOptions.key("sink.batch.max-size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum number of elements that may be passed"
                                    + " in a batch to be written downstream.");

    public static final ConfigOption<Integer> MAX_IN_FLIGHT_REQUESTS =
            ConfigOptions.key("sink.requests.max-inflight")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Request threshold for uncompleted requests before blocking new write requests.");

    public static final ConfigOption<Integer> MAX_BUFFERED_REQUESTS =
            ConfigOptions.key("sink.requests.max-buffered")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum number of buffered records before applying backpressure.");

    public static final ConfigOption<Long> FLUSH_BUFFER_SIZE =
            ConfigOptions.key("sink.flush-buffer.size")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Threshold value in bytes for writer buffer flushing.");

    public static final ConfigOption<Long> FLUSH_BUFFER_TIMEOUT =
            ConfigOptions.key("sink.flush-buffer.timeout")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Threshold time in milliseconds for an element to be in a buffer"
                                    + " before being flushed.");
}
