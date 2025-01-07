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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Configuration options for sinks. */
@PublicEvolving
public class SinkOptions {
    /**
     * The number of retries on a committable (e.g., transaction) before Flink application fails and
     * potentially restarts.
     */
    @Documentation.Section(Documentation.Sections.COMMON_MISCELLANEOUS)
    public static final ConfigOption<Integer> COMMITTER_RETRIES =
            key("sink.committer.retries")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The number of retries a Flink application attempts for committable operations (such as transactions) on retriable errors, as specified by the sink connector, before Flink fails and potentially restarts.");

    private SinkOptions() {}
}
