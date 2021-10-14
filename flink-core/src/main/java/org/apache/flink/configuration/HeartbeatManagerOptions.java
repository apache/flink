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
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import static org.apache.flink.configuration.ConfigOptions.key;

/** The set of configuration options relating to heartbeat manager settings. */
@PublicEvolving
public class HeartbeatManagerOptions {

    /** Time interval for requesting heartbeat from sender side. */
    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> HEARTBEAT_INTERVAL =
            key("heartbeat.interval")
                    .defaultValue(10000L)
                    .withDescription(
                            "Time interval between heartbeat RPC requests from the sender to the receiver side.");

    /** Timeout for requesting and receiving heartbeat for both sender and receiver sides. */
    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> HEARTBEAT_TIMEOUT =
            key("heartbeat.timeout")
                    .defaultValue(50000L)
                    .withDescription(
                            "Timeout for requesting and receiving heartbeats for both sender and receiver sides.");

    private static final String HEARTBEAT_RPC_FAILURE_THRESHOLD_KEY =
            "heartbeat.rpc-failure-threshold";

    @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Integer> HEARTBEAT_RPC_FAILURE_THRESHOLD =
            key(HEARTBEAT_RPC_FAILURE_THRESHOLD_KEY)
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of consecutive failed heartbeat RPCs until a heartbeat target is marked as unreachable. "
                                                    + "Failed heartbeat RPCs can be used to detect dead targets faster because they no longer receive the RPCs. "
                                                    + "The detection time is %s * %s. "
                                                    + "In environments with a flaky network, setting this value too low can produce false positives. "
                                                    + "In this case, we recommend to increase this value, but not higher than %s / %s. "
                                                    + "The mechanism can be disabled by setting this option to %s",
                                            TextElement.code(HEARTBEAT_INTERVAL.key()),
                                            TextElement.code(HEARTBEAT_RPC_FAILURE_THRESHOLD_KEY),
                                            TextElement.code(HEARTBEAT_TIMEOUT.key()),
                                            TextElement.code(HEARTBEAT_INTERVAL.key()),
                                            TextElement.code("-1"))
                                    .build());

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private HeartbeatManagerOptions() {}
}
