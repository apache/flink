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

package org.apache.flink.table.gateway.rest.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Options to configure {@link org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint}. */
@PublicEvolving
public class SqlGatewayRestOptions {

    /** The address that should be used by clients to connect to the sql gateway server. */
    public static final ConfigOption<String> ADDRESS =
            key("address")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The address that should be used by clients to connect to the sql gateway server.");

    /** The address that the sql gateway server binds itself to. */
    public static final ConfigOption<String> BIND_ADDRESS =
            key("bind-address")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The address that the sql gateway server binds itself.");

    /** The port range that the sql gateway server could bind itself to. */
    public static final ConfigOption<String> BIND_PORT =
            key("bind-port")
                    .stringType()
                    .defaultValue("8083")
                    .withDescription(
                            "The port that the sql gateway server binds itself. Accepts a list of ports (“50100,50101”), ranges"
                                    + " (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid"
                                    + " collisions when multiple sql gateway servers are running on the same machine.");

    /** The port that the client connects to. */
    public static final ConfigOption<Integer> PORT =
            key("port")
                    .intType()
                    .defaultValue(8083)
                    .withDescription(
                            String.format(
                                    "The port that the client connects to. If %s has not been specified, then the sql gateway server will bind to this port.",
                                    BIND_PORT.key()));
}
