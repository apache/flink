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

import static org.apache.flink.configuration.ConfigOptions.key;

/** The set of configuration options relating to JMX server. */
@PublicEvolving
public class JMXServerOptions {

    /** Port configured to enable JMX server for metrics and debugging. */
    @Documentation.Section(Documentation.Sections.EXPERT_DEBUGGING_AND_TUNING)
    public static final ConfigOption<String> JMX_SERVER_PORT =
            key("jmx.server.port")
                    .noDefaultValue()
                    .withDescription(
                            new Description.DescriptionBuilder()
                                    .text(
                                            "The port range for the JMX server to start the registry. The "
                                                    + "port config can be a single port: \"9123\", a range of ports: \"50100-50200\", "
                                                    + "or a list of ranges and ports: \"50100-50200,50300-50400,51234\". ")
                                    .linebreak()
                                    .text("This option overrides metrics.reporter.*.port option.")
                                    .build());

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private JMXServerOptions() {}
}
