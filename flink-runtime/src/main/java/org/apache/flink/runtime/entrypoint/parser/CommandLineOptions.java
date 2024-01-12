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

package org.apache.flink.runtime.entrypoint.parser;

import org.apache.commons.cli.Option;

/** Container class for command line options. */
public class CommandLineOptions {

    public static final Option CONFIG_DIR_OPTION =
            Option.builder("c")
                    .longOpt("configDir")
                    .required(true)
                    .hasArg(true)
                    .argName("configuration directory")
                    .desc("Directory which contains the configuration file flink-conf.yml.")
                    .build();

    /** @deprecated subsumed by dynamic properties */
    @Deprecated
    public static final Option REST_PORT_OPTION =
            Option.builder("r")
                    .longOpt("webui-port")
                    .required(false)
                    .hasArg(true)
                    .argName("rest port")
                    .desc("Port for the rest endpoint and the web UI.")
                    .build();

    public static final Option REPLACE_KEY_VALUE_OPTION =
            Option.builder("repKV")
                    .argName("key,oldValue,newValue")
                    .longOpt("replaceKeyValue")
                    .numberOfArgs(3)
                    .valueSeparator(',')
                    .desc(
                            "Replace the specified key's value with a new one if it matches the old value.")
                    .build();

    public static final Option REMOVE_KEY_VALUE_OPTION =
            Option.builder("rmKV")
                    .argName("key=value")
                    .longOpt("removeKeyValue")
                    .numberOfArgs(2)
                    .valueSeparator('=')
                    .desc("Remove the specified key-value pairs if it matches the old value.")
                    .build();

    public static final Option REMOVE_KEY_OPTION =
            Option.builder("rmKey")
                    .argName("Key")
                    .longOpt("removeKey")
                    .hasArg(true)
                    .desc("Key to remove from the configuration.")
                    .build();

    public static final Option FLATTEN_CONFIG_OPTION =
            Option.builder("flatten")
                    .argName("flatten configuration")
                    .longOpt("flattenConfig")
                    .hasArg(false)
                    .desc(
                            "If present, the configuration will be output in a flattened format instead of nested YAML.")
                    .build();

    public static final Option DYNAMIC_PROPERTY_OPTION =
            Option.builder("D")
                    .argName("property=value")
                    .numberOfArgs(2)
                    .valueSeparator('=')
                    .desc("use value for given property")
                    .build();

    /** @deprecated subsumed by dynamic properties */
    @Deprecated
    public static final Option HOST_OPTION =
            Option.builder("h")
                    .longOpt("host")
                    .required(false)
                    .hasArg(true)
                    .argName("hostname")
                    .desc("Hostname for the RPC service.")
                    .build();

    /**
     * @deprecated exists only for compatibility with legacy mode. Remove once legacy mode and
     *     execution mode option has been removed.
     */
    @Deprecated
    public static final Option EXECUTION_MODE_OPTION =
            Option.builder("x")
                    .longOpt("executionMode")
                    .required(false)
                    .hasArg(true)
                    .argName("execution mode")
                    .desc("Deprecated option")
                    .build();

    private CommandLineOptions() {}
}
