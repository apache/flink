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

package org.apache.flink.connector.print.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Options for the Print sink connector. */
@PublicEvolving
public class PrintConnectorOptions {

    public static final ConfigOption<String> PRINT_IDENTIFIER =
            key("print-identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Message that identify print and is prefixed to the output of the value.");

    public static final ConfigOption<Boolean> STANDARD_ERROR =
            key("standard-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "True, if the format should print to standard error instead of standard out.");

    private PrintConnectorOptions() {}
}
