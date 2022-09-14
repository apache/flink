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

package org.apache.flink.formats.protobuf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** This class holds configuration constants used by protobuf format. */
public class PbFormatOptions {
    public static final ConfigOption<String> MESSAGE_CLASS_NAME =
            ConfigOptions.key("message-class-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Required option to specify the full name of protobuf message class. The protobuf class "
                                    + "must be located in the classpath both in client and task side");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional read flag to skip rows with parse errors instead of failing; false by default.");

    public static final ConfigOption<Boolean> READ_DEFAULT_VALUES =
            ConfigOptions.key("read-default-values")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to read as default values instead of null when some field does not exist in deserialization; default to false."
                                    + "If proto syntax is proto3, this value will be set true forcibly because proto3's standard is to use default values.");
    public static final ConfigOption<String> WRITE_NULL_STRING_LITERAL =
            ConfigOptions.key("write-null-string-literal")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "When serializing to protobuf data, this is the optional config to specify the string literal in protobuf's array/map in case of null values."
                                    + "By default empty string is used.");
}
