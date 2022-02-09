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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.elasticsearch.source.Elasticsearch7Source;

import java.time.Duration;
import java.util.List;

/**
 * Options for the {@link Elasticsearch7Source}. Needs to be public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
@PublicEvolving
public class Elasticsearch7SourceOptions {
    Elasticsearch7SourceOptions() {}

    public static final ConfigOption<List<String>> HOSTS_OPTION =
            ConfigOptions.key("hosts")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Elasticsearch hosts to connect to.");

    public static final ConfigOption<String> INDEX_OPTION =
            ConfigOptions.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Elasticsearch index to retrieve from.");

    public static final ConfigOption<Integer> NUMBER_OF_SLICES_OPTION =
            ConfigOptions.key("num-slices")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Number of search slices.");

    public static final ConfigOption<Duration> PIT_KEEP_ALIVE_OPTION =
            ConfigOptions.key("pit-keep-alive")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription("Keep-alive duration for Elasticsearch PIT");

    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELDS =
            ConfigOptions.key("fail-on-missing-fields")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to fail if a field is missing or not, false by default.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default.");

    public static final ConfigOption<String> TIMESTAMP_FORMAT =
            ConfigOptions.key("timestamp-format.standard")
                    .stringType()
                    .defaultValue("SQL")
                    .withDescription(
                            "Optional flag to specify timestamp format, SQL by default."
                                    + " Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format."
                                    + " Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

    public static final ConfigOption<String> PASSWORD_OPTION =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password used to connect to Elasticsearch instance.");

    public static final ConfigOption<String> USERNAME_OPTION =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username used to connect to Elasticsearch instance.");

    public static final ConfigOption<String> CONNECTION_PATH_PREFIX_OPTION =
            ConfigOptions.key("connection.path-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prefix string to be added to every REST communication.");

    public static final ConfigOption<Duration> CONNECTION_REQUEST_TIMEOUT =
            ConfigOptions.key("connection.request-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The timeout for requesting a connection from the connection manager.");

    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
            ConfigOptions.key("connection.timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The timeout for establishing a connection.");

    public static final ConfigOption<Duration> SOCKET_TIMEOUT =
            ConfigOptions.key("socket.timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The socket timeout (SO_TIMEOUT) for waiting for data or, put differently,"
                                    + "a maximum period inactivity between two consecutive data packets.");
}
