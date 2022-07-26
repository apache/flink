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

package org.apache.flink.table.endpoint.hive;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.HIVE_CONF_DIR;

/** Config Options for {@code HiveServer2Endpoint}. */
@PublicEvolving
public class HiveServer2EndpointConfigOptions {

    // --------------------------------------------------------------------------------------------
    // Server Options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> THRIFT_HOST =
            ConfigOptions.key("thrift.host")
                    .stringType()
                    .defaultValue("localhost")
                    .withDescription(
                            "The server address of HiveServer2 host to be used for communication."
                                    + "Default is empty, which means the to bind to the localhost. "
                                    + "This is only necessary if the host has multiple network addresses.");

    public static final ConfigOption<Integer> THRIFT_PORT =
            ConfigOptions.key("thrift.port")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("The port of the HiveServer2 endpoint");

    public static final ConfigOption<Integer> THRIFT_WORKER_THREADS_MIN =
            ConfigOptions.key("thrift.worker.threads.min")
                    .intType()
                    .defaultValue(5)
                    .withDescription("The minimum number of Thrift worker threads");

    public static final ConfigOption<Integer> THRIFT_WORKER_THREADS_MAX =
            ConfigOptions.key("thrift.worker.threads.max")
                    .intType()
                    .defaultValue(512)
                    .withDescription("The maximum number of Thrift worker threads");

    public static final ConfigOption<Duration> THRIFT_WORKER_KEEPALIVE_TIME =
            ConfigOptions.key("thrift.worker.keepalive-time")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            "Keepalive time for an idle worker thread. When the number of workers exceeds min workers, "
                                    + "excessive threads are killed after this time interval.");

    public static final ConfigOption<Long> THRIFT_MAX_MESSAGE_SIZE =
            ConfigOptions.key("thrift.max.message.size")
                    .longType()
                    .defaultValue(100 * 1024 * 1024L)
                    .withDescription("Maximum message size in bytes a HS2 server will accept.");

    public static final ConfigOption<Duration> THRIFT_LOGIN_TIMEOUT =
            ConfigOptions.key("thrift.login.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(20))
                    .withDescription("Timeout for Thrift clients during login to HiveServer2");

    public static final ConfigOption<Duration> THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH =
            ConfigOptions.key("thrift.exponential.backoff.slot.length")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription(
                            "Binary exponential backoff slot time for Thrift clients during login to HiveServer2,"
                                    + "for retries until hitting Thrift client timeout");

    // --------------------------------------------------------------------------------------------
    // Hive Catalog Options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> CATALOG_HIVE_CONF_DIR =
            ConfigOptions.key("catalog." + HIVE_CONF_DIR.key())
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "URI to your Hive conf dir containing hive-site.xml. The URI needs to be supported by Hadoop FileSystem. "
                                    + "If the URI is relative, i.e. without a scheme, local file system is assumed. If the option is not specified,"
                                    + " hive-site.xml is searched in class path.");

    public static final ConfigOption<String> CATALOG_NAME =
            ConfigOptions.key("catalog.name")
                    .stringType()
                    .defaultValue("hive")
                    .withDescription("Name for the pre-registered hive catalog.");

    public static final ConfigOption<String> CATALOG_DEFAULT_DATABASE =
            ConfigOptions.key("catalog." + DEFAULT_DATABASE.key())
                    .stringType()
                    .defaultValue(DEFAULT_DATABASE.defaultValue())
                    .withDescription(
                            "The default database to use when the catalog is set as the current catalog.");

    // --------------------------------------------------------------------------------------------
    // Hive Module Options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> MODULE_NAME =
            ConfigOptions.key("module.name")
                    .stringType()
                    .defaultValue("hive")
                    .withDescription("Name for the pre-registered hive module.");
}
