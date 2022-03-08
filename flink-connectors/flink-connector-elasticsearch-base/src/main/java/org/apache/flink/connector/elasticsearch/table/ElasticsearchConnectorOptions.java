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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;

import java.time.Duration;
import java.util.List;

/**
 * Base options for the Elasticsearch connector. Needs to be public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
@PublicEvolving
public class ElasticsearchConnectorOptions {

    ElasticsearchConnectorOptions() {}

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
                    .withDescription("Elasticsearch index for every record.");

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

    public static final ConfigOption<String> KEY_DELIMITER_OPTION =
            ConfigOptions.key("document-id.key-delimiter")
                    .stringType()
                    .defaultValue("_")
                    .withDescription(
                            "Delimiter for composite keys e.g., \"$\" would result in IDs \"KEY1$KEY2$KEY3\".");

    public static final ConfigOption<Integer> BULK_FLUSH_MAX_ACTIONS_OPTION =
            ConfigOptions.key("sink.bulk-flush.max-actions")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum number of actions to buffer for each bulk request.");

    public static final ConfigOption<MemorySize> BULK_FLUSH_MAX_SIZE_OPTION =
            ConfigOptions.key("sink.bulk-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription("Maximum size of buffered actions per bulk request");

    public static final ConfigOption<Duration> BULK_FLUSH_INTERVAL_OPTION =
            ConfigOptions.key("sink.bulk-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Bulk flush interval");

    public static final ConfigOption<FlushBackoffType> BULK_FLUSH_BACKOFF_TYPE_OPTION =
            ConfigOptions.key("sink.bulk-flush.backoff.strategy")
                    .enumType(FlushBackoffType.class)
                    .noDefaultValue()
                    .withDescription("Backoff strategy");

    public static final ConfigOption<Integer> BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION =
            ConfigOptions.key("sink.bulk-flush.backoff.max-retries")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Maximum number of retries.");

    public static final ConfigOption<Duration> BULK_FLUSH_BACKOFF_DELAY_OPTION =
            ConfigOptions.key("sink.bulk-flush.backoff.delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Delay between each backoff attempt.");

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

    public static final ConfigOption<String> FORMAT_OPTION =
            ConfigOptions.key("format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "The format must produce a valid JSON document. "
                                    + "Please refer to the documentation on formats for more details.");

    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE_OPTION =
            ConfigOptions.key("sink.delivery-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.NONE)
                    .withDescription("Optional delivery guarantee when committing.");
}
