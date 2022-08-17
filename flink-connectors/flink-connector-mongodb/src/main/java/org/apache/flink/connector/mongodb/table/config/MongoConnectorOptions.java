/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.table.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;

import java.time.Duration;

/**
 * Base options for the MongoDB connector. Needs to be public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
@PublicEvolving
public class MongoConnectorOptions {

    private MongoConnectorOptions() {}

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the connection uri of MongoDB.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the database to read or write of MongoDB.");

    public static final ConfigOption<String> COLLECTION =
            ConfigOptions.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the collection to read or write of MongoDB.");

    public static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            ConfigOptions.key("scan.fetch-size")
                    .intType()
                    .defaultValue(2048)
                    .withDescription(
                            "Gives the reader a hint as to the number of documents that should be fetched from the database per round-trip when reading. ");

    public static final ConfigOption<Integer> SCAN_CURSOR_BATCH_SIZE =
            ConfigOptions.key("scan.cursor.batch-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Specifies the number of documents to return in each batch of the response from the MongoDB instance. Set to 0 to use server's default.");

    public static final ConfigOption<Boolean> SCAN_CURSOR_NO_TIMEOUT =
            ConfigOptions.key("scan.cursor.no-timeout")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "The server normally times out idle cursors after an inactivity"
                                    + " period (10 minutes) to prevent excess memory use. Set this option to true to prevent that."
                                    + " However, if the application takes longer than 30 minutes to process the current batch of documents,"
                                    + " the session is marked as expired and closed.");

    public static final ConfigOption<PartitionStrategy> SCAN_PARTITION_STRATEGY =
            ConfigOptions.key("scan.partition.strategy")
                    .enumType(PartitionStrategy.class)
                    .defaultValue(PartitionStrategy.DEFAULT)
                    .withDescription(
                            "Specifies the partition strategy. Available strategies are single, sample, split-vector, sharded and default.");

    public static final ConfigOption<MemorySize> SCAN_PARTITION_SIZE =
            ConfigOptions.key("scan.partition.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription("Specifies the partition memory size.");

    public static final ConfigOption<Integer> SCAN_PARTITION_SAMPLES =
            ConfigOptions.key("scan.partition.samples")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Specifies the the samples count per partition. It only takes effect when the partition strategy is sample.");

    public static final ConfigOption<Integer> BULK_FLUSH_MAX_ACTIONS =
            ConfigOptions.key("sink.bulk-flush.max-actions")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Specifies the maximum number of buffered actions per bulk request.");

    public static final ConfigOption<Duration> BULK_FLUSH_INTERVAL =
            ConfigOptions.key("sink.bulk-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Specifies the bulk flush interval.");

    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
            ConfigOptions.key("sink.delivery-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
                    .withDescription("Optional delivery guarantee when committing.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Specifies the max retry times if writing records to database failed.");
}
