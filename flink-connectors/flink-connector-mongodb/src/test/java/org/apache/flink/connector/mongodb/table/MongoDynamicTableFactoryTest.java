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

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link MongoDynamicTableSource} and {@link MongoDynamicTableSink} created by {@link
 * MongoDynamicTableFactory}.
 */
public class MongoDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Arrays.asList("bbb", "aaa")));

    @Test
    public void testMongoCommonProperties() {
        Map<String, String> properties = getRequiredOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);

        MongoConnectionOptions connectionOptions =
                MongoConnectionOptions.builder()
                        .setUri("mongodb://127.0.0.1:27017")
                        .setDatabase("test_db")
                        .setCollection("test_coll")
                        .build();

        MongoDynamicTableSource expectedSource =
                new MongoDynamicTableSource(
                        connectionOptions,
                        MongoReadOptions.builder().build(),
                        null,
                        LookupOptions.MAX_RETRIES.defaultValue(),
                        SCHEMA.toPhysicalRowDataType());
        assertThat(actualSource).isEqualTo(expectedSource);

        // validation for sink
        DynamicTableSink actualSink = createTableSink(SCHEMA, properties);

        MongoWriteOptions writeOptions = MongoWriteOptions.builder().build();
        MongoDynamicTableSink expectedSink =
                new MongoDynamicTableSink(
                        connectionOptions,
                        writeOptions,
                        SCHEMA.toPhysicalRowDataType(),
                        MongoKeyExtractor.createKeyExtractor(SCHEMA));
        assertThat(actualSink).isEqualTo(expectedSink);
    }

    @Test
    public void testMongoReadProperties() {
        Map<String, String> properties = getRequiredOptions();
        properties.put("scan.fetch-size", "1024");
        properties.put("scan.cursor.batch-size", "2048");
        properties.put("scan.cursor.no-timeout", "false");
        properties.put("scan.partition.strategy", "split-vector");
        properties.put("scan.partition.size", "128m");
        properties.put("scan.partition.samples", "5");

        DynamicTableSource actual = createTableSource(SCHEMA, properties);

        MongoConnectionOptions connectionOptions =
                MongoConnectionOptions.builder()
                        .setUri("mongodb://127.0.0.1:27017")
                        .setDatabase("test_db")
                        .setCollection("test_coll")
                        .build();
        MongoReadOptions readOptions =
                MongoReadOptions.builder()
                        .setFetchSize(1024)
                        .setCursorBatchSize(2048)
                        .setNoCursorTimeout(false)
                        .setPartitionStrategy(PartitionStrategy.SPLIT_VECTOR)
                        .setPartitionSize(MemorySize.ofMebiBytes(128))
                        .setSamplesPerPartition(5)
                        .build();

        MongoDynamicTableSource expected =
                new MongoDynamicTableSource(
                        connectionOptions,
                        readOptions,
                        null,
                        LookupOptions.MAX_RETRIES.defaultValue(),
                        SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testMongoLookupProperties() {
        Map<String, String> properties = getRequiredOptions();
        properties.put("lookup.cache", "PARTIAL");
        properties.put("lookup.partial-cache.expire-after-write", "10s");
        properties.put("lookup.partial-cache.expire-after-access", "20s");
        properties.put("lookup.partial-cache.cache-missing-key", "false");
        properties.put("lookup.partial-cache.max-rows", "15213");
        properties.put("lookup.max-retries", "10");

        DynamicTableSource actual = createTableSource(SCHEMA, properties);

        MongoConnectionOptions connectionOptions =
                MongoConnectionOptions.builder()
                        .setUri("mongodb://127.0.0.1:27017")
                        .setDatabase("test_db")
                        .setCollection("test_coll")
                        .build();

        MongoDynamicTableSource expected =
                new MongoDynamicTableSource(
                        connectionOptions,
                        MongoReadOptions.builder().build(),
                        DefaultLookupCache.fromConfig(Configuration.fromMap(properties)),
                        10,
                        SCHEMA.toPhysicalRowDataType());

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testMongoSinkProperties() {
        Map<String, String> properties = getRequiredOptions();
        properties.put("sink.bulk-flush.max-actions", "1001");
        properties.put("sink.bulk-flush.interval", "2min");
        properties.put("sink.delivery-guarantee", "at-least-once");
        properties.put("sink.max-retries", "5");

        DynamicTableSink actual = createTableSink(SCHEMA, properties);

        MongoConnectionOptions connectionOptions =
                MongoConnectionOptions.builder()
                        .setUri("mongodb://127.0.0.1:27017")
                        .setDatabase("test_db")
                        .setCollection("test_coll")
                        .build();
        MongoWriteOptions writeOptions =
                MongoWriteOptions.builder()
                        .setBulkFlushMaxActions(1001)
                        .setBulkFlushIntervalMs(TimeUnit.MINUTES.toMillis(2))
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setMaxRetryTimes(5)
                        .build();

        MongoDynamicTableSink expected =
                new MongoDynamicTableSink(
                        connectionOptions,
                        writeOptions,
                        SCHEMA.toPhysicalRowDataType(),
                        MongoKeyExtractor.createKeyExtractor(SCHEMA));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testMongoSinkWithParallelism() {
        Map<String, String> properties = getRequiredOptions();
        properties.put("sink.parallelism", "2");

        DynamicTableSink actual = createTableSink(SCHEMA, properties);

        MongoConnectionOptions connectionOptions =
                MongoConnectionOptions.builder()
                        .setUri("mongodb://127.0.0.1:27017")
                        .setDatabase("test_db")
                        .setCollection("test_coll")
                        .build();

        MongoWriteOptions writeOptions = MongoWriteOptions.builder().setParallelism(2).build();

        MongoDynamicTableSink expected =
                new MongoDynamicTableSink(
                        connectionOptions,
                        writeOptions,
                        SCHEMA.toPhysicalRowDataType(),
                        MongoKeyExtractor.createKeyExtractor(SCHEMA));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testMongoValidation() {
        // fetch size lower than 1
        Map<String, String> properties = getRequiredOptions();
        properties.put("scan.fetch-size", "0");

        Map<String, String> finalProperties1 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties1))
                .hasStackTraceContaining("The fetch size must be larger than 0.");

        // cursor batch size lower than 0
        properties = getRequiredOptions();
        properties.put("scan.cursor.batch-size", "-1");

        Map<String, String> finalProperties2 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties2))
                .hasStackTraceContaining("The cursor batch size must be larger than or equal to 0");

        // partition memory size lower than 1mb
        properties = getRequiredOptions();
        properties.put("scan.partition.size", "900kb");

        Map<String, String> finalProperties3 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties3))
                .hasStackTraceContaining(
                        "The partition size must be larger than or equals to 1mb.");

        // samples per partition lower than 1
        properties = getRequiredOptions();
        properties.put("scan.partition.samples", "0");

        Map<String, String> finalProperties4 = properties;
        assertThatThrownBy(() -> createTableSource(SCHEMA, finalProperties4))
                .hasStackTraceContaining("The samples per partition must be larger than 0.");

        // sink retries shouldn't be negative
        properties = getRequiredOptions();
        properties.put("sink.max-retries", "-1");
        Map<String, String> finalProperties5 = properties;
        assertThatThrownBy(() -> createTableSink(SCHEMA, finalProperties5))
                .hasStackTraceContaining("The max retry times must be larger than or equal to 0.");

        // sink buffered actions shouldn't be smaller than 1
        properties = getRequiredOptions();
        properties.put("sink.bulk-flush.max-actions", "0");
        Map<String, String> finalProperties6 = properties;
        assertThatThrownBy(() -> createTableSink(SCHEMA, finalProperties6))
                .hasStackTraceContaining("Max number of buffered actions must be larger than 0.");
    }

    private Map<String, String> getRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mongodb");
        options.put("uri", "mongodb://127.0.0.1:27017");
        options.put("database", "test_db");
        options.put("collection", "test_coll");
        return options;
    }
}
