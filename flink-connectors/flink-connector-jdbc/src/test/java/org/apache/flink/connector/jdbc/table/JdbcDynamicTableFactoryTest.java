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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link JdbcTableSource} and {@link JdbcUpsertTableSink} created by {@link
 * JdbcTableSourceSinkFactory}.
 */
public class JdbcDynamicTableFactoryTest {

    private static final TableSchema schema =
            TableSchema.builder()
                    .field("aaa", DataTypes.INT().notNull())
                    .field("bbb", DataTypes.STRING().notNull())
                    .field("ccc", DataTypes.DOUBLE())
                    .field("ddd", DataTypes.DECIMAL(31, 18))
                    .field("eee", DataTypes.TIMESTAMP(3))
                    .primaryKey("bbb", "aaa")
                    .build();

    @Test
    public void testJdbcCommonProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("driver", "org.apache.derby.jdbc.EmbeddedDriver");
        properties.put("username", "user");
        properties.put("password", "pass");
        properties.put("connection.max-retry-timeout", "120s");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        JdbcOptions options =
                JdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .setDriverName("org.apache.derby.jdbc.EmbeddedDriver")
                        .setUsername("user")
                        .setPassword("pass")
                        .setConnectionCheckTimeoutSeconds(120)
                        .build();
        JdbcLookupOptions lookupOptions =
                JdbcLookupOptions.builder()
                        .setCacheMaxSize(-1)
                        .setCacheExpireMs(10_000)
                        .setMaxRetryTimes(3)
                        .build();
        JdbcDynamicTableSource expectedSource =
                new JdbcDynamicTableSource(
                        options, JdbcReadOptions.builder().build(), lookupOptions, schema);
        assertEquals(expectedSource, actualSource);

        // validation for sink
        DynamicTableSink actualSink = createTableSink(properties);
        // default flush configurations
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(schema.getFieldNames())
                        .withKeyFields("bbb", "aaa")
                        .build();
        JdbcDynamicTableSink expectedSink =
                new JdbcDynamicTableSink(options, executionOptions, dmlOptions, schema);
        assertEquals(expectedSink, actualSink);
    }

    @Test
    public void testJdbcReadProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.partition.column", "aaa");
        properties.put("scan.partition.lower-bound", "-10");
        properties.put("scan.partition.upper-bound", "100");
        properties.put("scan.partition.num", "10");
        properties.put("scan.fetch-size", "20");
        properties.put("scan.auto-commit", "false");

        DynamicTableSource actual = createTableSource(properties);

        JdbcOptions options =
                JdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        JdbcReadOptions readOptions =
                JdbcReadOptions.builder()
                        .setPartitionColumnName("aaa")
                        .setPartitionLowerBound(-10)
                        .setPartitionUpperBound(100)
                        .setNumPartitions(10)
                        .setFetchSize(20)
                        .setAutoCommit(false)
                        .build();
        JdbcLookupOptions lookupOptions =
                JdbcLookupOptions.builder()
                        .setCacheMaxSize(-1)
                        .setCacheExpireMs(10_000)
                        .setMaxRetryTimes(3)
                        .build();
        JdbcDynamicTableSource expected =
                new JdbcDynamicTableSource(options, readOptions, lookupOptions, schema);

        assertEquals(expected, actual);
    }

    @Test
    public void testJdbcLookupProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("lookup.cache.max-rows", "1000");
        properties.put("lookup.cache.ttl", "10s");
        properties.put("lookup.max-retries", "10");

        DynamicTableSource actual = createTableSource(properties);

        JdbcOptions options =
                JdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        JdbcLookupOptions lookupOptions =
                JdbcLookupOptions.builder()
                        .setCacheMaxSize(1000)
                        .setCacheExpireMs(10_000)
                        .setMaxRetryTimes(10)
                        .build();
        JdbcDynamicTableSource expected =
                new JdbcDynamicTableSource(
                        options, JdbcReadOptions.builder().build(), lookupOptions, schema);

        assertEquals(expected, actual);
    }

    @Test
    public void testJdbcSinkProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("sink.buffer-flush.max-rows", "1000");
        properties.put("sink.buffer-flush.interval", "2min");
        properties.put("sink.max-retries", "5");

        DynamicTableSink actual = createTableSink(properties);

        JdbcOptions options =
                JdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(120_000)
                        .withMaxRetries(5)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(schema.getFieldNames())
                        .withKeyFields("bbb", "aaa")
                        .build();

        JdbcDynamicTableSink expected =
                new JdbcDynamicTableSink(options, executionOptions, dmlOptions, schema);

        assertEquals(expected, actual);
    }

    @Test
    public void testJDBCSinkWithParallelism() {
        Map<String, String> properties = getAllOptions();
        properties.put("sink.parallelism", "2");

        DynamicTableSink actual = createTableSink(properties);

        JdbcOptions options =
                JdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .setParallelism(2)
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(schema.getFieldNames())
                        .withKeyFields("bbb", "aaa")
                        .build();

        JdbcDynamicTableSink expected =
                new JdbcDynamicTableSink(options, executionOptions, dmlOptions, schema);

        assertEquals(expected, actual);
    }

    @Test
    public void testJdbcValidation() {
        // only password, no username
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("password", "pass");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "Either all or none of the following options should be provided:\n"
                                            + "username\npassword")
                            .isPresent());
        }

        // read partition properties not complete
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("scan.partition.column", "aaa");
            properties.put("scan.partition.lower-bound", "-10");
            properties.put("scan.partition.upper-bound", "100");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "Either all or none of the following options should be provided:\n"
                                            + "scan.partition.column\n"
                                            + "scan.partition.num\n"
                                            + "scan.partition.lower-bound\n"
                                            + "scan.partition.upper-bound")
                            .isPresent());
        }

        // read partition lower-bound > upper-bound
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("scan.partition.column", "aaa");
            properties.put("scan.partition.lower-bound", "100");
            properties.put("scan.partition.upper-bound", "-10");
            properties.put("scan.partition.num", "10");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "'scan.partition.lower-bound'='100' must not be larger than "
                                            + "'scan.partition.upper-bound'='-10'.")
                            .isPresent());
        }

        // lookup cache properties not complete
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("lookup.cache.max-rows", "10");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "Either all or none of the following options should be provided:\n"
                                            + "lookup.cache.max-rows\n"
                                            + "lookup.cache.ttl")
                            .isPresent());
        }

        // lookup cache properties not complete
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("lookup.cache.ttl", "1s");

            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "Either all or none of the following options should be provided:\n"
                                            + "lookup.cache.max-rows\n"
                                            + "lookup.cache.ttl")
                            .isPresent());
        }

        // lookup retries shouldn't be negative
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("lookup.max-retries", "-1");
            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The value of 'lookup.max-retries' option shouldn't be negative, but is -1.")
                            .isPresent());
        }

        // sink retries shouldn't be negative
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("sink.max-retries", "-1");
            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The value of 'sink.max-retries' option shouldn't be negative, but is -1.")
                            .isPresent());
        }

        // connection.max-retry-timeout shouldn't be smaller than 1 second
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("connection.max-retry-timeout", "100ms");
            createTableSource(properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    t,
                                    "The value of 'connection.max-retry-timeout' option must be in second granularity and shouldn't be smaller than 1 second, but is 100ms.")
                            .isPresent());
        }
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "jdbc");
        options.put("url", "jdbc:derby:memory:mydb");
        options.put("table-name", "mytable");
        return options;
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(JdbcDynamicTableFactoryTest.schema, options, "mock source"),
                new Configuration(),
                JdbcDynamicTableFactoryTest.class.getClassLoader(),
                false);
    }

    private static DynamicTableSink createTableSink(Map<String, String> options) {
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(JdbcDynamicTableFactoryTest.schema, options, "mock sink"),
                new Configuration(),
                JdbcDynamicTableFactoryTest.class.getClassLoader(),
                false);
    }
}
