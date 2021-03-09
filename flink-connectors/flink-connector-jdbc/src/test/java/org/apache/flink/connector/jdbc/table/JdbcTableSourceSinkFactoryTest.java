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

import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test for {@link JdbcTableSource} and {@link JdbcUpsertTableSink} created by {@link
 * JdbcTableSourceSinkFactory}.
 */
public class JdbcTableSourceSinkFactoryTest {

    private static final TableSchema schema =
            TableSchema.builder()
                    .field("aaa", DataTypes.INT())
                    .field("bbb", DataTypes.STRING())
                    .field("ccc", DataTypes.DOUBLE())
                    .field("ddd", DataTypes.DECIMAL(31, 18))
                    .field("eee", DataTypes.TIMESTAMP(3))
                    .build();

    @Test
    public void testJdbcCommonProperties() {
        Map<String, String> properties = getBasicProperties();
        properties.put("connector.driver", "org.apache.derby.jdbc.EmbeddedDriver");
        properties.put("connector.username", "user");
        properties.put("connector.password", "pass");
        properties.put("connector.connection.max-retry-timeout", "120s");

        final StreamTableSource<?> actual =
                TableFactoryService.find(StreamTableSourceFactory.class, properties)
                        .createStreamTableSource(properties);

        final JdbcOptions options =
                JdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .setDriverName("org.apache.derby.jdbc.EmbeddedDriver")
                        .setUsername("user")
                        .setPassword("pass")
                        .setConnectionCheckTimeoutSeconds(120)
                        .build();
        final JdbcTableSource expected =
                JdbcTableSource.builder().setOptions(options).setSchema(schema).build();

        TableSourceValidation.validateTableSource(expected, schema);
        TableSourceValidation.validateTableSource(actual, schema);
        assertEquals(expected, actual);
    }

    @Test
    public void testJdbcReadProperties() {
        Map<String, String> properties = getBasicProperties();
        properties.put("connector.read.query", "SELECT aaa FROM mytable");
        properties.put("connector.read.partition.column", "aaa");
        properties.put("connector.read.partition.lower-bound", "-10");
        properties.put("connector.read.partition.upper-bound", "100");
        properties.put("connector.read.partition.num", "10");
        properties.put("connector.read.fetch-size", "20");

        final StreamTableSource<?> actual =
                TableFactoryService.find(StreamTableSourceFactory.class, properties)
                        .createStreamTableSource(properties);

        final JdbcOptions options =
                JdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        final JdbcReadOptions readOptions =
                JdbcReadOptions.builder()
                        .setQuery("SELECT aaa FROM mytable")
                        .setPartitionColumnName("aaa")
                        .setPartitionLowerBound(-10)
                        .setPartitionUpperBound(100)
                        .setNumPartitions(10)
                        .setFetchSize(20)
                        .build();
        final JdbcTableSource expected =
                JdbcTableSource.builder()
                        .setOptions(options)
                        .setReadOptions(readOptions)
                        .setSchema(schema)
                        .build();

        assertEquals(expected, actual);
    }

    @Test
    public void testJdbcLookupProperties() {
        Map<String, String> properties = getBasicProperties();
        properties.put("connector.lookup.cache.max-rows", "1000");
        properties.put("connector.lookup.cache.ttl", "10s");
        properties.put("connector.lookup.max-retries", "10");

        final StreamTableSource<?> actual =
                TableFactoryService.find(StreamTableSourceFactory.class, properties)
                        .createStreamTableSource(properties);

        final JdbcOptions options =
                JdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        final JdbcLookupOptions lookupOptions =
                JdbcLookupOptions.builder()
                        .setCacheMaxSize(1000)
                        .setCacheExpireMs(10_000)
                        .setMaxRetryTimes(10)
                        .build();
        final JdbcTableSource expected =
                JdbcTableSource.builder()
                        .setOptions(options)
                        .setLookupOptions(lookupOptions)
                        .setSchema(schema)
                        .build();

        assertEquals(expected, actual);
    }

    @Test
    public void testJdbcSinkProperties() {
        Map<String, String> properties = getBasicProperties();
        properties.put("connector.write.flush.max-rows", "1000");
        properties.put("connector.write.flush.interval", "2min");
        properties.put("connector.write.max-retries", "5");

        final StreamTableSink<?> actual =
                TableFactoryService.find(StreamTableSinkFactory.class, properties)
                        .createStreamTableSink(properties);

        final JdbcOptions options =
                JdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .build();
        final JdbcUpsertTableSink expected =
                JdbcUpsertTableSink.builder()
                        .setOptions(options)
                        .setTableSchema(schema)
                        .setFlushMaxSize(1000)
                        .setFlushIntervalMills(120_000)
                        .setMaxRetryTimes(5)
                        .build();

        assertEquals(expected, actual);
    }

    @Test
    public void testJdbcFieldsProjection() {
        Map<String, String> properties = getBasicProperties();
        properties.put("connector.driver", "org.apache.derby.jdbc.EmbeddedDriver");
        properties.put("connector.username", "user");
        properties.put("connector.password", "pass");

        final TableSource<?> actual =
                ((JdbcTableSource)
                                TableFactoryService.find(StreamTableSourceFactory.class, properties)
                                        .createStreamTableSource(properties))
                        .projectFields(new int[] {0, 2});

        List<DataType> projectedFields = actual.getProducedDataType().getChildren();
        assertEquals(Arrays.asList(DataTypes.INT(), DataTypes.DOUBLE()), projectedFields);

        // test jdbc table source description
        List<String> fieldNames =
                ((RowType) actual.getProducedDataType().getLogicalType()).getFieldNames();
        String expectedSourceDescription =
                actual.getClass().getSimpleName()
                        + "("
                        + String.join(", ", fieldNames.stream().toArray(String[]::new))
                        + ")";
        assertEquals(expectedSourceDescription, actual.explainSource());
    }

    @Test
    public void testJdbcValidation() {
        // only password, no username
        try {
            Map<String, String> properties = getBasicProperties();
            properties.put("connector.password", "pass");

            TableFactoryService.find(StreamTableSourceFactory.class, properties)
                    .createStreamTableSource(properties);
            fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // read partition properties not complete
        try {
            Map<String, String> properties = getBasicProperties();
            properties.put("connector.read.partition.column", "aaa");
            properties.put("connector.read.partition.lower-bound", "-10");
            properties.put("connector.read.partition.upper-bound", "100");

            TableFactoryService.find(StreamTableSourceFactory.class, properties)
                    .createStreamTableSource(properties);
            fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // read partition lower-bound > upper-bound
        try {
            Map<String, String> properties = getBasicProperties();
            properties.put("connector.read.partition.column", "aaa");
            properties.put("connector.read.partition.lower-bound", "100");
            properties.put("connector.read.partition.upper-bound", "-10");
            properties.put("connector.read.partition.num", "10");

            TableFactoryService.find(StreamTableSourceFactory.class, properties)
                    .createStreamTableSource(properties);
            fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // lookup cache properties not complete
        try {
            Map<String, String> properties = getBasicProperties();
            properties.put("connector.lookup.cache.max-rows", "10");

            TableFactoryService.find(StreamTableSourceFactory.class, properties)
                    .createStreamTableSource(properties);
            fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // lookup cache properties not complete
        try {
            Map<String, String> properties = getBasicProperties();
            properties.put("connector.lookup.cache.ttl", "1s");

            TableFactoryService.find(StreamTableSourceFactory.class, properties)
                    .createStreamTableSource(properties);
            fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // lookup max-retries property less than zero
        try {
            Map<String, String> properties = getBasicProperties();
            properties.put("connector.lookup.max-retries", "-1");

            TableFactoryService.find(StreamTableSourceFactory.class, properties)
                    .createStreamTableSource(properties);
            fail("exception expected");
        } catch (ValidationException ignored) {
        }

        // connection.max-retry-timeout property is smaller than 1 second
        try {
            Map<String, String> properties = getBasicProperties();
            properties.put("connector.connection.max-retry-timeout", "100ms");

            TableFactoryService.find(StreamTableSourceFactory.class, properties)
                    .createStreamTableSource(properties);
            fail("exception expected");
        } catch (ValidationException ignored) {
        }
    }

    private Map<String, String> getBasicProperties() {
        Map<String, String> properties = new HashMap<>();

        properties.put("connector.type", "jdbc");
        properties.put("connector.property-version", "1");

        properties.put("connector.url", "jdbc:derby:memory:mydb");
        properties.put("connector.table", "mytable");

        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(properties);
        descriptorProperties.putTableSchema("schema", schema);

        return new HashMap<>(descriptorProperties.asMap());
    }
}
