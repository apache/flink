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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JdbcValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_DRIVER;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_LOOKUP_CACHE_TTL;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_PASSWORD;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_FETCH_SIZE;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_COLUMN;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_LOWER_BOUND;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_NUM;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_UPPER_BOUND;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_QUERY;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_TYPE_VALUE_JDBC;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_URL;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_USERNAME;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_WRITE_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_WRITE_FLUSH_MAX_ROWS;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_WRITE_MAX_RETRIES;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link JdbcTableSource} and {@link
 * JdbcUpsertTableSink}.
 */
public class JdbcTableSourceSinkFactory
        implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>> {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_JDBC); // jdbc
        context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // common options
        properties.add(CONNECTOR_DRIVER);
        properties.add(CONNECTOR_URL);
        properties.add(CONNECTOR_TABLE);
        properties.add(CONNECTOR_USERNAME);
        properties.add(CONNECTOR_PASSWORD);
        properties.add(CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT);

        // scan options
        properties.add(CONNECTOR_READ_QUERY);
        properties.add(CONNECTOR_READ_PARTITION_COLUMN);
        properties.add(CONNECTOR_READ_PARTITION_NUM);
        properties.add(CONNECTOR_READ_PARTITION_LOWER_BOUND);
        properties.add(CONNECTOR_READ_PARTITION_UPPER_BOUND);
        properties.add(CONNECTOR_READ_FETCH_SIZE);

        // lookup options
        properties.add(CONNECTOR_LOOKUP_CACHE_MAX_ROWS);
        properties.add(CONNECTOR_LOOKUP_CACHE_TTL);
        properties.add(CONNECTOR_LOOKUP_MAX_RETRIES);

        // sink options
        properties.add(CONNECTOR_WRITE_FLUSH_MAX_ROWS);
        properties.add(CONNECTOR_WRITE_FLUSH_INTERVAL);
        properties.add(CONNECTOR_WRITE_MAX_RETRIES);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        // computed column
        properties.add(SCHEMA + ".#." + EXPR);

        // watermark
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);

        // table constraint
        properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_NAME);
        properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_COLUMNS);

        return properties;
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema schema =
                TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));

        return JdbcTableSource.builder()
                .setOptions(getJdbcOptions(descriptorProperties))
                .setReadOptions(getJdbcReadOptions(descriptorProperties))
                .setLookupOptions(getJdbcLookupOptions(descriptorProperties))
                .setSchema(schema)
                .build();
    }

    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(
            Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema schema =
                TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));

        final JdbcUpsertTableSink.Builder builder =
                JdbcUpsertTableSink.builder()
                        .setOptions(getJdbcOptions(descriptorProperties))
                        .setTableSchema(schema);

        descriptorProperties
                .getOptionalInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS)
                .ifPresent(builder::setFlushMaxSize);
        descriptorProperties
                .getOptionalDuration(CONNECTOR_WRITE_FLUSH_INTERVAL)
                .ifPresent(s -> builder.setFlushIntervalMills(s.toMillis()));
        descriptorProperties
                .getOptionalInt(CONNECTOR_WRITE_MAX_RETRIES)
                .ifPresent(builder::setMaxRetryTimes);

        return builder.build();
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new SchemaValidator(true, false, false).validate(descriptorProperties);
        new JdbcValidator().validate(descriptorProperties);

        return descriptorProperties;
    }

    private JdbcOptions getJdbcOptions(DescriptorProperties descriptorProperties) {
        final String url = descriptorProperties.getString(CONNECTOR_URL);
        final JdbcOptions.Builder builder =
                JdbcOptions.builder()
                        .setDBUrl(url)
                        .setTableName(descriptorProperties.getString(CONNECTOR_TABLE))
                        .setDialect(JdbcDialects.get(url).get());

        descriptorProperties
                .getOptionalDuration(CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT)
                .ifPresent(s -> builder.setConnectionCheckTimeoutSeconds((int) s.getSeconds()));
        descriptorProperties.getOptionalString(CONNECTOR_DRIVER).ifPresent(builder::setDriverName);
        descriptorProperties.getOptionalString(CONNECTOR_USERNAME).ifPresent(builder::setUsername);
        descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).ifPresent(builder::setPassword);

        return builder.build();
    }

    private JdbcReadOptions getJdbcReadOptions(DescriptorProperties descriptorProperties) {
        final Optional<String> query = descriptorProperties.getOptionalString(CONNECTOR_READ_QUERY);
        final Optional<String> partitionColumnName =
                descriptorProperties.getOptionalString(CONNECTOR_READ_PARTITION_COLUMN);
        final Optional<Long> partitionLower =
                descriptorProperties.getOptionalLong(CONNECTOR_READ_PARTITION_LOWER_BOUND);
        final Optional<Long> partitionUpper =
                descriptorProperties.getOptionalLong(CONNECTOR_READ_PARTITION_UPPER_BOUND);
        final Optional<Integer> numPartitions =
                descriptorProperties.getOptionalInt(CONNECTOR_READ_PARTITION_NUM);

        final JdbcReadOptions.Builder builder = JdbcReadOptions.builder();
        if (query.isPresent()) {
            builder.setQuery(query.get());
        }
        if (partitionColumnName.isPresent()) {
            builder.setPartitionColumnName(partitionColumnName.get());
            builder.setPartitionLowerBound(partitionLower.get());
            builder.setPartitionUpperBound(partitionUpper.get());
            builder.setNumPartitions(numPartitions.get());
        }
        descriptorProperties
                .getOptionalInt(CONNECTOR_READ_FETCH_SIZE)
                .ifPresent(builder::setFetchSize);

        return builder.build();
    }

    private JdbcLookupOptions getJdbcLookupOptions(DescriptorProperties descriptorProperties) {
        final JdbcLookupOptions.Builder builder = JdbcLookupOptions.builder();

        descriptorProperties
                .getOptionalLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS)
                .ifPresent(builder::setCacheMaxSize);
        descriptorProperties
                .getOptionalDuration(CONNECTOR_LOOKUP_CACHE_TTL)
                .ifPresent(s -> builder.setCacheExpireMs(s.toMillis()));
        descriptorProperties
                .getOptionalInt(CONNECTOR_LOOKUP_MAX_RETRIES)
                .ifPresent(builder::setMaxRetryTimes);

        return builder.build();
    }
}
