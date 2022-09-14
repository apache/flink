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

package org.apache.flink.connector.base.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BATCH_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/**
 * Abstract Implementation of {@link DynamicTableSinkFactory} having {@link AsyncSinkBase} fields as
 * optional table options defined in {@link AsyncSinkConnectorOptions}.
 */
@PublicEvolving
public abstract class AsyncDynamicTableSinkFactory implements DynamicTableSinkFactory {

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(AsyncSinkConnectorOptions.MAX_BATCH_SIZE);
        options.add(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE);
        options.add(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS);
        options.add(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT);
        options.add(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS);
        return options;
    }

    protected AsyncDynamicTableSinkBuilder<?, ?> addAsyncOptionsToBuilder(
            Properties configuration, AsyncDynamicTableSinkBuilder<?, ?> builder) {
        Optional.ofNullable((Long) configuration.get(FLUSH_BUFFER_SIZE.key()))
                .ifPresent(builder::setMaxBufferSizeInBytes);
        Optional.ofNullable((Long) configuration.get(FLUSH_BUFFER_TIMEOUT.key()))
                .ifPresent(builder::setMaxTimeInBufferMS);
        Optional.ofNullable((Integer) configuration.get(MAX_BATCH_SIZE.key()))
                .ifPresent(builder::setMaxBatchSize);
        Optional.ofNullable((Integer) configuration.get(MAX_BUFFERED_REQUESTS.key()))
                .ifPresent(builder::setMaxBufferedRequests);
        Optional.ofNullable((Integer) configuration.get(MAX_IN_FLIGHT_REQUESTS.key()))
                .ifPresent(builder::setMaxInFlightRequests);
        return builder;
    }

    /**
     * Class providing common table data required by {@link AsyncDynamicTableSinkFactory} to create
     * {@link AsyncDynamicTableSink}.
     */
    @Internal
    public class AsyncDynamicSinkContext {

        private final ReadableConfig tableOptions;
        private final DataType physicalDataType;
        private final FactoryUtil.TableFactoryHelper helper;
        private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
        private final Map<String, String> resolvedOptions;
        private final List<String> partitionKeys;
        private final boolean isPartitioned;

        public AsyncDynamicSinkContext(
                AsyncDynamicTableSinkFactory factory, DynamicTableFactory.Context context) {
            helper = FactoryUtil.createTableFactoryHelper(factory, context);
            tableOptions = helper.getOptions();
            ResolvedCatalogTable catalogTable = context.getCatalogTable();
            resolvedOptions = catalogTable.getOptions();
            physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
            encodingFormat =
                    helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT);
            partitionKeys = catalogTable.getPartitionKeys();
            isPartitioned = catalogTable.isPartitioned();
        }

        public ReadableConfig getTableOptions() {
            return tableOptions;
        }

        public DataType getPhysicalDataType() {
            return physicalDataType;
        }

        public FactoryUtil.TableFactoryHelper getFactoryHelper() {
            return helper;
        }

        public Map<String, String> getResolvedOptions() {
            return resolvedOptions;
        }

        public EncodingFormat<SerializationSchema<RowData>> getEncodingFormat() {
            return encodingFormat;
        }

        public List<String> getPartitionKeys() {
            return partitionKeys;
        }

        public boolean isPartitioned() {
            return isPartitioned;
        }
    }
}
