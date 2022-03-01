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

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.SINK_PARTITIONER_FIELD_DELIMITER;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.STREAM;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/** Factory for creating {@link KinesisDynamicSource}. */
@Internal
public class KinesisDynamicTableFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "kinesis";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        DataType physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        KinesisConnectorOptionsUtil optionsUtils =
                new KinesisConnectorOptionsUtil(catalogTable.getOptions(), tableOptions);

        // initialize the table format early in order to register its consumedOptionKeys
        // in the TableFactoryHelper, as those are needed for correct option validation
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT);

        // validate the data types of the table options
        helper.validateExcept(optionsUtils.getNonValidatedPrefixes().toArray(new String[0]));
        Properties properties = optionsUtils.getValidatedSourceConfigurations();

        return new KinesisDynamicSource(
                physicalDataType, tableOptions.get(STREAM), properties, decodingFormat);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(STREAM);
        options.add(FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SINK_PARTITIONER);
        options.add(SINK_PARTITIONER_FIELD_DELIMITER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(STREAM, SINK_PARTITIONER, SINK_PARTITIONER_FIELD_DELIMITER)
                .collect(Collectors.toSet());
    }
}
