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

package org.apache.flink.connector.datagen.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Factory for creating configured instances of {@link DataGenTableSource} in a stream environment.
 */
@Internal
public class DataGenTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "datagen";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DataGenConnectorOptions.ROWS_PER_SECOND);
        options.add(DataGenConnectorOptions.NUMBER_OF_ROWS);

        // Placeholder options
        options.add(DataGenConnectorOptions.FIELD_KIND);
        options.add(DataGenConnectorOptions.FIELD_MIN);
        options.add(DataGenConnectorOptions.FIELD_MAX);
        options.add(DataGenConnectorOptions.FIELD_LENGTH);
        options.add(DataGenConnectorOptions.FIELD_START);
        options.add(DataGenConnectorOptions.FIELD_END);

        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = new Configuration();
        context.getCatalogTable().getOptions().forEach(options::setString);

        TableSchema schema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        DataGenerator<?>[] fieldGenerators = new DataGenerator[schema.getFieldCount()];
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        for (int i = 0; i < fieldGenerators.length; i++) {
            String name = schema.getFieldNames()[i];
            DataType type = schema.getFieldDataTypes()[i];

            ConfigOption<String> kind =
                    key(DataGenConnectorOptionsUtil.FIELDS
                                    + "."
                                    + name
                                    + "."
                                    + DataGenConnectorOptionsUtil.KIND)
                            .stringType()
                            .defaultValue(DataGenConnectorOptionsUtil.RANDOM);
            DataGeneratorContainer container =
                    createContainer(name, type, options.get(kind), options);
            fieldGenerators[i] = container.getGenerator();

            optionalOptions.add(kind);
            optionalOptions.addAll(container.getOptions());
        }

        FactoryUtil.validateFactoryOptions(requiredOptions(), optionalOptions, options);

        Set<String> consumedOptionKeys = new HashSet<>();
        consumedOptionKeys.add(CONNECTOR.key());
        consumedOptionKeys.add(DataGenConnectorOptions.ROWS_PER_SECOND.key());
        consumedOptionKeys.add(DataGenConnectorOptions.NUMBER_OF_ROWS.key());
        optionalOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
        FactoryUtil.validateUnconsumedKeys(
                factoryIdentifier(), options.keySet(), consumedOptionKeys);

        String name = context.getObjectIdentifier().toString();
        return new DataGenTableSource(
                fieldGenerators,
                name,
                schema,
                options.get(DataGenConnectorOptions.ROWS_PER_SECOND),
                options.get(DataGenConnectorOptions.NUMBER_OF_ROWS));
    }

    private DataGeneratorContainer createContainer(
            String name, DataType type, String kind, ReadableConfig options) {
        switch (kind) {
            case DataGenConnectorOptionsUtil.RANDOM:
                return type.getLogicalType().accept(new RandomGeneratorVisitor(name, options));
            case DataGenConnectorOptionsUtil.SEQUENCE:
                return type.getLogicalType().accept(new SequenceGeneratorVisitor(name, options));
            default:
                throw new ValidationException("Unsupported generator kind: " + kind);
        }
    }
}
