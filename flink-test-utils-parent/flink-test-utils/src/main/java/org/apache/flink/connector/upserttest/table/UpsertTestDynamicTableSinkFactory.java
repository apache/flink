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

package org.apache.flink.connector.upserttest.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.upserttest.table.UpsertTestConnectorOptions.KEY_FORMAT_OPTION;
import static org.apache.flink.connector.upserttest.table.UpsertTestConnectorOptions.OUTPUT_FILEPATH_OPTION;
import static org.apache.flink.connector.upserttest.table.UpsertTestConnectorOptions.VALUE_FORMAT_OPTION;

/** A {@link DynamicTableSinkFactory} for discovering {@link UpsertTestDynamicTableSink}. */
@Internal
public class UpsertTestDynamicTableSinkFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "upsert-files";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, KEY_FORMAT_OPTION);
        EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                helper.discoverEncodingFormat(
                        SerializationFormatFactory.class, VALUE_FORMAT_OPTION);

        final ReadableConfig tableOptions = helper.getOptions();
        final String outputFilePath = tableOptions.get(OUTPUT_FILEPATH_OPTION);

        return new UpsertTestDynamicTableSink(
                context.getPhysicalRowDataType(),
                keyEncodingFormat,
                valueEncodingFormat,
                outputFilePath);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OUTPUT_FILEPATH_OPTION);
        options.add(KEY_FORMAT_OPTION);
        options.add(VALUE_FORMAT_OPTION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
