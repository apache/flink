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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptionsUtil;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.elasticsearch.table.ElasticsearchSourceOptions.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchSourceOptions.INDEX_OPTION;
import static org.apache.flink.formats.json.JsonFormatOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonFormatOptions.IGNORE_PARSE_ERRORS;

/** Factory for creating a {@link ElasticsearchDynamicSource}. */
@Internal
public class Elasticsearch7DynamicSourceFactory implements DynamicTableSourceFactory {
    public static final String FACTORY_IDENTIFIER = "elasticsearch-7-src";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig tableOptions = helper.getOptions();

        //        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
        //                getDecodingFormat(helper);

        helper.validate(); // TODO: check if we have any properties that should not be validated

        validateTableSourceOptions(tableOptions);

        DataType physicalRowDataType = context.getPhysicalRowDataType();

        ElasticsearchSourceConfig sourceConfig =
                new ElasticsearchSourceConfig(
                        Configuration.fromMap(context.getCatalogTable().getOptions()));

        final boolean failOnMissingFields = tableOptions.get(FAIL_ON_MISSING_FIELD);
        final boolean ignoreParseErrors = tableOptions.get(IGNORE_PARSE_ERRORS);
        TimestampFormat timestampFormat = JsonFormatOptionsUtil.getTimestampFormat(tableOptions);

        return createElasticsearchTableSource(
                physicalRowDataType,
                sourceConfig,
                context.getObjectIdentifier().asSummaryString(),
                failOnMissingFields,
                ignoreParseErrors,
                timestampFormat);
    }

    @VisibleForTesting
    protected DynamicTableSource createElasticsearchTableSource(
            DataType physicalRowDataType,
            ElasticsearchSourceConfig config,
            String tableIdentifier,
            boolean failOnMissingFields,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {

        return new ElasticsearchDynamicSource(
                physicalRowDataType,
                config,
                tableIdentifier,
                failOnMissingFields,
                ignoreParseErrors,
                timestampFormat);
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(HOSTS_OPTION, INDEX_OPTION).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet(); // TODO
    }

    // ------------------------------------------------

    private static DecodingFormat<DeserializationSchema<RowData>> getDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverDecodingFormat(
                DeserializationFormatFactory.class, FactoryUtil.FORMAT);
    }

    private static void validateTableSourceOptions(ReadableConfig tableOptions) {
        if (!tableOptions.getOptional(HOSTS_OPTION).isPresent()) {
            throw new ValidationException("'hosts' must be set.");
        }
        if (!tableOptions.getOptional(INDEX_OPTION).isPresent()) {
            throw new ValidationException("'index' must be set.");
        }
    }
}
