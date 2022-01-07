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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.CONNECTION_PATH_PREFIX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.CONNECTION_REQUEST_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.FAIL_ON_MISSING_FIELDS;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.NUMBER_OF_SLICES_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.PASSWORD_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.PIT_KEEP_ALIVE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.SOCKET_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.TIMESTAMP_FORMAT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7SourceOptions.USERNAME_OPTION;
import static org.apache.flink.formats.json.JsonFormatOptionsUtil.ISO_8601;
import static org.apache.flink.formats.json.JsonFormatOptionsUtil.SQL;

/** Factory for creating a {@link Elasticsearch7DynamicSource}. */
@Internal
public class Elasticsearch7DynamicSourceFactory implements DynamicTableSourceFactory {
    public static final String FACTORY_IDENTIFIER = "elasticsearch-7-src";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig tableOptions = helper.getOptions();

        helper.validate();

        validateTableSourceOptions(tableOptions);

        DataType physicalRowDataType = context.getPhysicalRowDataType();

        Elasticsearch7DymamicSourceConfiguration sourceConfig =
                new Elasticsearch7DymamicSourceConfiguration(
                        Configuration.fromMap(context.getCatalogTable().getOptions()));

        final boolean failOnMissingFields = tableOptions.get(FAIL_ON_MISSING_FIELDS);
        final boolean ignoreParseErrors = tableOptions.get(IGNORE_PARSE_ERRORS);
        TimestampFormat timestampFormat = getTimestampFormat(tableOptions.get(TIMESTAMP_FORMAT));

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
            Elasticsearch7DymamicSourceConfiguration config,
            String tableIdentifier,
            boolean failOnMissingFields,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {

        return new Elasticsearch7DynamicSource(
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
        return Stream.of(
                        NUMBER_OF_SLICES_OPTION,
                        PIT_KEEP_ALIVE_OPTION,
                        FAIL_ON_MISSING_FIELDS,
                        IGNORE_PARSE_ERRORS,
                        TIMESTAMP_FORMAT,
                        PASSWORD_OPTION,
                        USERNAME_OPTION,
                        CONNECTION_PATH_PREFIX_OPTION,
                        CONNECTION_REQUEST_TIMEOUT,
                        CONNECTION_TIMEOUT,
                        SOCKET_TIMEOUT)
                .collect(Collectors.toSet());
    }

    // ------------------------------------------------

    private static TimestampFormat getTimestampFormat(String timestampFormat) {
        switch (timestampFormat) {
            case SQL:
                return TimestampFormat.SQL;
            case ISO_8601:
                return TimestampFormat.ISO_8601;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
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
