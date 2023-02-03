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

package org.apache.flink.connector.hybrid.table;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.connector.hybrid.table.HybridConnectorOptions.SOURCE_IDENTIFIERS;
import static org.apache.flink.connector.hybrid.table.HybridConnectorOptions.SOURCE_IDENTIFIER_REGEX;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Factory for creating {@link HybridTableSource} based-on FLIP-27 source API. It means that all
 * hybrid child source must be {@link Source}. Note some limitations:
 *
 * <ol>
 *   <li>1.hybrid source must specify at least 2 child sources.
 *   <li>2.hybrid source works in batch mode * then all child sources must be bounded.
 *   <li>3.the first child source must be bounded (otherwise first child source never end).
 * </ol>
 */
public class HybridTableSourceFactory implements DynamicTableSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HybridTableSourceFactory.class);
    public static final String IDENTIFIER = "hybrid";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final Configuration tableOptions = (Configuration) helper.getOptions();

        String tableName = context.getObjectIdentifier().toString();
        ResolvedCatalogTable hybridCatalogTable = context.getCatalogTable();
        ResolvedSchema tableSchema = hybridCatalogTable.getResolvedSchema();

        // process and check source identifiers
        String sourceIdentifiersStr = tableOptions.get(SOURCE_IDENTIFIERS);
        List<String> sourceIdentifiers = Arrays.asList(sourceIdentifiersStr.split(","));
        sourceIdentifiers.forEach(
                identifier ->
                        Preconditions.checkArgument(
                                identifier.matches(SOURCE_IDENTIFIER_REGEX),
                                "source-identifier pattern must be " + SOURCE_IDENTIFIER_REGEX));
        Preconditions.checkArgument(
                sourceIdentifiers.size() >= 2,
                String.format(
                        "hybrid source '%s' option must specify at least 2 sources",
                        SOURCE_IDENTIFIERS.key()));
        Preconditions.checkArgument(
                sourceIdentifiers.stream().distinct().count() == sourceIdentifiers.size(),
                "each source identifier must not be the same");

        // validate params
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        for (String optionKey : tableOptions.toMap().keySet()) {
            if (sourceIdentifiers.stream().anyMatch(optionKey::startsWith)) {
                ConfigOption<String> childOption = key(optionKey).stringType().noDefaultValue();
                optionalOptions.add(childOption);
            }
        }
        optionalOptions.addAll(optionalOptions());
        FactoryUtil.validateFactoryOptions(requiredOptions(), optionalOptions, tableOptions);

        Set<String> consumedOptionKeys = new HashSet<>();
        consumedOptionKeys.add(CONNECTOR.key());
        consumedOptionKeys.add(SOURCE_IDENTIFIERS.key());
        optionalOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
        FactoryUtil.validateUnconsumedKeys(
                factoryIdentifier(), tableOptions.keySet(), consumedOptionKeys);

        // generate each child table sources & concat sources to hybrid table source
        List<ScanTableSource> childTableSources = new ArrayList<>();
        ClassLoader cl = HybridTableSourceFactory.class.getClassLoader();
        for (String sourceIdentifier : sourceIdentifiers) {
            ResolvedCatalogTable childCatalogTable =
                    hybridCatalogTable.copy(
                            extractChildSourceOptions(
                                    hybridCatalogTable.getOptions(), sourceIdentifier));
            String newTableName =
                    String.join(
                            "_", context.getObjectIdentifier().getObjectName(), sourceIdentifier);
            DynamicTableSource tableSource =
                    FactoryUtil.createDynamicTableSource(
                            null,
                            ObjectIdentifier.of(
                                    context.getObjectIdentifier().getCatalogName(),
                                    context.getObjectIdentifier().getDatabaseName(),
                                    newTableName),
                            childCatalogTable,
                            Collections.emptyMap(),
                            context.getConfiguration(),
                            cl,
                            true);
            childTableSources.add(validateAndCastTableSource(tableSource));
        }

        LOG.info("Generate hybrid child sources with: {}.", childTableSources);

        Preconditions.checkArgument(
                sourceIdentifiers.size() == childTableSources.size(),
                "unmatched source identifiers size and generated child sources size");

        return new HybridTableSource(tableName, childTableSources, tableOptions, tableSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(SOURCE_IDENTIFIERS);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(HybridConnectorOptions.OPTIONAL_SWITCHED_START_POSITION_ENABLED);
        return optionalOptions;
    }

    @VisibleForTesting
    protected Map<String, String> extractChildSourceOptions(
            Map<String, String> originalOptions, String sourceIdentifier) {
        if (originalOptions == null || originalOptions.isEmpty()) {
            return originalOptions;
        }
        String optionPrefix = sourceIdentifier + HybridConnectorOptions.SOURCE_IDENTIFIER_DELIMITER;
        Map<String, String> sourceOptions =
                originalOptions.entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith(optionPrefix))
                        .collect(
                                Collectors.toMap(
                                        entry ->
                                                StringUtils.removeStart(
                                                        entry.getKey(), optionPrefix),
                                        Map.Entry::getValue));
        String connectorType = originalOptions.get(optionPrefix + FactoryUtil.CONNECTOR.key());
        sourceOptions.put(FactoryUtil.CONNECTOR.key(), connectorType);

        return sourceOptions;
    }

    private ScanTableSource validateAndCastTableSource(DynamicTableSource tableSource) {
        Preconditions.checkState(
                tableSource instanceof ScanTableSource,
                "HybridSource child source only support ScanTableSource but %s is not, please "
                        + "check it.",
                tableSource.getClass().getName());
        return (ScanTableSource) tableSource;
    }
}
