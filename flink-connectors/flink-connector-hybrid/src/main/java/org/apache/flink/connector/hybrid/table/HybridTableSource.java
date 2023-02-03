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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SupportsGetEndTimestamp;
import org.apache.flink.api.connector.source.SupportsSwitchedStartTimestamp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanSourceAbilityProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.hybrid.table.HybridConnectorOptions.OPTIONAL_SWITCHED_START_POSITION_ENABLED;

/**
 * A {@link ScanTableSource} that connect several numbers child sources to be a mixed hybrid source.
 * See {@link HybridSource}.
 */
public class HybridTableSource
        implements ScanTableSource, SupportsReadingMetadata, SupportsProjectionPushDown {

    private final String tableName;
    private final ResolvedSchema tableSchema;
    private final List<ScanTableSource> childTableSources;
    private final Configuration configuration;

    public HybridTableSource(
            String tableName,
            @Nonnull List<ScanTableSource> childTableSources,
            Configuration configuration,
            ResolvedSchema tableSchema) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        Preconditions.checkArgument(
                childTableSources.size() >= 2, "child table sources must at least 2 sources.");
        this.childTableSources = childTableSources;
        this.configuration = configuration;
    }

    @Override
    public DynamicTableSource copy() {
        return new HybridTableSource(tableName, childTableSources, configuration, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "HybridTableSource";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        Set<RowKind> kinds = new HashSet<>();
        for (ScanTableSource childSource : childTableSources) {
            kinds.addAll(childSource.getChangelogMode().getContainedKinds());
        }
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : kinds) {
            builder.addContainedKind(kind);
        }
        return builder.build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        HybridSource.HybridSourceBuilder<RowData, SplitEnumerator> builder;
        ScanTableSource firstTableSource = childTableSources.get(0);
        ScanTableSource.ScanRuntimeProvider firstProvider =
                validateAndGetProvider(firstTableSource);
        Source<RowData, ?, ?> firstSource = validateAndGetSource(firstProvider);
        builder = HybridSource.builder(firstSource);

        if (configuration.getBoolean(OPTIONAL_SWITCHED_START_POSITION_ENABLED)) {
            // switched start position
            for (int i = 1; i < childTableSources.size(); i++) {
                ScanTableSource nextTableSource = childTableSources.get(i);
                ScanTableSource.ScanRuntimeProvider nextProvider =
                        validateAndGetProvider(nextTableSource);
                Source<RowData, ?, ?> nextSource = validateAndGetSource(nextProvider);
                Boundedness boundedness = nextSource.getBoundedness();
                final Source<RowData, ?, ?> localCopySource;
                try {
                    localCopySource = InstantiationUtil.clone(nextSource);
                } catch (ClassNotFoundException | IOException e) {
                    throw new IllegalStateException("Unable to clone the hybrid child source.", e);
                }
                // builder#addSource below is a serialized-lambda. if we use nextSource, the
                // lambda captured variables will be HybridTableSource and nextSource
                // while nextSource can be serialized, but HybridTableSource can not be
                // serialized, it will cause serialized exception. So here we do a deepCopy to
                // address it.
                builder.addSource(
                        switchContext -> {
                            SplitEnumerator previousEnumerator =
                                    switchContext.getPreviousEnumerator();
                            // how to get and apply timestamp depends on specific enumerator
                            long switchedTimestamp =
                                    validateAndCastSplitEnumerator(previousEnumerator)
                                            .getEndTimestamp();
                            validateAndCastSource(localCopySource)
                                    .applySwitchedStartTimestamp(switchedTimestamp);
                            return localCopySource;
                        },
                        boundedness);
            }
        } else {
            // fixed start position
            for (int i = 1; i < childTableSources.size(); i++) {
                ScanTableSource.ScanRuntimeProvider provider =
                        validateAndGetProvider(childTableSources.get(i));
                builder.addSource(validateAndGetSource(provider));
            }
        }

        return SourceProvider.of(builder.build());
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new HashMap<>();
        tableSchema.getColumns().stream()
                .filter(column -> column instanceof Column.MetadataColumn)
                .forEach(
                        column ->
                                metadataMap.put(
                                        ((Column.MetadataColumn) column)
                                                .getMetadataKey()
                                                .orElse(column.getName()),
                                        column.getDataType()));
        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        // we pass the ddl metadata fields to each child sources, if child source has this
        // metadata then return its value else metadata value is null.
        for (ScanTableSource childSource : childTableSources) {
            Preconditions.checkState(
                    childSource instanceof SupportsReadingMetadata,
                    "The table source %s must implement "
                            + "SupportsReadingMetadata interface to be used in hybrid source.",
                    childSource.getClass().getName());
            ((SupportsReadingMetadata) childSource)
                    .applyReadableMetadata(metadataKeys, producedDataType);
        }
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        for (ScanTableSource childSource : childTableSources) {
            Preconditions.checkState(
                    childSource instanceof SupportsProjectionPushDown,
                    "The table source %s must implement "
                            + "SupportsProjectionPushDown interface to be used in hybrid source.",
                    childSource.getClass().getName());
            ((SupportsProjectionPushDown) childSource)
                    .applyProjection(projectedFields, producedDataType);
        }
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    // ------------------------------------------------------------------------

    private static ScanTableSource.ScanRuntimeProvider validateAndGetProvider(
            ScanTableSource tableSource) {
        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        Preconditions.checkState(
                provider instanceof SourceProvider
                        || provider instanceof DataStreamScanSourceAbilityProvider,
                "Provider %s is not a SourceProvider or DataStreamScanSourceAbilityProvider.",
                provider.getClass().getName());
        return provider;
    }

    private static Source<RowData, ?, ?> validateAndGetSource(
            ScanTableSource.ScanRuntimeProvider provider) {
        if (provider instanceof DataStreamScanSourceAbilityProvider) {
            Object source = ((DataStreamScanSourceAbilityProvider<?>) provider).createSource();
            Preconditions.checkState(
                    source instanceof Source,
                    "Child source %s is not a new Source.",
                    source.getClass().getName());
            return (Source<RowData, ?, ?>) source;
        } else {
            return ((SourceProvider) provider).createSource();
        }
    }

    private static SupportsGetEndTimestamp validateAndCastSplitEnumerator(
            SplitEnumerator splitEnumerator) {
        Preconditions.checkState(
                splitEnumerator instanceof SupportsGetEndTimestamp,
                "The split enumerator %s must implement "
                        + "SupportsGetEndTimestamp interface to be used in hybrid source when %s "
                        + "is true.",
                splitEnumerator.getClass().getName(),
                OPTIONAL_SWITCHED_START_POSITION_ENABLED.key());
        return (SupportsGetEndTimestamp) splitEnumerator;
    }

    private static SupportsSwitchedStartTimestamp validateAndCastSource(Source source) {
        Preconditions.checkState(
                source instanceof SupportsSwitchedStartTimestamp,
                "The dynamic table source %s must implement "
                        + "SupportsSwitchedStartTimestamp interface"
                        + "to be used in hybrid source when %s is true.",
                source.getClass().getName(),
                OPTIONAL_SWITCHED_START_POSITION_ENABLED.key());
        return (SupportsSwitchedStartTimestamp) source;
    }
}
