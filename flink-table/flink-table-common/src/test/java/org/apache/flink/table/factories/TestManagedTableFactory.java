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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.TestManagedTableSink;
import org.apache.flink.table.connector.source.CompactPartition;
import org.apache.flink.table.connector.source.CompactPartitions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.TestManagedTableSource;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.table.connector.source.CompactPartitions.deserializeCompactPartitions;
import static org.apache.flink.table.connector.source.CompactPartitions.serializeCompactPartitions;

/** A test {@link ManagedTableFactory}. */
public class TestManagedTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory, ManagedTableFactory {

    public static final String ENRICHED_KEY = "ENRICHED_KEY";

    public static final String ENRICHED_VALUE = "ENRICHED_VALUE";

    public static final Map<ObjectIdentifier, AtomicReference<Map<String, String>>> MANAGED_TABLES =
            new ConcurrentHashMap<>();

    public static final Map<
                    ObjectIdentifier, AtomicReference<Map<CatalogPartitionSpec, List<Path>>>>
            MANAGED_TABLE_FILE_ENTRIES = new ConcurrentHashMap<>();

    private static final ConfigOption<String> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode")
                    .stringType()
                    .defaultValue("I"); // all available "I,UA,UB,D"

    private static final ConfigOption<String> COMPACT_FILE_BASE_PATH =
            ConfigOptions.key("compact.file-base-path").stringType().defaultValue("/foo/bar/dir/");

    private static final ConfigOption<List<String>> COMPACT_FILE_ENTRIES =
            ConfigOptions.key("compact.file-entries").stringType().asList().defaultValues();

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> configOptions = new HashSet<>();
        configOptions.add(CHANGELOG_MODE);
        return configOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Map<String, String> enrichOptions(Context context) {
        Map<String, String> newOptions = new HashMap<>(context.getCatalogTable().getOptions());
        if (MANAGED_TABLES.containsKey(context.getObjectIdentifier())) {
            newOptions.put(ENRICHED_KEY, ENRICHED_VALUE);
        }
        return newOptions;
    }

    @Override
    public void onCreateTable(Context context, boolean ignoreIfExists) {
        MANAGED_TABLES.compute(
                context.getObjectIdentifier(),
                (k, v) -> {
                    if (v != null) {
                        if (v.get() == null) {
                            v.set(context.getCatalogTable().getOptions());
                        } else if (!ignoreIfExists) {
                            throw new TableException("Table exists.");
                        }
                    }
                    return v;
                });
    }

    @Override
    public void onDropTable(Context context, boolean ignoreIfNotExists) {
        AtomicReference<Map<String, String>> reference =
                MANAGED_TABLES.get(context.getObjectIdentifier());
        if (reference != null) {
            Map<String, String> previous = reference.getAndSet(null);
            if (!context.getCatalogTable().getOptions().equals(previous) && !ignoreIfNotExists) {
                throw new TableException("Table does not exist.");
            }
        }
    }

    @Override
    public Map<String, String> onCompactTable(
            Context context, CatalogPartitionSpec catalogPartitionSpec) {
        ObjectIdentifier tableIdentifier = context.getObjectIdentifier();
        ResolvedCatalogTable table = context.getCatalogTable();
        Map<String, String> newOptions = new HashMap<>(table.getOptions());

        resolveCompactFileBasePath(tableIdentifier)
                .ifPresent(s -> newOptions.put(COMPACT_FILE_BASE_PATH.key(), s));

        validateAndResolveCompactFileEntries(tableIdentifier, catalogPartitionSpec)
                .ifPresent(s -> newOptions.put(COMPACT_FILE_ENTRIES.key(), s));
        return newOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ChangelogMode changelogMode = parseChangelogMode(helper.getOptions().get(CHANGELOG_MODE));
        CompactPartitions compactPartitions =
                deserializeCompactPartitions(
                                context.getCatalogTable()
                                        .getOptions()
                                        .getOrDefault(COMPACT_FILE_ENTRIES.key(), ""))
                        .orElse(CompactPartitions.from(Collections.emptyList()));
        return new TestManagedTableSource(context, compactPartitions, changelogMode);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        String basePath = helper.getOptions().get(COMPACT_FILE_BASE_PATH);
        return new TestManagedTableSink(context, new Path(basePath));
    }

    // ~ Tools ------------------------------------------------------------------

    private static ChangelogMode parseChangelogMode(String string) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (String split : string.split(",")) {
            switch (split.trim()) {
                case "I":
                    builder.addContainedKind(RowKind.INSERT);
                    break;
                case "UB":
                    builder.addContainedKind(RowKind.UPDATE_BEFORE);
                    break;
                case "UA":
                    builder.addContainedKind(RowKind.UPDATE_AFTER);
                    break;
                case "D":
                    builder.addContainedKind(RowKind.DELETE);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid ChangelogMode string: " + string);
            }
        }
        return builder.build();
    }

    private static boolean compactedPartition(List<Path> partitionPaths) {
        return partitionPaths != null
                && partitionPaths.size() == 1
                && partitionPaths.get(0).getName().startsWith("compact-");
    }

    private static Optional<String> resolveCompactFileBasePath(ObjectIdentifier tableIdentifier) {
        AtomicReference<Map<CatalogPartitionSpec, List<Path>>> reference =
                MANAGED_TABLE_FILE_ENTRIES.get(tableIdentifier);
        if (reference != null) {
            Map<CatalogPartitionSpec, List<Path>> managedTableFileEntries = reference.get();
            for (Map.Entry<CatalogPartitionSpec, List<Path>> entry :
                    managedTableFileEntries.entrySet()) {
                List<Path> partitionFiles = entry.getValue();
                if (partitionFiles.size() > 0) {
                    Path file = partitionFiles.get(0);
                    LinkedHashMap<String, String> partitionSpec =
                            PartitionPathUtils.extractPartitionSpecFromPath(file);
                    if (partitionSpec.isEmpty()) {
                        return Optional.of(file.getParent().getPath());
                    } else {
                        String tableName = tableIdentifier.asSummaryString();
                        int index = file.getPath().indexOf(tableName);
                        if (index != -1) {
                            return Optional.of(
                                    file.getPath().substring(0, index + tableName.length()));
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    private Optional<String> validateAndResolveCompactFileEntries(
            ObjectIdentifier tableIdentifier, CatalogPartitionSpec unresolvedPartitionSpec) {
        AtomicReference<Map<CatalogPartitionSpec, List<Path>>> reference =
                MANAGED_TABLE_FILE_ENTRIES.get(tableIdentifier);
        if (reference != null) {
            List<CatalogPartitionSpec> resolvedPartitionSpecs = new ArrayList<>();
            List<CompactPartition> partitionFilesToCompact = new ArrayList<>();
            reference
                    .get()
                    .forEach(
                            (resolvedPartitionSpec, partitionPaths) -> {
                                if (resolvedPartitionSpec
                                        .getPartitionSpec()
                                        .entrySet()
                                        .containsAll(
                                                unresolvedPartitionSpec
                                                        .getPartitionSpec()
                                                        .entrySet())) {
                                    resolvedPartitionSpecs.add(resolvedPartitionSpec);
                                    if (!compactedPartition(partitionPaths)) {
                                        partitionFilesToCompact.add(
                                                CompactPartition.of(
                                                        resolvedPartitionSpec, partitionPaths));
                                    }
                                }
                            });
            if (resolvedPartitionSpecs.isEmpty()) {
                throw new ValidationException(
                        String.format("Cannot resolve partition spec %s", unresolvedPartitionSpec));
            }
            return serializeCompactPartitions(CompactPartitions.from(partitionFilesToCompact));
        }
        return Optional.empty();
    }
}
