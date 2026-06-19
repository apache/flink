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

package org.apache.flink.table.test.program;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Abstract class for {@link SourceTestStep} and {@link SinkTestStep}. */
public abstract class TableTestStep implements TestStep {

    private static final Map<RowKind, String> ROWKIND_TO_STRING_MAP =
            new EnumMap<>(
                    Map.of(
                            RowKind.INSERT,
                            "I",
                            RowKind.UPDATE_BEFORE,
                            "UB",
                            RowKind.UPDATE_AFTER,
                            "UA",
                            RowKind.DELETE,
                            "D"));

    public final String name;
    public final List<String> schemaComponents;
    public final @Nullable TableDistribution distribution;
    public final List<String> partitionKeys;
    public final Map<String, String> options;
    public final List<List<String>> indexes;

    TableTestStep(
            String name,
            List<String> schemaComponents,
            @Nullable TableDistribution distribution,
            List<String> partitionKeys,
            Map<String, String> options,
            List<List<String>> indexes) {
        this.name = name;
        this.schemaComponents = schemaComponents;
        this.distribution = distribution;
        this.partitionKeys = partitionKeys;
        this.options = options;
        this.indexes = indexes;
    }

    public void apply(TableEnvironment env) {
        apply(env, Collections.emptyMap());
    }

    public void apply(TableEnvironment env, Map<String, String> extraOptions) {
        final Map<String, String> allOptions = new HashMap<>(options);
        allOptions.putAll(extraOptions);

        final String distributedBy =
                Optional.ofNullable(distribution).map(d -> d + "\n").orElse("");
        final String partitionedBy =
                partitionKeys.isEmpty()
                        ? ""
                        : "PARTITIONED BY (" + String.join(", ", partitionKeys) + ")\n";
        final String createTable =
                String.format(
                        "CREATE TABLE %s (\n%s)\n%s%sWITH (\n%s)",
                        name,
                        String.join(",\n", schemaComponents),
                        distributedBy,
                        partitionedBy,
                        allOptions.entrySet().stream()
                                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(",\n")));

        env.executeSql(createTable);
        if (indexes.isEmpty()) {
            return;
        }

        Optional<Catalog> currentCatalogOp = env.getCatalog(env.getCurrentCatalog());
        Preconditions.checkState(currentCatalogOp.isPresent());
        Catalog catalog = currentCatalogOp.get();

        String currentDatabaseName = env.getCurrentDatabase();
        ObjectPath tablePath = new ObjectPath(currentDatabaseName, name);
        CatalogTable oldTable;
        try {
            oldTable = (CatalogTable) catalog.getTable(tablePath);
            catalog.dropTable(tablePath, false);
        } catch (TableNotExistException e) {
            throw new IllegalStateException(e);
        }
        Schema schema = oldTable.getUnresolvedSchema();
        Schema.Builder schemaBuilder = Schema.newBuilder().fromSchema(schema);
        indexes.forEach(schemaBuilder::index);
        CatalogTable newTable =
                CatalogTable.newBuilder()
                        .schema(schemaBuilder.build())
                        .comment(oldTable.getComment())
                        .partitionKeys(oldTable.getPartitionKeys())
                        .options(oldTable.getOptions())
                        .snapshot(oldTable.getSnapshot().orElse(null))
                        .distribution(oldTable.getDistribution().orElse(null))
                        .build();
        try {
            catalog.createTable(tablePath, newTable, false);
        } catch (TableAlreadyExistException | DatabaseNotExistException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Builder pattern for {@link SourceTestStep} and {@link SinkTestStep}. */
    @SuppressWarnings("unchecked")
    protected abstract static class AbstractBuilder<
            SpecificBuilder extends AbstractBuilder<SpecificBuilder>> {

        protected final String name;

        protected final List<String> schemaComponents = new ArrayList<>();
        protected @Nullable TableDistribution distribution;
        protected final List<String> partitionKeys = new ArrayList<>();
        protected final Map<String, String> options = new HashMap<>();

        protected AbstractBuilder(String name) {
            this.name = name;
        }

        /**
         * Define the schema like you would in SQL e.g. "my_col INT", "PRIMARY KEY (uid) NOT
         * ENFORCED", or "WATERMARK FOR ts AS ts".
         */
        public SpecificBuilder addSchema(String... schemaComponents) {
            this.schemaComponents.addAll(Arrays.asList(schemaComponents));
            return (SpecificBuilder) this;
        }

        public SpecificBuilder addSchema(List<String> schemaComponents) {
            this.schemaComponents.addAll(schemaComponents);
            return (SpecificBuilder) this;
        }

        /**
         * Unless the test requires a very specific configuration, try to avoid calling this method
         * and fill in options later via {@link TableTestStep#apply(TableEnvironment, Map)}.
         */
        public SpecificBuilder addOptions(Map<String, String> options) {
            this.options.putAll(options);
            return (SpecificBuilder) this;
        }

        /**
         * Unless the test requires a very specific configuration, try to avoid calling this method
         * and fill in options later via {@link TableTestStep#apply(TableEnvironment, Map)}.
         */
        public SpecificBuilder addOption(String key, String value) {
            this.options.put(key, value);
            return (SpecificBuilder) this;
        }

        /**
         * Unless the test requires a very specific configuration, try to avoid calling this method
         * and fill in options later via {@link TableTestStep#apply(TableEnvironment, Map)}.
         */
        public <T> SpecificBuilder addOption(ConfigOption<T> option, String value) {
            this.options.put(option.key(), ConfigurationUtils.convertValue(value, String.class));
            return (SpecificBuilder) this;
        }

        public SpecificBuilder addDistribution(@Nullable TableDistribution distribution) {
            this.distribution = distribution;
            return (SpecificBuilder) this;
        }

        public SpecificBuilder addPartitionKeys(String... partitionKeys) {
            this.partitionKeys.addAll(Arrays.asList(partitionKeys));
            return (SpecificBuilder) this;
        }

        public SpecificBuilder addMode(ChangelogMode mode) {
            this.options.put("connector", "values");
            final String changelogsStr =
                    mode.getContainedKinds().stream()
                            .map(ROWKIND_TO_STRING_MAP::get)
                            .collect(Collectors.joining(","));
            this.options.put("changelog-mode", changelogsStr);
            this.options.put("sink-changelog-mode-enforced", changelogsStr);
            final String keyOnlyDeleteStr = Boolean.valueOf(mode.keyOnlyDeletes()).toString();
            this.options.put("source.produces-delete-by-key", keyOnlyDeleteStr);
            this.options.put("sink.supports-delete-by-key", keyOnlyDeleteStr);
            return (SpecificBuilder) this;
        }
    }
}
