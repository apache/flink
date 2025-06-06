/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.jdbc;

import org.apache.flink.table.planner.catalog.CatalogSchemaModel;
import org.apache.flink.table.planner.catalog.FlinkSchema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.NameMultimap;
import org.apache.calcite.util.NameSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A concrete implementation of {@link org.apache.calcite.jdbc.CalciteSchema} that maintains minimal
 * state. This is copied from {@link org.apache.calcite.jdbc.CalciteSchema} calcite and modified to
 * support getModel operation.
 */
public class SimpleCalciteSchema extends CalciteSchema {

    // ----- FLINK MODIFICATION START -----
    public static class ModelEntry extends Entry {
        private final CatalogSchemaModel model;

        ModelEntry(CalciteSchema schema, String name, CatalogSchemaModel model) {
            super(schema, name);
            this.model = model;
        }

        public CatalogSchemaModel getModel() {
            return model;
        }
    }

    // ----- FLINK MODIFICATION END -----

    /**
     * Creates a SimpleCalciteSchema.
     *
     * <p>Use {@link CalciteSchema#createRootSchema(boolean)} or {@link #add(String, Schema)}.
     */
    SimpleCalciteSchema(@Nullable CalciteSchema parent, Schema schema, String name) {
        this(parent, schema, name, null, null, null, null, null, null, null, null);
    }

    private SimpleCalciteSchema(
            @Nullable CalciteSchema parent,
            Schema schema,
            String name,
            @Nullable NameMap<CalciteSchema> subSchemaMap,
            @Nullable NameMap<TableEntry> tableMap,
            @Nullable NameMap<LatticeEntry> latticeMap,
            @Nullable NameMap<TypeEntry> typeMap,
            @Nullable NameMultimap<FunctionEntry> functionMap,
            @Nullable NameSet functionNames,
            @Nullable NameMap<FunctionEntry> nullaryFunctionMap,
            @Nullable List<? extends List<String>> path) {
        super(
                parent,
                schema,
                name,
                subSchemaMap,
                tableMap,
                latticeMap,
                typeMap,
                functionMap,
                functionNames,
                nullaryFunctionMap,
                path);
    }

    @Override
    public void setCache(boolean cache) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CalciteSchema add(String name, Schema schema) {
        final CalciteSchema calciteSchema = new SimpleCalciteSchema(this, schema, name);
        subSchemaMap.put(name, calciteSchema);
        return calciteSchema;
    }

    private static @Nullable String caseInsensitiveLookup(Set<String> candidates, String name) {
        // Exact string lookup
        if (candidates.contains(name)) {
            return name;
        }
        // Upper case string lookup
        final String upperCaseName = name.toUpperCase(Locale.ROOT);
        if (candidates.contains(upperCaseName)) {
            return upperCaseName;
        }
        // Lower case string lookup
        final String lowerCaseName = name.toLowerCase(Locale.ROOT);
        if (candidates.contains(lowerCaseName)) {
            return lowerCaseName;
        }
        // Fall through: Set iteration
        for (String candidate : candidates) {
            if (candidate.equalsIgnoreCase(name)) {
                return candidate;
            }
        }
        return null;
    }

    @Override
    protected @Nullable CalciteSchema getImplicitSubSchema(
            String schemaName, boolean caseSensitive) {
        // Check implicit schemas.
        final String schemaName2 =
                caseSensitive
                        ? schemaName
                        : caseInsensitiveLookup(schema.getSubSchemaNames(), schemaName);
        if (schemaName2 == null) {
            return null;
        }
        final Schema s = schema.getSubSchema(schemaName2);
        if (s == null) {
            return null;
        }
        return new SimpleCalciteSchema(this, s, schemaName2);
    }

    @Override
    protected @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
        // Check implicit tables.
        final String tableName2 =
                caseSensitive
                        ? tableName
                        : caseInsensitiveLookup(schema.getTableNames(), tableName);
        if (tableName2 == null) {
            return null;
        }
        final Table table = schema.getTable(tableName2);
        if (table == null) {
            return null;
        }
        return tableEntry(tableName2, table);
    }

    // ----- FLINK MODIFICATION START -----
    public @Nullable ModelEntry getModel(String modelName, boolean caseSensitive) {
        FlinkSchema flinkSchema = (FlinkSchema) schema;
        final String modelName2 =
                caseSensitive
                        ? modelName
                        : caseInsensitiveLookup(flinkSchema.getModelNames(), modelName);
        if (modelName2 == null) {
            return null;
        }
        final CatalogSchemaModel model = flinkSchema.getModel(modelName2);
        if (model == null) {
            return null;
        }
        return new ModelEntry(this, modelName2, model);
    }

    // ----- FLINK MODIFICATION END -----

    @Override
    protected @Nullable TypeEntry getImplicitType(String name, boolean caseSensitive) {
        // Check implicit types.
        final String name2 =
                caseSensitive ? name : caseInsensitiveLookup(schema.getTypeNames(), name);
        if (name2 == null) {
            return null;
        }
        final RelProtoDataType type = schema.getType(name2);
        if (type == null) {
            return null;
        }
        return typeEntry(name2, type);
    }

    @Override
    protected void addImplicitSubSchemaToBuilder(
            ImmutableSortedMap.Builder<String, CalciteSchema> builder) {
        ImmutableSortedMap<String, CalciteSchema> explicitSubSchemas = builder.build();
        for (String schemaName : schema.getSubSchemaNames()) {
            if (explicitSubSchemas.containsKey(schemaName)) {
                // explicit subschema wins.
                continue;
            }
            Schema s = schema.getSubSchema(schemaName);
            if (s != null) {
                CalciteSchema calciteSchema = new SimpleCalciteSchema(this, s, schemaName);
                builder.put(schemaName, calciteSchema);
            }
        }
    }

    @Override
    protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(schema.getTableNames());
    }

    @Override
    protected void addImplicitFunctionsToBuilder(
            ImmutableList.Builder<Function> builder, String name, boolean caseSensitive) {
        Collection<Function> functions = schema.getFunctions(name);
        if (functions != null) {
            builder.addAll(functions);
        }
    }

    @Override
    protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(schema.getFunctionNames());
    }

    @Override
    protected void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
        builder.addAll(schema.getTypeNames());
    }

    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
            ImmutableSortedMap.Builder<String, Table> builder) {
        ImmutableSortedMap<String, Table> explicitTables = builder.build();

        for (String s : schema.getFunctionNames()) {
            // explicit table wins.
            if (explicitTables.containsKey(s)) {
                continue;
            }
            for (Function function : schema.getFunctions(s)) {
                if (function instanceof TableMacro && function.getParameters().isEmpty()) {
                    final Table table = ((TableMacro) function).apply(ImmutableList.of());
                    builder.put(s, table);
                }
            }
        }
    }

    @Override
    protected @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(
            String tableName, boolean caseSensitive) {
        Collection<Function> functions = schema.getFunctions(tableName);
        if (functions != null) {
            for (Function function : functions) {
                if (function instanceof TableMacro && function.getParameters().isEmpty()) {
                    final Table table = ((TableMacro) function).apply(ImmutableList.of());
                    return tableEntry(tableName, table);
                }
            }
        }
        return null;
    }

    @Override
    protected CalciteSchema snapshot(@Nullable CalciteSchema parent, SchemaVersion version) {
        CalciteSchema snapshot =
                new SimpleCalciteSchema(
                        parent,
                        schema.snapshot(version),
                        name,
                        null,
                        tableMap,
                        latticeMap,
                        typeMap,
                        functionMap,
                        functionNames,
                        nullaryFunctionMap,
                        getPath());
        for (CalciteSchema subSchema : subSchemaMap.map().values()) {
            CalciteSchema subSchemaSnapshot = subSchema.snapshot(snapshot, version);
            snapshot.subSchemaMap.put(subSchema.name, subSchemaSnapshot);
        }
        return snapshot;
    }

    @Override
    protected boolean isCacheEnabled() {
        return false;
    }
}
