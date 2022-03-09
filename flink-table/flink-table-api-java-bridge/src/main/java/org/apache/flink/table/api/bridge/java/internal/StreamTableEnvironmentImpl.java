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

package org.apache.flink.table.api.bridge.java.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.SchemaTranslator;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExpressionParser;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.OutputConversionModifyOperation;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The implementation for a Java {@link StreamTableEnvironment}. This enables conversions from/to
 * {@link DataStream}.
 *
 * <p>It binds to a given {@link StreamExecutionEnvironment}.
 */
@Internal
public final class StreamTableEnvironmentImpl extends AbstractStreamTableEnvironmentImpl
        implements StreamTableEnvironment {

    public StreamTableEnvironmentImpl(
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog,
            TableConfig tableConfig,
            StreamExecutionEnvironment executionEnvironment,
            Planner planner,
            Executor executor,
            boolean isStreamingMode,
            ClassLoader userClassLoader) {
        super(
                catalogManager,
                moduleManager,
                tableConfig,
                executor,
                functionCatalog,
                planner,
                isStreamingMode,
                userClassLoader,
                executionEnvironment);
    }

    public static StreamTableEnvironment create(
            StreamExecutionEnvironment executionEnvironment, EnvironmentSettings settings) {

        // temporary solution until FLINK-15635 is fixed
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        final Executor executor = lookupExecutor(classLoader, executionEnvironment);

        final TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(executor.getConfiguration());
        tableConfig.addConfiguration(settings.getConfiguration());

        final ModuleManager moduleManager = new ModuleManager();

        final CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(classLoader)
                        .config(tableConfig)
                        .defaultCatalog(
                                settings.getBuiltInCatalogName(),
                                new GenericInMemoryCatalog(
                                        settings.getBuiltInCatalogName(),
                                        settings.getBuiltInDatabaseName()))
                        .executionConfig(executionEnvironment.getConfig())
                        .build();

        final FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, catalogManager, moduleManager);

        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor, tableConfig, moduleManager, catalogManager, functionCatalog);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                functionCatalog,
                tableConfig,
                executionEnvironment,
                planner,
                executor,
                settings.isStreamingMode(),
                classLoader);
    }

    @Override
    public <T> void registerFunction(String name, TableFunction<T> tableFunction) {
        TypeInformation<T> typeInfo =
                UserDefinedFunctionHelper.getReturnTypeOfTableFunction(tableFunction);

        functionCatalog.registerTempSystemTableFunction(name, tableFunction, typeInfo);
    }

    @Override
    public <T, ACC> void registerFunction(
            String name, AggregateFunction<T, ACC> aggregateFunction) {
        TypeInformation<T> typeInfo =
                UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(aggregateFunction);
        TypeInformation<ACC> accTypeInfo =
                UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(aggregateFunction);

        functionCatalog.registerTempSystemAggregateFunction(
                name, aggregateFunction, typeInfo, accTypeInfo);
    }

    @Override
    public <T, ACC> void registerFunction(
            String name, TableAggregateFunction<T, ACC> tableAggregateFunction) {
        TypeInformation<T> typeInfo =
                UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(tableAggregateFunction);
        TypeInformation<ACC> accTypeInfo =
                UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(
                        tableAggregateFunction);

        functionCatalog.registerTempSystemAggregateFunction(
                name, tableAggregateFunction, typeInfo, accTypeInfo);
    }

    @Override
    public <T> Table fromDataStream(DataStream<T> dataStream) {
        return fromStreamInternal(dataStream, null, null, ChangelogMode.insertOnly());
    }

    @Override
    public <T> Table fromDataStream(DataStream<T> dataStream, Schema schema) {
        Preconditions.checkNotNull(schema, "Schema must not be null.");
        return fromStreamInternal(dataStream, schema, null, ChangelogMode.insertOnly());
    }

    @Override
    public Table fromChangelogStream(DataStream<Row> dataStream) {
        return fromStreamInternal(dataStream, null, null, ChangelogMode.all());
    }

    @Override
    public Table fromChangelogStream(DataStream<Row> dataStream, Schema schema) {
        Preconditions.checkNotNull(schema, "Schema must not be null.");
        return fromStreamInternal(dataStream, schema, null, ChangelogMode.all());
    }

    @Override
    public Table fromChangelogStream(
            DataStream<Row> dataStream, Schema schema, ChangelogMode changelogMode) {
        Preconditions.checkNotNull(schema, "Schema must not be null.");
        return fromStreamInternal(dataStream, schema, null, changelogMode);
    }

    @Override
    public <T> void createTemporaryView(String path, DataStream<T> dataStream) {
        createTemporaryView(
                path, fromStreamInternal(dataStream, null, path, ChangelogMode.insertOnly()));
    }

    @Override
    public <T> void createTemporaryView(String path, DataStream<T> dataStream, Schema schema) {
        createTemporaryView(
                path, fromStreamInternal(dataStream, schema, path, ChangelogMode.insertOnly()));
    }

    @Override
    public DataStream<Row> toDataStream(Table table) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        // include all columns of the query (incl. metadata and computed columns)
        final DataType sourceType = table.getResolvedSchema().toSourceRowDataType();
        return toDataStream(table, sourceType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataStream<T> toDataStream(Table table, Class<T> targetClass) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        Preconditions.checkNotNull(targetClass, "Target class must not be null.");
        if (targetClass == Row.class) {
            // for convenience, we allow the Row class here as well
            return (DataStream<T>) toDataStream(table);
        }

        return toDataStream(table, DataTypes.of(targetClass));
    }

    @Override
    public <T> DataStream<T> toDataStream(Table table, AbstractDataType<?> targetDataType) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        Preconditions.checkNotNull(targetDataType, "Target data type must not be null.");

        final SchemaTranslator.ProducingResult schemaTranslationResult =
                SchemaTranslator.createProducingResult(
                        getCatalogManager().getDataTypeFactory(),
                        table.getResolvedSchema(),
                        targetDataType);

        return toStreamInternal(table, schemaTranslationResult, ChangelogMode.insertOnly());
    }

    @Override
    public DataStream<Row> toChangelogStream(Table table) {
        Preconditions.checkNotNull(table, "Table must not be null.");

        final SchemaTranslator.ProducingResult schemaTranslationResult =
                SchemaTranslator.createProducingResult(table.getResolvedSchema(), null);

        return toStreamInternal(table, schemaTranslationResult, null);
    }

    @Override
    public DataStream<Row> toChangelogStream(Table table, Schema targetSchema) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        Preconditions.checkNotNull(targetSchema, "Target schema must not be null.");

        final SchemaTranslator.ProducingResult schemaTranslationResult =
                SchemaTranslator.createProducingResult(table.getResolvedSchema(), targetSchema);

        return toStreamInternal(table, schemaTranslationResult, null);
    }

    @Override
    public DataStream<Row> toChangelogStream(
            Table table, Schema targetSchema, ChangelogMode changelogMode) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        Preconditions.checkNotNull(targetSchema, "Target schema must not be null.");
        Preconditions.checkNotNull(changelogMode, "Changelog mode must not be null.");

        final SchemaTranslator.ProducingResult schemaTranslationResult =
                SchemaTranslator.createProducingResult(table.getResolvedSchema(), targetSchema);

        return toStreamInternal(table, schemaTranslationResult, changelogMode);
    }

    @Override
    public StreamStatementSet createStatementSet() {
        return new StreamStatementSetImpl(this);
    }

    @Override
    public <T> Table fromDataStream(DataStream<T> dataStream, String fields) {
        List<Expression> expressions = ExpressionParser.INSTANCE.parseExpressionList(fields);
        return fromDataStream(dataStream, expressions.toArray(new Expression[0]));
    }

    @Override
    public <T> Table fromDataStream(DataStream<T> dataStream, Expression... fields) {
        return createTable(asQueryOperation(dataStream, Optional.of(Arrays.asList(fields))));
    }

    @Override
    public <T> void registerDataStream(String name, DataStream<T> dataStream) {
        createTemporaryView(name, dataStream);
    }

    @Override
    public <T> void registerDataStream(String name, DataStream<T> dataStream, String fields) {
        createTemporaryView(name, dataStream, fields);
    }

    @Override
    public <T> void createTemporaryView(String path, DataStream<T> dataStream, String fields) {
        createTemporaryView(path, fromDataStream(dataStream, fields));
    }

    @Override
    public <T> void createTemporaryView(
            String path, DataStream<T> dataStream, Expression... fields) {
        createTemporaryView(path, fromDataStream(dataStream, fields));
    }

    @Override
    public <T> DataStream<T> toAppendStream(Table table, Class<T> clazz) {
        TypeInformation<T> typeInfo = extractTypeInformation(table, clazz);
        return toAppendStream(table, typeInfo);
    }

    @Override
    public <T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo) {
        OutputConversionModifyOperation modifyOperation =
                new OutputConversionModifyOperation(
                        table.getQueryOperation(),
                        TypeConversions.fromLegacyInfoToDataType(typeInfo),
                        OutputConversionModifyOperation.UpdateMode.APPEND);
        return toStreamInternal(table, modifyOperation);
    }

    @Override
    public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz) {
        TypeInformation<T> typeInfo = extractTypeInformation(table, clazz);
        return toRetractStream(table, typeInfo);
    }

    @Override
    public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(
            Table table, TypeInformation<T> typeInfo) {
        OutputConversionModifyOperation modifyOperation =
                new OutputConversionModifyOperation(
                        table.getQueryOperation(),
                        wrapWithChangeFlag(typeInfo),
                        OutputConversionModifyOperation.UpdateMode.RETRACT);
        return toStreamInternal(table, modifyOperation);
    }

    @Override
    protected void validateTableSource(TableSource<?> tableSource) {
        super.validateTableSource(tableSource);
        validateTimeCharacteristic(TableSourceValidation.hasRowtimeAttribute(tableSource));
    }
}
