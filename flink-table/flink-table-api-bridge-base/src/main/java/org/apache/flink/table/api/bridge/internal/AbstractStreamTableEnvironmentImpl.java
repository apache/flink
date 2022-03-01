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

package org.apache.flink.table.api.bridge.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.SchemaTranslator;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.StreamExecutorFactory;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.DataStreamQueryOperation;
import org.apache.flink.table.operations.ExternalModifyOperation;
import org.apache.flink.table.operations.ExternalQueryOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.FieldInfoUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Abstract class to implement a {@code StreamTableEnvironment}. */
@Internal
public abstract class AbstractStreamTableEnvironmentImpl extends TableEnvironmentImpl {

    protected final StreamExecutionEnvironment executionEnvironment;

    public AbstractStreamTableEnvironmentImpl(
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            TableConfig tableConfig,
            Executor executor,
            FunctionCatalog functionCatalog,
            Planner planner,
            boolean isStreamingMode,
            ClassLoader userClassLoader,
            StreamExecutionEnvironment executionEnvironment) {
        super(
                catalogManager,
                moduleManager,
                tableConfig,
                executor,
                functionCatalog,
                planner,
                isStreamingMode,
                userClassLoader);
        this.executionEnvironment = executionEnvironment;
    }

    public static Executor lookupExecutor(
            ClassLoader classLoader, StreamExecutionEnvironment executionEnvironment) {
        final ExecutorFactory executorFactory;
        try {
            executorFactory =
                    FactoryUtil.discoverFactory(
                            classLoader, ExecutorFactory.class, ExecutorFactory.DEFAULT_IDENTIFIER);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
        if (executorFactory instanceof StreamExecutorFactory) {
            return ((StreamExecutorFactory) executorFactory).create(executionEnvironment);
        } else {
            throw new TableException(
                    "The resolved ExecutorFactory '"
                            + executorFactory.getClass()
                            + "' doesn't implement StreamExecutorFactory.");
        }
    }

    protected <T> Table fromStreamInternal(
            DataStream<T> dataStream,
            @Nullable Schema schema,
            @Nullable String viewPath,
            ChangelogMode changelogMode) {
        Preconditions.checkNotNull(dataStream, "Data stream must not be null.");
        Preconditions.checkNotNull(changelogMode, "Changelog mode must not be null.");

        if (dataStream.getExecutionEnvironment() != executionEnvironment) {
            throw new ValidationException(
                    "The DataStream's StreamExecutionEnvironment must be identical to the one that "
                            + "has been passed to the StreamTableEnvironment during instantiation.");
        }

        final CatalogManager catalogManager = getCatalogManager();
        final OperationTreeBuilder operationTreeBuilder = getOperationTreeBuilder();

        final SchemaTranslator.ConsumingResult schemaTranslationResult =
                SchemaTranslator.createConsumingResult(
                        catalogManager.getDataTypeFactory(), dataStream.getType(), schema);

        final ResolvedCatalogTable resolvedCatalogTable =
                catalogManager.resolveCatalogTable(
                        new ExternalCatalogTable(schemaTranslationResult.getSchema()));

        final ContextResolvedTable contextResolvedTable;
        if (viewPath != null) {
            UnresolvedIdentifier unresolvedIdentifier = getParser().parseIdentifier(viewPath);
            final ObjectIdentifier objectIdentifier =
                    catalogManager.qualifyIdentifier(unresolvedIdentifier);
            contextResolvedTable =
                    ContextResolvedTable.temporary(objectIdentifier, resolvedCatalogTable);
        } else {
            contextResolvedTable =
                    ContextResolvedTable.anonymous("datastream_source", resolvedCatalogTable);
        }

        final QueryOperation scanOperation =
                new ExternalQueryOperation<>(
                        contextResolvedTable,
                        dataStream,
                        schemaTranslationResult.getPhysicalDataType(),
                        schemaTranslationResult.isTopLevelRecord(),
                        changelogMode);

        final List<String> projections = schemaTranslationResult.getProjections();
        if (projections == null) {
            return createTable(scanOperation);
        }

        final QueryOperation projectOperation =
                operationTreeBuilder.project(
                        projections.stream()
                                .map(ApiExpressionUtils::unresolvedRef)
                                .collect(Collectors.toList()),
                        scanOperation);

        return createTable(projectOperation);
    }

    protected <T> DataStream<T> toStreamInternal(
            Table table,
            SchemaTranslator.ProducingResult schemaTranslationResult,
            @Nullable ChangelogMode changelogMode) {
        final CatalogManager catalogManager = getCatalogManager();
        final OperationTreeBuilder operationTreeBuilder = getOperationTreeBuilder();

        final QueryOperation projectOperation =
                schemaTranslationResult
                        .getProjections()
                        .map(
                                projections ->
                                        operationTreeBuilder.project(
                                                projections.stream()
                                                        .map(ApiExpressionUtils::unresolvedRef)
                                                        .collect(Collectors.toList()),
                                                table.getQueryOperation()))
                        .orElseGet(table::getQueryOperation);

        final ResolvedCatalogTable resolvedCatalogTable =
                catalogManager.resolveCatalogTable(
                        new ExternalCatalogTable(schemaTranslationResult.getSchema()));

        final ExternalModifyOperation modifyOperation =
                new ExternalModifyOperation(
                        ContextResolvedTable.anonymous("datastream_sink", resolvedCatalogTable),
                        projectOperation,
                        changelogMode,
                        schemaTranslationResult
                                .getPhysicalDataType()
                                .orElseGet(
                                        () ->
                                                resolvedCatalogTable
                                                        .getResolvedSchema()
                                                        .toPhysicalRowDataType()));

        return toStreamInternal(table, modifyOperation);
    }

    protected <T> DataStream<T> toStreamInternal(Table table, ModifyOperation modifyOperation) {
        final List<Transformation<?>> transformations =
                planner.translate(Collections.singletonList(modifyOperation));

        final Transformation<T> transformation = getTransformation(table, transformations);
        executionEnvironment.addOperator(transformation);

        // reconfigure whenever planner transformations are added
        executionEnvironment.configure(tableConfig.getConfiguration());

        return new DataStream<>(executionEnvironment, transformation);
    }

    /**
     * This is a temporary workaround for Python API. Python API should not use
     * StreamExecutionEnvironment at all.
     */
    @Internal
    public StreamExecutionEnvironment execEnv() {
        return executionEnvironment;
    }

    protected <T> TypeInformation<T> extractTypeInformation(Table table, Class<T> clazz) {
        try {
            return TypeExtractor.createTypeInfo(clazz);
        } catch (Exception ex) {
            throw new ValidationException(
                    String.format(
                            "Could not convert query: %s to a DataStream of class %s",
                            table.getQueryOperation().asSummaryString(), clazz.getSimpleName()),
                    ex);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> Transformation<T> getTransformation(
            Table table, List<Transformation<?>> transformations) {
        if (transformations.size() != 1) {
            throw new TableException(
                    String.format(
                            "Expected a single transformation for query: %s\n Got: %s",
                            table.getQueryOperation().asSummaryString(), transformations));
        }

        return (Transformation<T>) transformations.get(0);
    }

    protected <T> DataType wrapWithChangeFlag(TypeInformation<T> outputType) {
        TupleTypeInfo tupleTypeInfo =
                new TupleTypeInfo<Tuple2<Boolean, T>>(Types.BOOLEAN(), outputType);
        return TypeConversions.fromLegacyInfoToDataType(tupleTypeInfo);
    }

    protected <T> DataStreamQueryOperation<T> asQueryOperation(
            DataStream<T> dataStream, Optional<List<Expression>> fields) {
        TypeInformation<T> streamType = dataStream.getType();

        // get field names and types for all non-replaced fields
        FieldInfoUtils.TypeInfoSchema typeInfoSchema =
                fields.map(
                                f -> {
                                    FieldInfoUtils.TypeInfoSchema fieldsInfo =
                                            FieldInfoUtils.getFieldsInfo(
                                                    streamType, f.toArray(new Expression[0]));

                                    // check if event-time is enabled
                                    validateTimeCharacteristic(fieldsInfo.isRowtimeDefined());
                                    return fieldsInfo;
                                })
                        .orElseGet(() -> FieldInfoUtils.getFieldsInfo(streamType));

        return new DataStreamQueryOperation<>(
                dataStream, typeInfoSchema.getIndices(), typeInfoSchema.toResolvedSchema());
    }

    protected void validateTimeCharacteristic(boolean isRowtimeDefined) {
        if (isRowtimeDefined
                && executionEnvironment.getStreamTimeCharacteristic()
                        != TimeCharacteristic.EventTime) {
            throw new ValidationException(
                    String.format(
                            "A rowtime attribute requires an EventTime time characteristic in stream environment. But is: %s",
                            executionEnvironment.getStreamTimeCharacteristic()));
        }
    }

    @Override
    protected QueryOperation qualifyQueryOperation(
            ObjectIdentifier identifier, QueryOperation queryOperation) {
        if (queryOperation instanceof DataStreamQueryOperation) {
            DataStreamQueryOperation<?> operation = (DataStreamQueryOperation) queryOperation;
            return new DataStreamQueryOperation<>(
                    identifier,
                    operation.getDataStream(),
                    operation.getFieldIndices(),
                    operation.getResolvedSchema());
        } else {
            return queryOperation;
        }
    }

    public void attachAsDataStream(List<ModifyOperation> modifyOperations) {
        final List<Transformation<?>> transformations = translate(modifyOperations);
        transformations.forEach(executionEnvironment::addOperator);
    }
}
