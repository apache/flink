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

package org.apache.flink.table.api.java.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserFunctionsTypeHelper;
import org.apache.flink.table.operations.JavaDataStreamQueryOperation;
import org.apache.flink.table.operations.OutputConversionModifyOperation;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.FieldInfoUtils;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The implementation for a Java {@link StreamTableEnvironment}. This enables conversions from/to
 * {@link DataStream}.
 *
 * <p>It binds to a given {@link StreamExecutionEnvironment}.
 */
@Internal
public final class StreamTableEnvironmentImpl extends TableEnvironmentImpl implements StreamTableEnvironment {

	private final StreamExecutionEnvironment executionEnvironment;

	public StreamTableEnvironmentImpl(
			CatalogManager catalogManager,
			FunctionCatalog functionCatalog,
			TableConfig tableConfig,
			StreamExecutionEnvironment executionEnvironment,
			Planner planner,
			Executor executor,
			boolean isStreamingMode) {
		super(catalogManager, tableConfig, executor, functionCatalog, planner, isStreamingMode);
		this.executionEnvironment = executionEnvironment;
	}

	public static StreamTableEnvironment create(
			StreamExecutionEnvironment executionEnvironment,
			EnvironmentSettings settings,
			TableConfig tableConfig) {

		if (!settings.isStreamingMode()) {
			throw new TableException(
				"StreamTableEnvironment can not run in batch mode for now, please use TableEnvironment.");
		}

		CatalogManager catalogManager = new CatalogManager(
			settings.getBuiltInCatalogName(),
			new GenericInMemoryCatalog(settings.getBuiltInCatalogName(), settings.getBuiltInDatabaseName()));

		FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);

		Map<String, String> executorProperties = settings.toExecutorProperties();
		Executor executor = lookupExecutor(executorProperties, executionEnvironment);

		Map<String, String> plannerProperties = settings.toPlannerProperties();
		Planner planner = ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
			.create(plannerProperties, executor, tableConfig, functionCatalog, catalogManager);

		return new StreamTableEnvironmentImpl(
			catalogManager,
			functionCatalog,
			tableConfig,
			executionEnvironment,
			planner,
			executor,
			settings.isStreamingMode()
		);
	}

	private static Executor lookupExecutor(
			Map<String, String> executorProperties,
			StreamExecutionEnvironment executionEnvironment) {
		try {
			ExecutorFactory executorFactory = ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
			Method createMethod = executorFactory.getClass()
				.getMethod("create", Map.class, StreamExecutionEnvironment.class);

			return (Executor) createMethod.invoke(
				executorFactory,
				executorProperties,
				executionEnvironment);
		} catch (Exception e) {
			throw new TableException(
				"Could not instantiate the executor. Make sure a planner module is on the classpath",
				e);
		}
	}

	@Override
	public <T> void registerFunction(String name, TableFunction<T> tableFunction) {
		TypeInformation<T> typeInfo = UserFunctionsTypeHelper.getReturnTypeOfTableFunction(tableFunction);

		functionCatalog.registerTableFunction(
			name,
			tableFunction,
			typeInfo
		);
	}

	@Override
	public <T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction) {
		TypeInformation<T> typeInfo = UserFunctionsTypeHelper.getReturnTypeOfAggregateFunction(aggregateFunction);
		TypeInformation<ACC> accTypeInfo = UserFunctionsTypeHelper
			.getAccumulatorTypeOfAggregateFunction(aggregateFunction);

		functionCatalog.registerAggregateFunction(
			name,
			aggregateFunction,
			typeInfo,
			accTypeInfo
		);
	}

	@Override
	public <T, ACC> void registerFunction(String name, TableAggregateFunction<T, ACC> tableAggregateFunction) {
		TypeInformation<T> typeInfo = UserFunctionsTypeHelper.getReturnTypeOfAggregateFunction(
			tableAggregateFunction);
		TypeInformation<ACC> accTypeInfo = UserFunctionsTypeHelper
			.getAccumulatorTypeOfAggregateFunction(tableAggregateFunction);

		functionCatalog.registerAggregateFunction(
			name,
			tableAggregateFunction,
			typeInfo,
			accTypeInfo
		);
	}

	@Override
	public <T> Table fromDataStream(DataStream<T> dataStream) {
		JavaDataStreamQueryOperation<T> queryOperation = asQueryOperation(
			dataStream,
			Optional.empty());

		return createTable(queryOperation);
	}

	@Override
	public <T> Table fromDataStream(DataStream<T> dataStream, String fields) {
		List<Expression> expressions = ExpressionParser.parseExpressionList(fields);
		JavaDataStreamQueryOperation<T> queryOperation = asQueryOperation(
			dataStream,
			Optional.of(expressions));

		return createTable(queryOperation);
	}

	@Override
	public <T> void registerDataStream(String name, DataStream<T> dataStream) {
		registerTable(name, fromDataStream(dataStream));
	}

	@Override
	public <T> void registerDataStream(String name, DataStream<T> dataStream, String fields) {
		registerTable(name, fromDataStream(dataStream, fields));
	}

	@Override
	public <T> DataStream<T> toAppendStream(Table table, Class<T> clazz) {
		return toAppendStream(table, clazz, new StreamQueryConfig());
	}

	@Override
	public <T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo) {
		return toAppendStream(table, typeInfo, new StreamQueryConfig());
	}

	@Override
	public <T> DataStream<T> toAppendStream(
			Table table,
			Class<T> clazz,
			StreamQueryConfig queryConfig) {
		TypeInformation<T> typeInfo = extractTypeInformation(table, clazz);
		return toAppendStream(table, typeInfo, queryConfig);
	}

	@Override
	public <T> DataStream<T> toAppendStream(
			Table table,
			TypeInformation<T> typeInfo,
			StreamQueryConfig queryConfig) {
		OutputConversionModifyOperation modifyOperation = new OutputConversionModifyOperation(
			table.getQueryOperation(),
			TypeConversions.fromLegacyInfoToDataType(typeInfo),
			OutputConversionModifyOperation.UpdateMode.APPEND);
		tableConfig.setIdleStateRetentionTime(
			Time.milliseconds(queryConfig.getMinIdleStateRetentionTime()),
			Time.milliseconds(queryConfig.getMaxIdleStateRetentionTime()));
		return toDataStream(table, modifyOperation);
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz) {
		return toRetractStream(table, clazz, new StreamQueryConfig());
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo) {
		return toRetractStream(table, typeInfo, new StreamQueryConfig());
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(
			Table table,
			Class<T> clazz,
			StreamQueryConfig queryConfig) {
		TypeInformation<T> typeInfo = extractTypeInformation(table, clazz);
		return toRetractStream(table, typeInfo, queryConfig);
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(
			Table table,
			TypeInformation<T> typeInfo,
			StreamQueryConfig queryConfig) {
		OutputConversionModifyOperation modifyOperation = new OutputConversionModifyOperation(
			table.getQueryOperation(),
			wrapWithChangeFlag(typeInfo),
			OutputConversionModifyOperation.UpdateMode.RETRACT);
		tableConfig.setIdleStateRetentionTime(
			Time.milliseconds(queryConfig.getMinIdleStateRetentionTime()),
			Time.milliseconds(queryConfig.getMaxIdleStateRetentionTime()));
		return toDataStream(table, modifyOperation);
	}

	@Override
	public StreamTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return (StreamTableDescriptor) super.connect(connectorDescriptor);
	}

	@Override
	public void sqlUpdate(String stmt, StreamQueryConfig config) {
		tableConfig.setIdleStateRetentionTime(
			Time.milliseconds(config.getMinIdleStateRetentionTime()),
			Time.milliseconds(config.getMaxIdleStateRetentionTime()));
		sqlUpdate(stmt);
	}

	@Override
	public void insertInto(Table table, StreamQueryConfig queryConfig, String sinkPath, String... sinkPathContinued) {
		tableConfig.setIdleStateRetentionTime(
			Time.milliseconds(queryConfig.getMinIdleStateRetentionTime()),
			Time.milliseconds(queryConfig.getMaxIdleStateRetentionTime()));
		insertInto(table, sinkPath, sinkPathContinued);
	}

	/**
	 * This is a temporary workaround for Python API. Python API should not use StreamExecutionEnvironment at all.
	 */
	@Internal
	public StreamExecutionEnvironment execEnv() {
		return executionEnvironment;
	}

	private <T> DataStream<T> toDataStream(Table table, OutputConversionModifyOperation modifyOperation) {
		List<Transformation<?>> transformations = planner.translate(Collections.singletonList(modifyOperation));

		Transformation<T> transformation = getTransformation(table, transformations);

		executionEnvironment.addOperator(transformation);
		return new DataStream<>(executionEnvironment, transformation);
	}

	@Override
	protected void validateTableSource(TableSource<?> tableSource) {
		super.validateTableSource(tableSource);
		validateTimeCharacteristic(TableSourceValidation.hasRowtimeAttribute(tableSource));
	}

	@Override
	protected boolean isEagerOperationTranslation() {
		return true;
	}

	private <T> TypeInformation<T> extractTypeInformation(Table table, Class<T> clazz) {
		try {
			return TypeExtractor.createTypeInfo(clazz);
		} catch (Exception ex) {
			throw new ValidationException(
				String.format(
					"Could not convert query: %s to a DataStream of class %s",
					table.getQueryOperation().asSummaryString(),
					clazz.getSimpleName()),
				ex);
		}
	}

	@SuppressWarnings("unchecked")
	private <T> Transformation<T> getTransformation(
		Table table,
		List<Transformation<?>> transformations) {
		if (transformations.size() != 1) {
			throw new TableException(String.format(
				"Expected a single transformation for query: %s\n Got: %s",
				table.getQueryOperation().asSummaryString(),
				transformations));
		}

		return (Transformation<T>) transformations.get(0);
	}

	private <T> DataType wrapWithChangeFlag(TypeInformation<T> outputType) {
		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<Tuple2<Boolean, T>>(Types.BOOLEAN(), outputType);
		return TypeConversions.fromLegacyInfoToDataType(tupleTypeInfo);
	}

	private <T> JavaDataStreamQueryOperation<T> asQueryOperation(
			DataStream<T> dataStream,
			Optional<List<Expression>> fields) {
		TypeInformation<T> streamType = dataStream.getType();

		// get field names and types for all non-replaced fields
		FieldInfoUtils.TypeInfoSchema typeInfoSchema = fields.map(f -> {
			FieldInfoUtils.TypeInfoSchema fieldsInfo = FieldInfoUtils.getFieldsInfo(
				streamType,
				f.toArray(new Expression[0]));

			// check if event-time is enabled
			validateTimeCharacteristic(fieldsInfo.isRowtimeDefined());
			return fieldsInfo;
		}).orElseGet(() -> FieldInfoUtils.getFieldsInfo(streamType));

		return new JavaDataStreamQueryOperation<>(
			dataStream,
			typeInfoSchema.getIndices(),
			typeInfoSchema.toTableSchema());
	}

	private void validateTimeCharacteristic(boolean isRowtimeDefined) {
		if (isRowtimeDefined && executionEnvironment.getStreamTimeCharacteristic() != TimeCharacteristic.EventTime) {
			throw new ValidationException(String.format(
				"A rowtime attribute requires an EventTime time characteristic in stream environment. But is: %s",
				executionEnvironment.getStreamTimeCharacteristic()));
		}
	}
}
