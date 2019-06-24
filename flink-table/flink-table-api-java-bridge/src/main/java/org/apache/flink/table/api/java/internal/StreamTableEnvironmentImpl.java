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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.PlannerFactory;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
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

	private StreamTableEnvironmentImpl(
			CatalogManager catalogManager,
			FunctionCatalog functionCatalog,
			TableConfig tableConfig,
			StreamExecutionEnvironment executionEnvironment,
			Planner planner,
			Executor executor) {
		super(catalogManager, tableConfig, executor, functionCatalog, planner);
		this.executionEnvironment = executionEnvironment;
	}

	/**
	 * Creates an instance of a {@link StreamTableEnvironment}. It uses the {@link StreamExecutionEnvironment} for
	 * executing queries. This is also the {@link StreamExecutionEnvironment} that will be used when converting
	 * from/to {@link DataStream}.
	 *
	 * @param tableConfig The configuration of the TableEnvironment.
	 * @param executionEnvironment The {@link StreamExecutionEnvironment} of the TableEnvironment.
	 */
	public static StreamTableEnvironmentImpl create(
			TableConfig tableConfig,
			StreamExecutionEnvironment executionEnvironment) {
		CatalogManager catalogManager = new CatalogManager(
			tableConfig.getBuiltInCatalogName(),
			new GenericInMemoryCatalog(tableConfig.getBuiltInCatalogName(), tableConfig.getBuiltInDatabaseName()));
		return create(catalogManager, tableConfig, executionEnvironment);
	}

	/**
	 * Creates an instance of a {@link StreamTableEnvironment}. It uses the {@link StreamExecutionEnvironment} for
	 * executing queries. This is also the {@link StreamExecutionEnvironment} that will be used when converting
	 * from/to {@link DataStream}.
	 *
	 * @param catalogManager The {@link CatalogManager} to use for storing and looking up {@link Table}s.
	 * @param tableConfig The configuration of the TableEnvironment.
	 * @param executionEnvironment The {@link StreamExecutionEnvironment} of the TableEnvironment.
	 */
	public static StreamTableEnvironmentImpl create(
			CatalogManager catalogManager,
			TableConfig tableConfig,
			StreamExecutionEnvironment executionEnvironment) {
		FunctionCatalog functionCatalog = new FunctionCatalog(
			catalogManager.getCurrentCatalog(),
			catalogManager.getCurrentDatabase());
		Executor executor = lookupExecutor(executionEnvironment);
		Planner planner = PlannerFactory.lookupPlanner(executor, tableConfig, functionCatalog, catalogManager);
		return new StreamTableEnvironmentImpl(
			catalogManager,
			functionCatalog,
			tableConfig,
			executionEnvironment,
			planner,
			executor
		);
	}

	private static Executor lookupExecutor(StreamExecutionEnvironment executionEnvironment) {
		try {
			Class<?> clazz = Class.forName("org.apache.flink.table.executor.ExecutorFactory");
			Method createMethod = clazz.getMethod("create", StreamExecutionEnvironment.class);

			return (Executor) createMethod.invoke(null, executionEnvironment);
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
		queryConfigProvider.setConfig(queryConfig);
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
		queryConfigProvider.setConfig(queryConfig);
		return toDataStream(table, modifyOperation);
	}

	@Override
	public StreamTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return (StreamTableDescriptor) super.connect(connectorDescriptor);
	}

	/**
	 * This is a temporary workaround for Python API. Python API should not use StreamExecutionEnvironment at all.
	 */
	@Internal
	public StreamExecutionEnvironment execEnv() {
		return executionEnvironment;
	}

	private <T> DataStream<T> toDataStream(Table table, OutputConversionModifyOperation modifyOperation) {
		List<StreamTransformation<?>> transformations = planner.translate(Collections.singletonList(modifyOperation));

		StreamTransformation<T> streamTransformation = getStreamTransformation(table, transformations);

		executionEnvironment.addOperator(streamTransformation);
		return new DataStream<>(executionEnvironment, streamTransformation);
	}

	@Override
	protected void validateTableSource(TableSource<?> tableSource) {
		super.validateTableSource(tableSource);
		validateTimeCharacteristic(TableSourceValidation.hasRowtimeAttribute(tableSource));
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
	private <T> StreamTransformation<T> getStreamTransformation(
		Table table,
		List<StreamTransformation<?>> transformations) {
		if (transformations.size() != 1) {
			throw new TableException(String.format(
				"Expected a single transformation for query: %s\n Got: %s",
				table.getQueryOperation().asSummaryString(),
				transformations));
		}

		return (StreamTransformation<T>) transformations.get(0);
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
