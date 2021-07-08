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
package org.apache.flink.table.api.bridge.scala.internal

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JStreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.catalog._
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.delegation.{Executor, ExecutorFactory, Planner, PlannerFactory}
import org.apache.flink.table.descriptors.{ConnectorDescriptor, StreamTableDescriptor}
import org.apache.flink.table.expressions.{ApiExpressionUtils, Expression}
import org.apache.flink.table.factories.ComponentFactoryService
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction, TableFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations._
import org.apache.flink.table.sources.{TableSource, TableSourceValidation}
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.typeutils.FieldInfoUtils
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

import javax.annotation.Nullable

import java.util
import java.util.{Collections, List => JList, Map => JMap}

import scala.collection.JavaConverters._

/**
  * The implementation for a Scala [[StreamTableEnvironment]]. This enables conversions from/to
  * [[DataStream]]. It is bound to a given [[StreamExecutionEnvironment]].
  */
@Internal
class StreamTableEnvironmentImpl (
    catalogManager: CatalogManager,
    moduleManager: ModuleManager,
    functionCatalog: FunctionCatalog,
    config: TableConfig,
    scalaExecutionEnvironment: StreamExecutionEnvironment,
    planner: Planner,
    executor: Executor,
    isStreaming: Boolean,
    userClassLoader: ClassLoader)
  extends TableEnvironmentImpl(
    catalogManager,
    moduleManager,
    config,
    executor,
    functionCatalog,
    planner,
    isStreaming,
    userClassLoader)
  with org.apache.flink.table.api.bridge.scala.StreamTableEnvironment {

  override def fromDataStream[T](dataStream: DataStream[T]): Table = {
    Preconditions.checkNotNull(dataStream, "Data stream must not be null.")
    fromStreamInternal(dataStream.javaStream, null, null, ChangelogMode.insertOnly())
  }

  override def fromDataStream[T](dataStream: DataStream[T], schema: Schema): Table = {
    Preconditions.checkNotNull(dataStream, "Data stream must not be null.")
    Preconditions.checkNotNull(schema, "Schema must not be null.")
    fromStreamInternal(dataStream.javaStream, schema, null, ChangelogMode.insertOnly())
  }

  override def fromChangelogStream(dataStream: DataStream[Row]): Table = {
    Preconditions.checkNotNull(dataStream, "Data stream must not be null.")
    fromStreamInternal(dataStream.javaStream, null, null, ChangelogMode.all())
  }

  override def fromChangelogStream(dataStream: DataStream[Row], schema: Schema): Table = {
    Preconditions.checkNotNull(dataStream, "Data stream must not be null.")
    Preconditions.checkNotNull(schema, "Schema must not be null.")
    fromStreamInternal(dataStream.javaStream, schema, null, ChangelogMode.all())
  }

  override def fromChangelogStream(
      dataStream: DataStream[Row],
      schema: Schema,
      changelogMode: ChangelogMode)
    : Table = {
    Preconditions.checkNotNull(dataStream, "Data stream must not be null.")
    Preconditions.checkNotNull(schema, "Schema must not be null.")
    fromStreamInternal(dataStream.javaStream, schema, null, changelogMode)
  }

  override def createTemporaryView[T](
      path: String,
      dataStream: DataStream[T]): Unit = {
    Preconditions.checkNotNull(dataStream, "Data stream must not be null.")
    createTemporaryView(
      path,
      fromStreamInternal(dataStream.javaStream, null, path, ChangelogMode.insertOnly()))
  }

  override def createTemporaryView[T](
      path: String,
      dataStream: DataStream[T],
      schema: Schema): Unit = {
    Preconditions.checkNotNull(dataStream, "Data stream must not be null.")
    Preconditions.checkNotNull(schema, "Schema must not be null.")
    createTemporaryView(
      path,
      fromStreamInternal(dataStream.javaStream, schema, path, ChangelogMode.insertOnly()))
  }

  private def fromStreamInternal[T](
      dataStream: JDataStream[T],
      @Nullable schema: Schema,
      @Nullable viewPath: String,
      changelogMode: ChangelogMode): Table = {
    Preconditions.checkNotNull(changelogMode, "Changelog mode must not be null.")
    val catalogManager = getCatalogManager
    val schemaResolver = catalogManager.getSchemaResolver
    val operationTreeBuilder = getOperationTreeBuilder

    val unresolvedIdentifier = if (viewPath != null) {
      getParser.parseIdentifier(viewPath)
    } else {
      UnresolvedIdentifier.of("Unregistered_DataStream_Source_" + dataStream.getId)
    }
    val objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)

    val schemaTranslationResult =
      ExternalSchemaTranslator.fromExternal(
        catalogManager.getDataTypeFactory, dataStream.getType, schema)

    val resolvedSchema = schemaTranslationResult.getSchema.resolve(schemaResolver)

    val scanOperation =
      new ScalaExternalQueryOperation(
        objectIdentifier,
        dataStream,
        schemaTranslationResult.getPhysicalDataType,
        schemaTranslationResult.isTopLevelRecord,
        changelogMode,
        resolvedSchema)

    val projections = schemaTranslationResult.getProjections
    if (projections == null) {
      return createTable(scanOperation)
    }

    val projectOperation =
      operationTreeBuilder.project(
        util.Arrays.asList(
          projections
            .asScala
            .map(ApiExpressionUtils.unresolvedRef)
            .toArray),
        scanOperation)

    createTable(projectOperation)
  }

  override def toDataStream(table: Table): DataStream[Row] = {
    Preconditions.checkNotNull(table, "Table must not be null.")
    // include all columns of the query (incl. metadata and computed columns)
    val sourceType = table.getResolvedSchema.toSourceRowDataType
    toDataStream(table, sourceType)
  }

  override def toDataStream[T](table: Table, targetClass: Class[T]): DataStream[T] = {
    Preconditions.checkNotNull(table, "Table must not be null.")
    Preconditions.checkNotNull(targetClass, "Target class must not be null.")
    if (targetClass == classOf[Row]) {
      // for convenience, we allow the Row class here as well
      return toDataStream(table).asInstanceOf[DataStream[T]]
    }

    toDataStream(table, DataTypes.of(targetClass))
  }

  override def toDataStream[T](table: Table, targetDataType: AbstractDataType[_]): DataStream[T] = {
    Preconditions.checkNotNull(table, "Table must not be null.")
    Preconditions.checkNotNull(targetDataType, "Target data type must not be null.")

    val schemaTranslationResult = ExternalSchemaTranslator.fromInternal(
      catalogManager.getDataTypeFactory,
      table.getResolvedSchema,
      targetDataType)

    toStreamInternal(table, schemaTranslationResult, ChangelogMode.insertOnly())
  }

  override def toChangelogStream(table: Table): DataStream[Row] = {
    Preconditions.checkNotNull(table, "Table must not be null.")

    val schemaTranslationResult = ExternalSchemaTranslator.fromInternal(
      table.getResolvedSchema,
      null)

    toStreamInternal(table, schemaTranslationResult, null)
  }

  override def toChangelogStream(table: Table, targetSchema: Schema): DataStream[Row] = {
    Preconditions.checkNotNull(table, "Table must not be null.")
    Preconditions.checkNotNull(targetSchema, "Target schema must not be null.")

    val schemaTranslationResult = ExternalSchemaTranslator.fromInternal(
      table.getResolvedSchema,
      targetSchema)

    toStreamInternal(table, schemaTranslationResult, null)
  }

  override def toChangelogStream(
      table: Table,
      targetSchema: Schema,
      changelogMode: ChangelogMode)
    : DataStream[Row] = {
    Preconditions.checkNotNull(table, "Table must not be null.")
    Preconditions.checkNotNull(targetSchema, "Target schema must not be null.")
    Preconditions.checkNotNull(changelogMode, "Changelog mode must not be null.")

    val schemaTranslationResult = ExternalSchemaTranslator.fromInternal(
      table.getResolvedSchema,
      targetSchema)

    toStreamInternal(table, schemaTranslationResult, changelogMode)
  }

  private def toStreamInternal[T](
      table: Table,
      schemaTranslationResult: ExternalSchemaTranslator.OutputResult,
      @Nullable changelogMode: ChangelogMode)
    : DataStream[T] = {
    val catalogManager = getCatalogManager
    val schemaResolver = catalogManager.getSchemaResolver
    val operationTreeBuilder = getOperationTreeBuilder

    val optionalProjections = schemaTranslationResult.getProjections
    val projectOperation = if (optionalProjections.isPresent) {
      val projections = optionalProjections.get
      operationTreeBuilder.project(
        projections.asScala
          .map(ApiExpressionUtils.unresolvedRef)
          .map(_.asInstanceOf[Expression])
          .asJava,
        table.getQueryOperation)
    } else {
      table.getQueryOperation
    }

    val resolvedSchema = schemaResolver.resolve(schemaTranslationResult.getSchema)

    val unresolvedIdentifier =
      UnresolvedIdentifier.of("Unregistered_DataStream_Sink_" + ExternalModifyOperation.getUniqueId)
    val objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier)

    val modifyOperation = new ExternalModifyOperation(
      objectIdentifier,
      projectOperation,
      resolvedSchema,
      changelogMode,
      schemaTranslationResult.getPhysicalDataType
        .orElse(resolvedSchema.toPhysicalRowDataType))

    toStreamInternal(table, modifyOperation)
  }

  private def toStreamInternal[T](
      table: Table,
      modifyOperation: ModifyOperation)
    : DataStream[T] = {
    val transformations = planner
      .translate(Collections.singletonList(modifyOperation))
    val streamTransformation: Transformation[T] = getTransformation(
      table,
      transformations)
    scalaExecutionEnvironment.getWrappedStreamExecutionEnvironment.addOperator(streamTransformation)
    new DataStream[T](new JDataStream[T](
      scalaExecutionEnvironment
        .getWrappedStreamExecutionEnvironment, streamTransformation))
  }

  override def fromDataStream[T](dataStream: DataStream[T], fields: Expression*): Table = {
    val queryOperation = asQueryOperation(dataStream, Some(fields.toList.asJava))
    createTable(queryOperation)
  }

  override def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = {
    registerTable(name, fromDataStream(dataStream))
  }

  override def registerDataStream[T](
      name: String,
      dataStream: DataStream[T],
      fields: Expression*)
    : Unit = {
    registerTable(name, fromDataStream(dataStream, fields: _*))
  }

  override def toAppendStream[T: TypeInformation](table: Table): DataStream[T] = {
    val returnType = createTypeInformation[T]

    val modifyOperation = new OutputConversionModifyOperation(
      table.getQueryOperation,
      TypeConversions.fromLegacyInfoToDataType(returnType),
      OutputConversionModifyOperation.UpdateMode.APPEND)
    toStreamInternal[T](table, modifyOperation)
  }

  override def toRetractStream[T: TypeInformation](table: Table): DataStream[(Boolean, T)] = {
    val returnType = createTypeInformation[(Boolean, T)]

    val modifyOperation = new OutputConversionModifyOperation(
      table.getQueryOperation,
      TypeConversions.fromLegacyInfoToDataType(returnType),
      OutputConversionModifyOperation.UpdateMode.RETRACT)
    toStreamInternal(table, modifyOperation)
  }

  override def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    val typeInfo = UserDefinedFunctionHelper
      .getReturnTypeOfTableFunction(tf, implicitly[TypeInformation[T]])
    functionCatalog.registerTempSystemTableFunction(
      name,
      tf,
      typeInfo
    )
  }

  override def registerFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: AggregateFunction[T, ACC])
    : Unit = {
    val typeInfo = UserDefinedFunctionHelper
      .getReturnTypeOfAggregateFunction(f, implicitly[TypeInformation[T]])
    val accTypeInfo = UserDefinedFunctionHelper
      .getAccumulatorTypeOfAggregateFunction(f, implicitly[TypeInformation[ACC]])
    functionCatalog.registerTempSystemAggregateFunction(
      name,
      f,
      typeInfo,
      accTypeInfo
    )
  }

  override def registerFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: TableAggregateFunction[T, ACC])
    : Unit = {
    val typeInfo = UserDefinedFunctionHelper
      .getReturnTypeOfAggregateFunction(f, implicitly[TypeInformation[T]])
    val accTypeInfo = UserDefinedFunctionHelper
      .getAccumulatorTypeOfAggregateFunction(f, implicitly[TypeInformation[ACC]])
    functionCatalog.registerTempSystemAggregateFunction(
      name,
      f,
      typeInfo,
      accTypeInfo
    )
  }

  override def connect(connectorDescriptor: ConnectorDescriptor): StreamTableDescriptor = super
    .connect(connectorDescriptor).asInstanceOf[StreamTableDescriptor]


  override protected def validateTableSource(tableSource: TableSource[_]): Unit = {
    super.validateTableSource(tableSource)
    // check that event-time is enabled if table source includes rowtime attributes
    if (TableSourceValidation.hasRowtimeAttribute(tableSource) &&
      scalaExecutionEnvironment.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw new TableException(String.format(
        "A rowtime attribute requires an EventTime time characteristic in stream " +
          "environment. But is: %s}", scalaExecutionEnvironment.getStreamTimeCharacteristic))
    }
  }

  private def getTransformation[T](
      table: Table,
      transformations: util.List[Transformation[_]])
    : Transformation[T] = {
    if (transformations.size != 1) {
      throw new TableException(String
        .format(
          "Expected a single transformation for query: %s\n Got: %s",
          table.getQueryOperation.asSummaryString,
          transformations))
    }
    transformations.get(0).asInstanceOf[Transformation[T]]
  }

  private def asQueryOperation[T](
      dataStream: DataStream[T],
      fields: Option[util.List[Expression]]) = {
    val streamType = dataStream.javaStream.getType
    // get field names and types for all non-replaced fields
    val typeInfoSchema = fields.map((f: JList[Expression]) => {
      val fieldsInfo = FieldInfoUtils.getFieldsInfo(streamType, f.toArray(new Array[Expression](0)))
      // check if event-time is enabled
      if (fieldsInfo.isRowtimeDefined &&
        (scalaExecutionEnvironment.getStreamTimeCharacteristic ne TimeCharacteristic.EventTime)) {
        throw new ValidationException(String.format(
          "A rowtime attribute requires an EventTime time characteristic in stream " +
            "environment. But is: %s",
          scalaExecutionEnvironment.getStreamTimeCharacteristic))
      }
      fieldsInfo
    }).getOrElse(FieldInfoUtils.getFieldsInfo(streamType))
    new ScalaDataStreamQueryOperation[T](
      dataStream.javaStream,
      typeInfoSchema.getIndices,
      typeInfoSchema.toResolvedSchema)
  }

  override protected def qualifyQueryOperation(
    identifier: ObjectIdentifier,
    queryOperation: QueryOperation): QueryOperation = queryOperation match {
    case qo: ScalaDataStreamQueryOperation[Any] =>
      new ScalaDataStreamQueryOperation[Any](
        identifier,
        qo.getDataStream,
        qo.getFieldIndices,
        qo.getResolvedSchema
      )
    case _ =>
      queryOperation
  }

  override def createTemporaryView[T](
      path: String,
      dataStream: DataStream[T],
      fields: Expression*): Unit = {
    createTemporaryView(path, fromDataStream(dataStream, fields: _*))
  }
}

object StreamTableEnvironmentImpl {

  def create(
      executionEnvironment: StreamExecutionEnvironment,
      settings: EnvironmentSettings,
      tableConfig: TableConfig)
    : StreamTableEnvironmentImpl = {

    tableConfig.addConfiguration(settings.toConfiguration)

    if (!settings.isStreamingMode) {
      throw new TableException(
        "StreamTableEnvironment can not run in batch mode for now, please use TableEnvironment.")
    }

    // temporary solution until FLINK-15635 is fixed
    val classLoader = Thread.currentThread.getContextClassLoader

    val moduleManager = new ModuleManager

    val catalogManager = CatalogManager.newBuilder
      .classLoader(classLoader)
      .config(tableConfig.getConfiguration)
      .defaultCatalog(
        settings.getBuiltInCatalogName,
        new GenericInMemoryCatalog(
          settings.getBuiltInCatalogName,
          settings.getBuiltInDatabaseName))
      .executionConfig(executionEnvironment.getConfig)
      .build

    val functionCatalog = new FunctionCatalog(tableConfig, catalogManager, moduleManager)

    val executorProperties = settings.toExecutorProperties
    val executor = lookupExecutor(executorProperties, executionEnvironment)

    val plannerProperties = settings.toPlannerProperties
    val planner = ComponentFactoryService.find(classOf[PlannerFactory], plannerProperties)
      .create(
        plannerProperties,
        executor,
        tableConfig,
        functionCatalog,
        catalogManager)

    new StreamTableEnvironmentImpl(
      catalogManager,
      moduleManager,
      functionCatalog,
      tableConfig,
      executionEnvironment,
      planner,
      executor,
      settings.isStreamingMode,
      classLoader
    )
  }

  private def lookupExecutor(
      executorProperties: JMap[String, String],
      executionEnvironment: StreamExecutionEnvironment)
    :Executor =
    try {
      val executorFactory = ComponentFactoryService
        .find(classOf[ExecutorFactory], executorProperties)
      val createMethod = executorFactory.getClass
        .getMethod(
          "create",
          classOf[util.Map[String, String]],
          classOf[JStreamExecutionEnvironment])

      createMethod
        .invoke(
          executorFactory,
          executorProperties,
          executionEnvironment.getWrappedStreamExecutionEnvironment)
        .asInstanceOf[Executor]
    } catch {
      case e: Exception =>
        throw new TableException("Could not instantiate the executor. Make sure a planner module " +
          "is on the classpath", e)
    }
}
