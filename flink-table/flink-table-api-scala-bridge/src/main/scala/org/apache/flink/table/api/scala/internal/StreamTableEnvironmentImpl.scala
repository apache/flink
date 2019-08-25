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
package org.apache.flink.table.api.scala.internal

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JStreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, GenericInMemoryCatalog}
import org.apache.flink.table.delegation.{Executor, ExecutorFactory, Planner, PlannerFactory}
import org.apache.flink.table.descriptors.{ConnectorDescriptor, StreamTableDescriptor}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.factories.ComponentFactoryService
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction, TableFunction, UserFunctionsTypeHelper}
import org.apache.flink.table.operations.{OutputConversionModifyOperation, ScalaDataStreamQueryOperation}
import org.apache.flink.table.sources.{TableSource, TableSourceValidation}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.typeutils.FieldInfoUtils

import java.util
import java.util.{Collections, List => JList, Map => JMap}

import _root_.scala.collection.JavaConverters._

/**
  * The implementation for a Scala [[StreamTableEnvironment]]. This enables conversions from/to
  * [[DataStream]]. It is bound to a given [[StreamExecutionEnvironment]].
  */
@Internal
class StreamTableEnvironmentImpl (
    catalogManager: CatalogManager,
    functionCatalog: FunctionCatalog,
    config: TableConfig,
    scalaExecutionEnvironment: StreamExecutionEnvironment,
    planner: Planner,
    executor: Executor,
    isStreaming: Boolean)
  extends TableEnvironmentImpl(
    catalogManager,
    config,
    executor,
    functionCatalog,
    planner,
    isStreaming)
  with org.apache.flink.table.api.scala.StreamTableEnvironment {

  if (!isStreaming) {
    throw new TableException(
      "StreamTableEnvironment is not supported on batch mode now, please use TableEnvironment.")
  }

  override def fromDataStream[T](dataStream: DataStream[T]): Table = {
    val queryOperation = asQueryOperation(dataStream, None)
    createTable(queryOperation)
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
    toDataStream[T](table, modifyOperation)
  }

  override def toAppendStream[T: TypeInformation](
      table: Table,
      queryConfig: StreamQueryConfig)
    : DataStream[T] = {
    tableConfig.setIdleStateRetentionTime(
      Time.milliseconds(queryConfig.getMinIdleStateRetentionTime),
      Time.milliseconds(queryConfig.getMaxIdleStateRetentionTime))
    toAppendStream(table)
  }

  override def toRetractStream[T: TypeInformation](table: Table): DataStream[(Boolean, T)] = {
    val returnType = createTypeInformation[(Boolean, T)]

    val modifyOperation = new OutputConversionModifyOperation(
      table.getQueryOperation,
      TypeConversions.fromLegacyInfoToDataType(returnType),
      OutputConversionModifyOperation.UpdateMode.RETRACT)
    toDataStream(table, modifyOperation)
  }

  override def toRetractStream[T: TypeInformation](
      table: Table,
      queryConfig: StreamQueryConfig)
    : DataStream[(Boolean, T)] = {
    tableConfig.setIdleStateRetentionTime(
        Time.milliseconds(queryConfig.getMinIdleStateRetentionTime),
        Time.milliseconds(queryConfig.getMaxIdleStateRetentionTime))
    toRetractStream(table)
  }

  override def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    val typeInfo = UserFunctionsTypeHelper
      .getReturnTypeOfTableFunction(tf, implicitly[TypeInformation[T]])
    functionCatalog.registerTableFunction(
      name,
      tf,
      typeInfo
    )
  }

  override def registerFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: AggregateFunction[T, ACC])
    : Unit = {
    val typeInfo = UserFunctionsTypeHelper
      .getReturnTypeOfAggregateFunction(f, implicitly[TypeInformation[T]])
    val accTypeInfo = UserFunctionsTypeHelper
      .getAccumulatorTypeOfAggregateFunction(f, implicitly[TypeInformation[ACC]])
    functionCatalog.registerAggregateFunction(
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
    val typeInfo = UserFunctionsTypeHelper
      .getReturnTypeOfAggregateFunction(f, implicitly[TypeInformation[T]])
    val accTypeInfo = UserFunctionsTypeHelper
      .getAccumulatorTypeOfAggregateFunction(f, implicitly[TypeInformation[ACC]])
    functionCatalog.registerAggregateFunction(
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

  override protected def isEagerOperationTranslation(): Boolean = true

  private def toDataStream[T](
      table: Table,
      modifyOperation: OutputConversionModifyOperation)
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
      typeInfoSchema.toTableSchema)
  }

  override def sqlUpdate(stmt: String, config: StreamQueryConfig): Unit = {
    tableConfig
      .setIdleStateRetentionTime(
        Time.milliseconds(config.getMinIdleStateRetentionTime),
        Time.milliseconds(config.getMaxIdleStateRetentionTime))
    sqlUpdate(stmt)
  }

  override def insertInto(
      table: Table,
      queryConfig: StreamQueryConfig,
      sinkPath: String,
      sinkPathContinued: String*): Unit = {
    tableConfig
      .setIdleStateRetentionTime(
        Time.milliseconds(queryConfig.getMinIdleStateRetentionTime),
        Time.milliseconds(queryConfig.getMaxIdleStateRetentionTime))
    insertInto(table, sinkPath, sinkPathContinued: _*)
  }
}

object StreamTableEnvironmentImpl {

  def create(
      executionEnvironment: StreamExecutionEnvironment,
      settings: EnvironmentSettings,
      tableConfig: TableConfig)
    : StreamTableEnvironmentImpl = {

    val catalogManager = new CatalogManager(
      settings.getBuiltInCatalogName,
      new GenericInMemoryCatalog(settings.getBuiltInCatalogName, settings.getBuiltInDatabaseName))

    val functionCatalog = new FunctionCatalog(catalogManager)

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
      functionCatalog,
      tableConfig,
      executionEnvironment,
      planner,
      executor,
      settings.isStreamingMode
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
