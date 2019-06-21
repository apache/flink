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

import java.util
import java.util.{Collections, List => JList}

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JStreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.{PlannerFactory, TableEnvironmentImpl}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, GenericInMemoryCatalog}
import org.apache.flink.table.delegation.{Executor, Planner}
import org.apache.flink.table.descriptors.{ConnectorDescriptor, StreamTableDescriptor}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction, TableFunction, UserFunctionsTypeHelper}
import org.apache.flink.table.operations.{ScalaDataStreamQueryOperation, OutputConversionModifyOperation}
import org.apache.flink.table.sources.{TableSource, TableSourceValidation}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.typeutils.FieldInfoUtils

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
    executor: Executor)
  extends TableEnvironmentImpl(
    catalogManager,
    config,
    executor,
    functionCatalog,
    planner)
  with org.apache.flink.table.api.scala.StreamTableEnvironment {

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
    toAppendStream(table, new StreamQueryConfig)
  }

  override def toAppendStream[T: TypeInformation](
      table: Table,
      queryConfig: StreamQueryConfig)
    : DataStream[T] = {
    val returnType = createTypeInformation[T]

    val modifyOperation = new OutputConversionModifyOperation(
      table.getQueryOperation,
      TypeConversions.fromLegacyInfoToDataType(returnType),
      OutputConversionModifyOperation.UpdateMode.APPEND)
    queryConfigProvider.setConfig(queryConfig)
    toDataStream(table, modifyOperation)
  }

  override def toRetractStream[T: TypeInformation](table: Table): DataStream[(Boolean, T)] = {
    toRetractStream(table, new StreamQueryConfig)
  }

  override def toRetractStream[T: TypeInformation](
      table: Table,
      queryConfig: StreamQueryConfig)
    : DataStream[(Boolean, T)] = {
    val returnType = createTypeInformation[(Boolean, T)]

    val modifyOperation = new OutputConversionModifyOperation(
      table.getQueryOperation,
      TypeConversions.fromLegacyInfoToDataType(returnType),
      OutputConversionModifyOperation.UpdateMode.RETRACT)

    queryConfigProvider.setConfig(queryConfig)
    toDataStream(table, modifyOperation)
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

  private def toDataStream[T](
      table: Table,
      modifyOperation: OutputConversionModifyOperation)
    : DataStream[T] = {
    val transformations = planner
      .translate(Collections.singletonList(modifyOperation))
    val streamTransformation: StreamTransformation[T] = getStreamTransformation(
      table,
      transformations)
    scalaExecutionEnvironment.getWrappedStreamExecutionEnvironment.addOperator(streamTransformation)
    new DataStream[T](new JDataStream[T](
      scalaExecutionEnvironment
        .getWrappedStreamExecutionEnvironment, streamTransformation))
  }

  private def getStreamTransformation[T](
      table: Table,
      transformations: util.List[StreamTransformation[_]])
    : StreamTransformation[T] = {
    if (transformations.size != 1) {
      throw new TableException(String
        .format(
          "Expected a single transformation for query: %s\n Got: %s",
          table.getQueryOperation.asSummaryString,
          transformations))
    }
    transformations.get(0).asInstanceOf[StreamTransformation[T]]
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
}

object StreamTableEnvironmentImpl {

  /**
    * Creates an instance of a [[StreamTableEnvironment]]. It uses the
    * [[StreamExecutionEnvironment]] for executing queries. This is also the
    * [[StreamExecutionEnvironment]] that will be used when converting
    * from/to [[DataStream]].
    *
    * @param tableConfig The configuration of the TableEnvironment.
    * @param executionEnvironment The [[StreamExecutionEnvironment]] of the TableEnvironment.
    */
  def create(
      tableConfig: TableConfig,
      executionEnvironment: StreamExecutionEnvironment)
    : StreamTableEnvironmentImpl = {
    val catalogManager = new CatalogManager(
      tableConfig.getBuiltInCatalogName,
      new GenericInMemoryCatalog(
        tableConfig.getBuiltInCatalogName,
        tableConfig.getBuiltInDatabaseName)
    )
    create(catalogManager, tableConfig, executionEnvironment)
  }

  /**
    * Creates an instance of a [[StreamTableEnvironment]]. It uses the
    * [[StreamExecutionEnvironment]] for executing queries. This is also the
    * [[StreamExecutionEnvironment]] that will be used when converting
    * from/to [[DataStream]].
    *
    * @param catalogManager The [[CatalogManager]] to use for storing and looking up [[Table]]s.
    * @param tableConfig The configuration of the TableEnvironment.
    * @param executionEnvironment The [[StreamExecutionEnvironment]] of the TableEnvironment.
    */
  def create(
      catalogManager: CatalogManager,
      tableConfig: TableConfig,
      executionEnvironment: StreamExecutionEnvironment)
    : StreamTableEnvironmentImpl = {
    val executor = lookupExecutor(executionEnvironment)
    val functionCatalog = new FunctionCatalog(
      catalogManager.getCurrentCatalog,
      catalogManager.getCurrentDatabase)
    new StreamTableEnvironmentImpl(
      catalogManager,
      functionCatalog,
      tableConfig,
      executionEnvironment,
      PlannerFactory.lookupPlanner(executor, tableConfig, functionCatalog, catalogManager),
      executor)
  }

  private def lookupExecutor(executionEnvironment: StreamExecutionEnvironment) =
    try {
      val clazz = Class.forName("org.apache.flink.table.executor.ExecutorFactory")
      val createMethod = clazz.getMethod("create", classOf[JStreamExecutionEnvironment])
      createMethod.invoke(null, executionEnvironment.getWrappedStreamExecutionEnvironment)
        .asInstanceOf[Executor]
    } catch {
      case e: Exception =>
        throw new TableException("Could not instantiate the executor. Make sure a planner module " +
          "is on the classpath", e)
    }
}
