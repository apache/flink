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

import org.apache.flink.annotation.{Internal, VisibleForTesting}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl
import org.apache.flink.table.api.bridge.scala.{StreamStatementSet, StreamTableEnvironment}
import org.apache.flink.table.catalog._
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.delegation.{Executor, Planner}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.factories.PlannerFactoryUtil
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction, TableFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations._
import org.apache.flink.table.sources.{TableSource, TableSourceValidation}
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

import java.util.Optional

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
    tableConfig: TableConfig,
    scalaExecutionEnvironment: StreamExecutionEnvironment,
    planner: Planner,
    executor: Executor,
    isStreaming: Boolean,
    userClassLoader: ClassLoader)
  extends AbstractStreamTableEnvironmentImpl(
    catalogManager,
    moduleManager,
    tableConfig,
    executor,
    functionCatalog,
    planner,
    isStreaming,
    userClassLoader,
    scalaExecutionEnvironment.getWrappedStreamExecutionEnvironment)
  with StreamTableEnvironment {

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

    val schemaTranslationResult = SchemaTranslator.createProducingResult(
      catalogManager.getDataTypeFactory,
      table.getResolvedSchema,
      targetDataType)

    new DataStream[T](toStreamInternal(table, schemaTranslationResult, ChangelogMode.insertOnly()))
  }

  override def toChangelogStream(table: Table): DataStream[Row] = {
    Preconditions.checkNotNull(table, "Table must not be null.")

    val schemaTranslationResult = SchemaTranslator.createProducingResult(
      table.getResolvedSchema,
      null)

    new DataStream[Row](toStreamInternal(table, schemaTranslationResult, null))
  }

  override def toChangelogStream(table: Table, targetSchema: Schema): DataStream[Row] = {
    Preconditions.checkNotNull(table, "Table must not be null.")
    Preconditions.checkNotNull(targetSchema, "Target schema must not be null.")

    val schemaTranslationResult = SchemaTranslator.createProducingResult(
      table.getResolvedSchema,
      targetSchema)

    new DataStream[Row](toStreamInternal(table, schemaTranslationResult, null))
  }

  override def toChangelogStream(
      table: Table,
      targetSchema: Schema,
      changelogMode: ChangelogMode)
    : DataStream[Row] = {
    Preconditions.checkNotNull(table, "Table must not be null.")
    Preconditions.checkNotNull(targetSchema, "Target schema must not be null.")
    Preconditions.checkNotNull(changelogMode, "Changelog mode must not be null.")

    val schemaTranslationResult = SchemaTranslator.createProducingResult(
      table.getResolvedSchema,
      targetSchema)

    new DataStream[Row](toStreamInternal(table, schemaTranslationResult, changelogMode))
  }

  override def createStatementSet(): StreamStatementSet = {
    new StreamStatementSetImpl(this)
  }

  override def fromDataStream[T](dataStream: DataStream[T], fields: Expression*): Table = {
    val queryOperation = asQueryOperation(dataStream.javaStream, Optional.of(fields.toList.asJava))
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
    new DataStream[T](toStreamInternal[T](table, modifyOperation))
  }

  override def toRetractStream[T: TypeInformation](table: Table): DataStream[(Boolean, T)] = {
    val returnType = createTypeInformation[(Boolean, T)]

    val modifyOperation = new OutputConversionModifyOperation(
      table.getQueryOperation,
      TypeConversions.fromLegacyInfoToDataType(returnType),
      OutputConversionModifyOperation.UpdateMode.RETRACT)
    new DataStream[(Boolean, T)](toStreamInternal(table, modifyOperation))
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
      settings: EnvironmentSettings)
    : StreamTableEnvironmentImpl = {

    // temporary solution until FLINK-15635 is fixed
    val classLoader = Thread.currentThread.getContextClassLoader

    val executor = AbstractStreamTableEnvironmentImpl.lookupExecutor(
      classLoader, executionEnvironment.getWrappedStreamExecutionEnvironment)

    val tableConfig = TableConfig.getDefault
    tableConfig.setRootConfiguration(executor.getConfiguration)
    tableConfig.addConfiguration(settings.getConfiguration)

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

    val planner = PlannerFactoryUtil.createPlanner(
      executor, tableConfig, moduleManager, catalogManager, functionCatalog)

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
}
