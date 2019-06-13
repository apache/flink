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

package org.apache.flink.table.api

import _root_.java.lang.{Boolean => JBool}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.calcite.{CalciteConfig, FlinkTypeFactory}
import org.apache.flink.table.catalog.CatalogManager
import org.apache.flink.table.descriptors.{ConnectorDescriptor, StreamTableDescriptor}
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions._
import org.apache.flink.table.operations.{DataStreamQueryOperation, QueryOperation}
import org.apache.flink.table.plan.StreamOptimizer
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.planner.DataStreamConversions
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{StreamTableSource, TableSource, TableSourceUtil}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.table.typeutils.FieldInfoUtils.getFieldsInfo

import _root_.scala.collection.JavaConverters._

/**
  * The abstract base class for the implementation of stream TableEnvironments.
  *
  * @param execEnv The [[StreamExecutionEnvironment]] which is wrapped in this
  *                [[StreamTableEnvImpl]].
  * @param config  The [[TableConfig]] of this [[StreamTableEnvImpl]].
  */
abstract class StreamTableEnvImpl(
    private[flink] val execEnv: StreamExecutionEnvironment,
    config: TableConfig,
    catalogManager: CatalogManager)
  extends TableEnvImpl(config, catalogManager) {

  @VisibleForTesting
  private[flink] val optimizer = new StreamOptimizer(
    () => config.getPlannerConfig.unwrap(classOf[CalciteConfig]).orElse(CalciteConfig.DEFAULT),
    planningConfigurationBuilder
  )

  override def queryConfig: StreamQueryConfig = new StreamQueryConfig

  /**
    * Registers an internal [[StreamTableSource]] in this [[TableEnvImpl]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param tableSource The [[TableSource]] to register.
    */
  override protected def validateTableSource(tableSource: TableSource[_]): Unit = {

    TableSourceUtil.validateTableSource(tableSource)
    tableSource match {

      // check for proper stream table source
      case streamTableSource: StreamTableSource[_] if !streamTableSource.isBounded =>
        // check that event-time is enabled if table source includes rowtime attributes
        if (TableSourceUtil.hasRowtimeAttribute(streamTableSource) &&
          execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
            throw new TableException(
              s"A rowtime attribute requires an EventTime time characteristic in stream " +
                s"environment. But is: ${execEnv.getStreamTimeCharacteristic}")
        }

      case streamTableSource: StreamTableSource[_] if streamTableSource.isBounded =>
        throw new TableException("Only unbounded StreamTableSource (isBounded returns false) " +
          "can be registered in StreamTableEnvironment")

      // not a stream table source
      case _ =>
        throw new TableException("Only StreamTableSource can be registered in " +
          "StreamTableEnvironment")
    }
  }

  override protected  def validateTableSink(configuredSink: TableSink[_]): Unit = {
    if (!configuredSink.isInstanceOf[StreamTableSink[_]]) {
      throw new TableException(
        "Only AppendStreamTableSink, UpsertStreamTableSink, and RetractStreamTableSink can be " +
          "registered in StreamTableEnvironment.")
    }
  }

  def connect(connectorDescriptor: ConnectorDescriptor): StreamTableDescriptor = {
    new StreamTableDescriptor(this, connectorDescriptor)
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataStream]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param inputTable The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @param queryConfig The configuration for the query to generate.
    * @tparam T The expected type of the [[DataStream]] which represents the [[Table]].
    */
  override private[flink] def writeToSink[T](
      inputTable: Table,
      sink: TableSink[T],
      queryConfig: QueryConfig): Unit = {

    val tableOperation = inputTable.getQueryOperation
    val relNode = getRelBuilder.tableOperation(tableOperation).build()
    // Check query configuration
    val streamQueryConfig = queryConfig match {
      case streamConfig: StreamQueryConfig => streamConfig
      case _ =>
        throw new TableException("StreamQueryConfig required to configure stream query.")
    }

    sink match {

      case retractSink: RetractStreamTableSink[T] =>
        writeToRetractSink(retractSink, tableOperation, streamQueryConfig)

      case upsertSink: UpsertStreamTableSink[T] =>
        writeToUpsertSink(upsertSink, tableOperation, relNode, streamQueryConfig)

      case appendSink: AppendStreamTableSink[T] =>
        writeToAppendSink(appendSink, tableOperation, relNode, streamQueryConfig)

      case _ =>
        throw new TableException("Stream Tables can only be emitted by AppendStreamTableSink, " +
          "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
  }

  private def writeToRetractSink[T](
      sink: RetractStreamTableSink[T],
      tableOperation: QueryOperation,
      streamQueryConfig: StreamQueryConfig) = {
    // retraction sink can always be used
    val outputType = fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[JTuple2[JBool, T]]]
    // translate the Table into a DataStream and provide the type that the TableSink expects.
    val result: DataStream[JTuple2[JBool, T]] =
      translate(
        tableOperation,
        streamQueryConfig,
        updatesAsRetraction = true,
        withChangeFlag = true)(outputType)
    // Give the DataStream to the TableSink to emit it.
    sink.consumeDataStream(result)
  }

  private def writeToAppendSink[T](
      sink: AppendStreamTableSink[T],
      tableOperation: QueryOperation,
      relNode: RelNode,
      streamQueryConfig: StreamQueryConfig)
    : DataStreamSink[_]= {
    // optimize plan
    val optimizedPlan = optimizer.optimize(relNode, updatesAsRetraction = false, getRelBuilder)
    // verify table is an insert-only (append-only) table
    if (!UpdatingPlanChecker.isAppendOnly(optimizedPlan)) {
      throw new TableException(
        "AppendStreamTableSink requires that Table has only insert changes.")
    }
    val outputType = fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[T]]
    val resultType = getTableSchema(tableOperation.getTableSchema.getFieldNames, optimizedPlan)
    // translate the Table into a DataStream and provide the type that the TableSink expects.
    val result: DataStream[T] =
      translateOptimized(
        optimizedPlan,
        resultType,
        outputType,
        streamQueryConfig,
        withChangeFlag = false)
    // Give the DataStream to the TableSink to emit it.
    sink.consumeDataStream(result)
  }

  private def writeToUpsertSink[T](
      sink: UpsertStreamTableSink[T],
      tableOperation: QueryOperation,
      relNode: RelNode,
      streamQueryConfig: StreamQueryConfig)
    : DataStreamSink[_] = {
    // optimize plan
    val optimizedPlan = optimizer.optimize(relNode, updatesAsRetraction = false, getRelBuilder)
    // check for append only table
    val isAppendOnlyTable = UpdatingPlanChecker.isAppendOnly(optimizedPlan)
    sink.setIsAppendOnly(isAppendOnlyTable)
    // extract unique key fields
    val tableKeys: Option[Array[String]] = UpdatingPlanChecker.getUniqueKeyFields(optimizedPlan)
    // check that we have keys if the table has changes (is not append-only)
    tableKeys match {
      case Some(keys) => sink.setKeyFields(keys)
      case None if isAppendOnlyTable => sink.setKeyFields(null)
      case None if !isAppendOnlyTable => throw new TableException(
        "UpsertStreamTableSink requires that Table has full primary keys if it is updated.")
    }
    val outputType = fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[JTuple2[JBool, T]]]
    val resultType = getTableSchema(tableOperation.getTableSchema.getFieldNames, optimizedPlan)
    // translate the Table into a DataStream and provide the type that the TableSink expects.
    val result: DataStream[JTuple2[JBool, T]] =
      translateOptimized(
        optimizedPlan,
        resultType,
        outputType,
        streamQueryConfig,
        withChangeFlag = true)
    // Give the DataStream to the TableSink to emit it.
    sink.consumeDataStream(result)
  }

  private def translateOptimized[A](
      optimizedPlan: RelNode,
      logicalSchema: TableSchema,
      tpe: TypeInformation[A],
      queryConfig: StreamQueryConfig,
      withChangeFlag: Boolean)
    : DataStream[A] = {
    val dataStream = translateToCRow(optimizedPlan, queryConfig)
    DataStreamConversions.convert(dataStream, logicalSchema, withChangeFlag, tpe, config)
  }

  protected def asQueryOperation[T](
      dataStream: DataStream[T],
      fields: Option[Array[Expression]])
    : DataStreamQueryOperation[T] = {
    val streamType = dataStream.getType

    // get field names and types for all non-replaced fields
    val fieldsInfo = fields match {
      case Some(f) =>
        // validate and extract time attributes
        val fieldsInfo = getFieldsInfo[T](streamType, f)

        // check if event-time is enabled
        if (fieldsInfo.isRowtimeDefined &&
          execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
          throw new ValidationException(
            s"A rowtime attribute requires an EventTime time characteristic in stream environment" +
              s". But is: ${execEnv.getStreamTimeCharacteristic}")
        }

        fieldsInfo
      case None =>
        getFieldsInfo[T](streamType)
    }

    val dataStreamTable = new DataStreamQueryOperation(
      dataStream,
      fieldsInfo.getIndices,
      fieldsInfo.toTableSchema)
    dataStreamTable
  }

  /**
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * This method is used only by the bridging methods toAppendStream/toRetractStream.
    *
    * @param tableOperation The root node of the relational expression tree.
    * @param queryConfig The configuration for the query to generate.
    * @param updatesAsRetraction Set to true to encode updates as retraction messages.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param tpe The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      tableOperation: QueryOperation,
      queryConfig: StreamQueryConfig,
      updatesAsRetraction: Boolean,
      withChangeFlag: Boolean)(implicit tpe: TypeInformation[A]): DataStream[A] = {
    val relNode = getRelBuilder.tableOperation(tableOperation).build()
    val dataStreamPlan = optimizer.optimize(relNode, updatesAsRetraction, getRelBuilder)

    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !UpdatingPlanChecker.isAppendOnly(dataStreamPlan)) {
      throw new ValidationException(
        "Table is not an append-only table. " +
          "Use the toRetractStream() in order to handle add and retract messages.")
    }

    val logicalSchema = getTableSchema(tableOperation.getTableSchema.getFieldNames, dataStreamPlan)
    val dataStream = translateToCRow(dataStreamPlan, queryConfig)

    DataStreamConversions.convert(dataStream, logicalSchema, withChangeFlag, tpe, config)
  }

  /**
    * Translates a logical [[RelNode]] plan into a [[DataStream]] of type [[CRow]].
    *
    * @param logicalPlan The logical plan to translate.
    * @param queryConfig  The configuration for the query to generate.
    * @return The [[DataStream]] of type [[CRow]].
    */
  private def translateToCRow(
    logicalPlan: RelNode,
    queryConfig: StreamQueryConfig): DataStream[CRow] = {

    logicalPlan match {
      case node: DataStreamRel =>
        node.translateToPlan(this, queryConfig)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
    * Returns the record type of the optimized plan with field names of the logical plan.
    */
  private def getTableSchema(originalNames: Array[String], optimizedPlan: RelNode): TableSchema = {
    val fieldTypes = optimizedPlan.getRowType.getFieldList.asScala.map(_.getType)
      .map(FlinkTypeFactory.toTypeInfo)
      .map(TypeConversions.fromLegacyInfoToDataType)
      .toArray

    TableSchema.builder().fields(originalNames, fieldTypes).build()
  }

  def explain(table: Table): String = {
    val ast = getRelBuilder.tableOperation(table.getQueryOperation).build()
    val optimizedPlan = optimizer.optimize(ast, updatesAsRetraction = false, getRelBuilder)
    val dataStream = translateToCRow(optimizedPlan, queryConfig)

    val env = dataStream.getExecutionEnvironment
    val jsonSqlPlan = env.getExecutionPlan

    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jsonSqlPlan, false)

    s"== Abstract Syntax Tree ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(ast)}" +
        System.lineSeparator +
        s"== Optimized Logical Plan ==" +
        System.lineSeparator +
        s"${RelOptUtil.toString(optimizedPlan)}" +
        System.lineSeparator +
        s"== Physical Execution Plan ==" +
        System.lineSeparator +
        s"$sqlPlan"
  }

}
