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

package org.apache.flink.table.planner
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.sql.parser.dml.RichSqlInsert
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.{CalciteConfig, FlinkPlannerImpl, FlinkRelBuilder, FlinkTypeFactory, PreValidateReWriter}
import org.apache.flink.table.catalog.{CatalogManager, CatalogManagerCalciteSchema, CatalogTable, ConnectorCatalogTable, _}
import org.apache.flink.table.delegation.{Executor, Planner}
import org.apache.flink.table.executor.StreamExecutor
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.{ExpressionBridge, PlannerExpression, PlannerExpressionConverter, PlannerTypeInferenceUtilImpl}
import org.apache.flink.table.factories.{TableFactoryService, TableFactoryUtil, TableSinkFactory}
import org.apache.flink.table.operations.OutputConversionModifyOperation.UpdateMode
import org.apache.flink.table.operations._
import org.apache.flink.table.plan.StreamOptimizer
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sinks._
import org.apache.flink.table.sqlexec.SqlToOperationConverter
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.util.JavaScalaConversionUtil

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlKind

import _root_.java.lang.{Boolean => JBool}
import _root_.java.util
import _root_.java.util.{Objects, List => JList}

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.JavaConversions._

/**
  * Implementation of [[Planner]] for legacy Flink planner. It supports only streaming use cases.
  * (The new [[org.apache.flink.table.sources.InputFormatTableSource]] should work, but will be
  * handled as streaming sources, and no batch specific optimizations will be applied).
  *
  * @param executor        instance of [[StreamExecutor]], needed to extract
  *                        [[StreamExecutionEnvironment]] for
  *                        [[org.apache.flink.table.sources.StreamTableSource.getDataStream]]
  * @param config          mutable configuration passed from corresponding [[TableEnvironment]]
  * @param functionCatalog catalog of functions
  * @param catalogManager  manager of catalog meta objects such as tables, views, databases etc.
  */
class StreamPlanner(
    executor: Executor,
    config: TableConfig,
    functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager)
  extends Planner {

  // temporary utility until we don't use planner expressions anymore
  functionCatalog.setPlannerTypeInferenceUtil(PlannerTypeInferenceUtilImpl.INSTANCE)

  private val internalSchema: CalciteSchema =
    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, true))

  // temporary bridge between API and planner
  private val expressionBridge: ExpressionBridge[PlannerExpression] =
    new ExpressionBridge[PlannerExpression](functionCatalog, PlannerExpressionConverter.INSTANCE)

  private val planningConfigurationBuilder: PlanningConfigurationBuilder =
    new PlanningConfigurationBuilder(
      config,
      functionCatalog,
      internalSchema,
      expressionBridge)

  @VisibleForTesting
  private[flink] val optimizer: StreamOptimizer = new StreamOptimizer(
    () => config.getPlannerConfig
      .unwrap(classOf[CalciteConfig])
      .orElse(CalciteConfig.DEFAULT),
    planningConfigurationBuilder)

  override def parse(stmt: String): JList[Operation] = {
    val planner = getFlinkPlanner
    // parse the sql query
    val parsed = planner.parse(stmt)

    parsed match {
      case insert: RichSqlInsert =>
        val targetColumnList = insert.getTargetColumnList
        if (targetColumnList != null && insert.getTargetColumnList.size() != 0) {
          throw new ValidationException("Partial inserts are not supported")
        }
        List(SqlToOperationConverter.convert(planner, insert))
      case node if node.getKind.belongsTo(SqlKind.QUERY) || node.getKind.belongsTo(SqlKind.DDL) =>
        List(SqlToOperationConverter.convert(planner, parsed)).asJava
      case _ =>
        throw new TableException(
          "Unsupported SQL query! parse() only accepts SQL queries of type " +
            "SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY or INSERT;" +
            "and SQL DDLs of type " +
            "CREATE TABLE")
    }
  }

  override def translate(tableOperations: util.List[ModifyOperation])
    : util.List[Transformation[_]] = {
    tableOperations.asScala.map(translate).filter(Objects.nonNull).asJava
  }

  override def explain(operations: util.List[Operation], extended: Boolean): String = {
    operations.asScala.map {
      case queryOperation: QueryOperation =>
        explain(queryOperation, unwrapQueryConfig)
      case operation =>
        throw new TableException(s"${operation.getClass.getCanonicalName} is not supported")
    }.mkString(s"${System.lineSeparator}${System.lineSeparator}")
  }

  override def getCompletionHints(
      statement: String,
      position: Int)
    : Array[String] = {
    val planner = getFlinkPlanner
    planner.getCompletionHints(statement, position)
  }

  private def translate(tableOperation: ModifyOperation)
    : Transformation[_] = {
    tableOperation match {
      case s : UnregisteredSinkModifyOperation[_] =>
        writeToSink(s.getChild, s.getSink, unwrapQueryConfig)

      case catalogSink: CatalogSinkModifyOperation =>
        getTableSink(catalogSink.getTablePath)
          .map(sink => {
            TableSinkUtils.validateSink(
              catalogSink.getStaticPartitions,
              catalogSink.getChild,
              catalogSink.getTablePath,
              sink)
            // set static partitions if it is a partitioned sink
            sink match {
              case partitionableSink: PartitionableTableSink
                if partitionableSink.getPartitionFieldNames != null
                  && partitionableSink.getPartitionFieldNames.nonEmpty =>
                partitionableSink.setStaticPartition(catalogSink.getStaticPartitions)
              case _ =>
            }
            writeToSink(catalogSink.getChild, sink, unwrapQueryConfig)
          }) match {
          case Some(t) => t
          case None => throw new TableException(s"Sink ${catalogSink.getTablePath} does not exists")
        }

      case outputConversion: OutputConversionModifyOperation =>
        val (isRetract, withChangeFlag) = outputConversion.getUpdateMode match {
          case UpdateMode.RETRACT => (true, true)
          case UpdateMode.APPEND => (false, false)
          case UpdateMode.UPSERT => (false, true)
        }

        translateToType(
          tableOperation.getChild,
          unwrapQueryConfig,
          isRetract,
          withChangeFlag,
          TypeConversions.fromDataTypeToLegacyInfo(outputConversion.getType)).getTransformation

      case _ =>
        throw new TableException(s"Unsupported ModifyOperation: $tableOperation")
    }
  }

  private def unwrapQueryConfig = {
    new StreamQueryConfig(
      config.getMinIdleStateRetentionTime,
      config.getMaxIdleStateRetentionTime
    )
  }

  private def explain(tableOperation: QueryOperation, queryConfig: StreamQueryConfig) = {
    val ast = getRelBuilder.tableOperation(tableOperation).build()
    val optimizedPlan = optimizer
      .optimize(ast, updatesAsRetraction = false, getRelBuilder)
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

  private def getFlinkPlanner: FlinkPlannerImpl = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase

    planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase)
  }

  private[flink] def getRelBuilder: FlinkRelBuilder = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase

    planningConfigurationBuilder.createRelBuilder(currentCatalogName, currentDatabase)
  }

  private[flink] def getConfig: TableConfig = config

  private[flink] def getExecutionEnvironment: StreamExecutionEnvironment =
    executor.asInstanceOf[StreamExecutor].getExecutionEnvironment

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

  private def writeToSink[T](
      tableOperation: QueryOperation,
      sink: TableSink[T],
      queryConfig: StreamQueryConfig)
    : Transformation[_] = {

    val resultSink = sink match {
      case retractSink: RetractStreamTableSink[T] =>
        retractSink match {
          case partitionableSink: PartitionableTableSink
            if partitionableSink.getPartitionFieldNames.nonEmpty =>
            throw new TableException("Partitionable sink in retract stream mode " +
              "is not supported yet!")
          case _ =>
        }
        writeToRetractSink(retractSink, tableOperation, queryConfig)

      case upsertSink: UpsertStreamTableSink[T] =>
        upsertSink match {
          case partitionableSink: PartitionableTableSink
            if partitionableSink.getPartitionFieldNames.nonEmpty =>
            throw new TableException("Partitionable sink in upsert stream mode " +
              "is not supported yet!")
          case _ =>
        }
        writeToUpsertSink(upsertSink, tableOperation, queryConfig)

      case appendSink: AppendStreamTableSink[T] =>
        writeToAppendSink(appendSink, tableOperation, queryConfig)

      case _ =>
        throw new ValidationException("Stream Tables can only be emitted by AppendStreamTableSink, "
          + "RetractStreamTableSink, or UpsertStreamTableSink.")
    }

    if (resultSink != null) {
      resultSink.getTransformation
    } else {
      null
    }
  }

  private def writeToRetractSink[T](
      sink: RetractStreamTableSink[T],
      tableOperation: QueryOperation,
      streamQueryConfig: StreamQueryConfig)
    : DataStreamSink[_]= {
    // retraction sink can always be used
    val outputType = TypeConversions.fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[JTuple2[JBool, T]]]
    // translate the Table into a DataStream and provide the type that the TableSink expects.
    val result: DataStream[JTuple2[JBool, T]] =
      translateToType(
        tableOperation,
        streamQueryConfig,
        updatesAsRetraction = true,
        withChangeFlag = true,
        outputType)
    // Give the DataStream to the TableSink to emit it.
    sink.consumeDataStream(result)
  }

  private def writeToAppendSink[T](
      sink: AppendStreamTableSink[T],
      tableOperation: QueryOperation,
      streamQueryConfig: StreamQueryConfig)
    : DataStreamSink[_]= {
    // optimize plan
    val relNode = getRelBuilder.tableOperation(tableOperation).build()
    val optimizedPlan = optimizer.optimize(relNode, updatesAsRetraction = false, getRelBuilder)
    // verify table is an insert-only (append-only) table
    if (!UpdatingPlanChecker.isAppendOnly(optimizedPlan)) {
      throw new TableException(
        "AppendStreamTableSink requires that Table has only insert changes.")
    }
    val outputType = TypeConversions.fromDataTypeToLegacyInfo(sink.getConsumedDataType)
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
    sink.consumeDataStream(shuffleByPartitionFieldsIfNeeded(sink, result))
  }

  private def writeToUpsertSink[T](
      sink: UpsertStreamTableSink[T],
      tableOperation: QueryOperation,
      streamQueryConfig: StreamQueryConfig)
    : DataStreamSink[_] = {
    // optimize plan
    val relNode = getRelBuilder.tableOperation(tableOperation).build()
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
    val outputType = TypeConversions.fromDataTypeToLegacyInfo(sink.getConsumedDataType)
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

  /**
    * Key by the partition fields if the sink is a [[PartitionableTableSink]].
    * @param sink       the table sink
    * @param dataStream the data stream
    * @tparam R         the data stream record type
    * @return a data stream that maybe keyed by.
    */
  private def shuffleByPartitionFieldsIfNeeded[R](
      sink: TableSink[_],
      dataStream: DataStream[R]): DataStream[R] = {
    sink match {
      case partitionableSink: PartitionableTableSink
        if partitionableSink.getPartitionFieldNames.nonEmpty =>
        val fieldNames = sink.getTableSchema.getFieldNames
        val indices = partitionableSink.getPartitionFieldNames.map(fieldNames.indexOf(_))
        dataStream.keyBy(indices:_*)
      case _ => dataStream
    }
  }

  private def translateToType[A](
      table: QueryOperation,
      queryConfig: StreamQueryConfig,
      updatesAsRetraction: Boolean,
      withChangeFlag: Boolean,
      tpe: TypeInformation[A])
    : DataStream[A] = {
    val relNode = getRelBuilder.tableOperation(table).build()
    val dataStreamPlan = optimizer.optimize(relNode, updatesAsRetraction, getRelBuilder)
    val rowType = getTableSchema(table.getTableSchema.getFieldNames, dataStreamPlan)

    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !UpdatingPlanChecker.isAppendOnly(dataStreamPlan)) {
      throw new ValidationException(
        "Table is not an append-only table. " +
          "Use the toRetractStream() in order to handle add and retract messages.")
    }

    // get CRow plan
    translateOptimized(dataStreamPlan, rowType, tpe, queryConfig, withChangeFlag)
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

  private def getTableSink(tablePath: JList[String]): Option[TableSink[_]] = {
    JavaScalaConversionUtil.toScala(catalogManager.resolveTable(tablePath.asScala: _*)) match {
      case Some(s) if s.getExternalCatalogTable.isPresent =>

        Option(TableFactoryUtil.findAndCreateTableSink(s.getExternalCatalogTable.get()))

      case Some(s) if JavaScalaConversionUtil.toScala(s.getCatalogTable)
        .exists(_.isInstanceOf[ConnectorCatalogTable[_, _]]) =>

        JavaScalaConversionUtil
          .toScala(s.getCatalogTable.get().asInstanceOf[ConnectorCatalogTable[_, _]].getTableSink)

      case Some(s) if JavaScalaConversionUtil.toScala(s.getCatalogTable)
        .exists(_.isInstanceOf[CatalogTable]) =>

        val catalog = catalogManager.getCatalog(s.getTablePath.get(0))
        val catalogTable = s.getCatalogTable.get().asInstanceOf[CatalogTable]
        if (catalog.isPresent && catalog.get().getTableFactory.isPresent) {
          val dbName = s.getTablePath.get(1)
          val tableName = s.getTablePath.get(2)
          val sink = TableFactoryUtil.createTableSinkForCatalogTable(
            catalog.get(), catalogTable, new ObjectPath(dbName, tableName))
          if (sink.isPresent) {
            return Option(sink.get())
          }
        }
        val sinkProperties = catalogTable.toProperties
        Option(TableFactoryService.find(classOf[TableSinkFactory[_]], sinkProperties)
          .createTableSink(sinkProperties))

      case _ => None
    }
  }
}
