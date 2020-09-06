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
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.calcite._
import org.apache.flink.table.catalog.{CatalogManager, CatalogManagerCalciteSchema, CatalogTable, ConnectorCatalogTable, _}
import org.apache.flink.table.delegation.{Executor, Parser, Planner}
import org.apache.flink.table.executor.StreamExecutor
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.{ExpressionBridge, PlannerExpression, PlannerExpressionConverter, PlannerTypeInferenceUtilImpl}
import org.apache.flink.table.factories.{TableFactoryUtil, TableSinkFactoryContextImpl}
import org.apache.flink.table.operations.OutputConversionModifyOperation.UpdateMode
import org.apache.flink.table.operations._
import org.apache.flink.table.plan.StreamOptimizer
import org.apache.flink.table.plan.nodes.LogicalSink
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sinks._
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.util.{DummyStreamExecutionEnvironment, JavaScalaConversionUtil}

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalTableModify

import _root_.java.util
import _root_.java.util.Objects
import _root_.java.util.function.{Supplier => JSupplier}

import _root_.scala.collection.JavaConverters._

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
    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, config, true))

  // temporary bridge between API and planner
  private val expressionBridge: ExpressionBridge[PlannerExpression] =
    new ExpressionBridge[PlannerExpression](PlannerExpressionConverter.INSTANCE)

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

  private val parser: Parser = new ParserImpl(
    catalogManager,
    // we do not cache the parser in order to use the most up to
    // date configuration. Users might change parser configuration in TableConfig in between
    // parsing statements
    new JSupplier[FlinkPlannerImpl] {
      override def get(): FlinkPlannerImpl = getFlinkPlanner
    },
    new JSupplier[CalciteParser] {
      override def get(): CalciteParser = planningConfigurationBuilder.createCalciteParser()
    }
  )

  override def getParser: Parser = parser

  override def translate(
      tableOperations: util.List[ModifyOperation]): util.List[Transformation[_]] = {
    val planner = createDummyPlanner()
    tableOperations.asScala.map { operation =>
      val (ast, updatesAsRetraction) = translateToRel(operation)
      val optimizedPlan = optimizer.optimize(ast, updatesAsRetraction, getRelBuilder)
      val dataStream = translateToCRow(planner, optimizedPlan)
      dataStream.getTransformation.asInstanceOf[Transformation[_]]
    }.filter(Objects.nonNull).asJava
  }

  override def explain(operations: util.List[Operation], extraDetails: ExplainDetail*): String = {
    require(operations.asScala.nonEmpty, "operations should not be empty")
    val astWithUpdatesAsRetractionTuples = operations.asScala.map {
      case queryOperation: QueryOperation =>
        val relNode = getRelBuilder.tableOperation(queryOperation).build()
        relNode match {
          // SQL: explain plan for insert into xx
          case modify: LogicalTableModify =>
            // convert LogicalTableModify to CatalogSinkModifyOperation
            val qualifiedName = modify.getTable.getQualifiedName
            require(qualifiedName.size() == 3, "the length of qualified name should be 3.")
            val modifyOperation = new CatalogSinkModifyOperation(
              ObjectIdentifier.of(qualifiedName.get(0), qualifiedName.get(1), qualifiedName.get(2)),
              new PlannerQueryOperation(modify.getInput)
            )
            translateToRel(modifyOperation)
          case _ =>
            (relNode, false)
        }
      case modifyOperation: ModifyOperation =>
        translateToRel(modifyOperation)
      case o => throw new TableException(s"Unsupported operation: ${o.getClass.getCanonicalName}")
    }

    val optimizedNodes = astWithUpdatesAsRetractionTuples.map {
      case (ast, updatesAsRetraction) =>
        optimizer.optimize(ast, updatesAsRetraction, getRelBuilder)
    }

    val planner = createDummyPlanner()
    val dataStreams = optimizedNodes.map(p => translateToCRow(planner, p))

    val astPlan = astWithUpdatesAsRetractionTuples.map {
      p => RelOptUtil.toString(p._1)
    }.mkString(System.lineSeparator)
    val optimizedPlan = optimizedNodes.map(RelOptUtil.toString).mkString(System.lineSeparator)

    val env = dataStreams.head.getExecutionEnvironment
    val jsonSqlPlan = env.getExecutionPlan
    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jsonSqlPlan, false)

    s"== Abstract Syntax Tree ==" +
      System.lineSeparator +
      s"$astPlan" +
      System.lineSeparator +
      s"== Optimized Logical Plan ==" +
      System.lineSeparator +
      s"$optimizedPlan" +
      System.lineSeparator +
      s"== Physical Execution Plan ==" +
      System.lineSeparator +
      s"$sqlPlan"
  }

  override def getCompletionHints(
      statement: String,
      position: Int)
    : Array[String] = {
    val planner = getFlinkPlanner
    planner.getCompletionHints(statement, position)
  }

  private def translateToRel(modifyOperation: ModifyOperation): (RelNode, Boolean) = {
    modifyOperation match {
      case s: UnregisteredSinkModifyOperation[_] =>
        writeToSink(s.getChild, s.getSink, "UnregisteredSink")

      case s: SelectSinkOperation =>
        val sink = new StreamSelectTableSink(s.getChild.getTableSchema)
        s.setSelectResultProvider(sink.getSelectResultProvider)
        writeToSink(s.getChild, sink, "collect")

      case catalogSink: CatalogSinkModifyOperation =>
        getTableSink(catalogSink.getTableIdentifier)
          .map(sink => {
            TableSinkUtils.validateSink(
              catalogSink.getStaticPartitions,
              catalogSink.getChild,
              catalogSink.getTableIdentifier,
              sink)
            // set static partitions if it is a partitioned sink
            sink match {
              case partitionableSink: PartitionableTableSink =>
                partitionableSink.setStaticPartition(catalogSink.getStaticPartitions)
              case _ =>
            }
            // set whether to overwrite if it's an OverwritableTableSink
            sink match {
              case overwritableTableSink: OverwritableTableSink =>
                overwritableTableSink.setOverwrite(catalogSink.isOverwrite)
              case _ =>
                assert(!catalogSink.isOverwrite, "INSERT OVERWRITE requires " +
                  s"${classOf[OverwritableTableSink].getSimpleName} but actually got " +
                  sink.getClass.getName)
            }
            writeToSink(
              catalogSink.getChild,
              sink,
              catalogSink.getTableIdentifier.asSummaryString())
          }) match {
          case Some(t) => t
          case None =>
            throw new TableException(s"Sink ${catalogSink.getTableIdentifier} does not exists")
        }

      case outputConversion: OutputConversionModifyOperation =>
        val (isRetract, withChangeFlag) = outputConversion.getUpdateMode match {
          case UpdateMode.RETRACT => (true, true)
          case UpdateMode.APPEND => (false, false)
          case UpdateMode.UPSERT => (false, true)
        }

        val tableSink = new DataStreamTableSink(
          outputConversion.getChild.getTableSchema,
          TypeConversions.fromDataTypeToLegacyInfo(outputConversion.getType),
          withChangeFlag)
        val input = getRelBuilder.tableOperation(modifyOperation.getChild).build()
        val sink = LogicalSink.create(input, tableSink, "DataStreamTableSink")
        (sink, isRetract)

      case _ =>
        throw new TableException(s"Unsupported ModifyOperation: $modifyOperation")
    }
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

  private def translateToCRow(planner: StreamPlanner, logicalPlan: RelNode): DataStream[CRow] = {
    logicalPlan match {
      case node: DataStreamRel =>
        getExecutionEnvironment.configure(
          config.getConfiguration,
          Thread.currentThread().getContextClassLoader)
        node.translateToPlan(planner)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  private def writeToSink[T](
      tableOperation: QueryOperation,
      sink: TableSink[T],
      sinkName: String): (RelNode, Boolean) = {

    val updatesAsRetraction = sink match {
      case retractSink: RetractStreamTableSink[T] =>
        retractSink match {
          case _: PartitionableTableSink =>
            throw new TableException("Partitionable sink in retract stream mode " +
              "is not supported yet!")
          case _ => // do nothing
        }
        true

      case upsertSink: UpsertStreamTableSink[T] =>
        upsertSink match {
          case _: PartitionableTableSink =>
            throw new TableException("Partitionable sink in upsert stream mode " +
              "is not supported yet!")
          case _ => // do nothing
        }
        false

      case _: AppendStreamTableSink[T] =>
        false

      case _ =>
        throw new ValidationException("Stream Tables can only be emitted by AppendStreamTableSink, "
          + "RetractStreamTableSink, or UpsertStreamTableSink.")
    }

    val input = getRelBuilder.tableOperation(tableOperation).build()
    (LogicalSink.create(input, sink, sinkName), updatesAsRetraction)
  }

  private def getTableSink(objectIdentifier: ObjectIdentifier): Option[TableSink[_]] = {
    JavaScalaConversionUtil.toScala(catalogManager.getTable(objectIdentifier))
      .map(_.getTable) match {
      case Some(s) if s.isInstanceOf[ConnectorCatalogTable[_, _]] =>
        JavaScalaConversionUtil.toScala(s.asInstanceOf[ConnectorCatalogTable[_, _]].getTableSink)

      case Some(s) if s.isInstanceOf[CatalogTable] =>
        val catalog = catalogManager.getCatalog(objectIdentifier.getCatalogName)
        val catalogTable = s.asInstanceOf[CatalogTable]
        val context = new TableSinkFactoryContextImpl(
          objectIdentifier, catalogTable, config.getConfiguration, false)
        if (catalog.isPresent && catalog.get().getTableFactory.isPresent) {
          val sink = TableFactoryUtil.createTableSinkForCatalogTable(catalog.get(), context)
          if (sink.isPresent) {
            return Option(sink.get())
          }
        }
        Option(TableFactoryUtil.findAndCreateTableSink(context))

      case _ => None
    }
  }

  private def createDummyPlanner(): StreamPlanner = {
    val dummyExecEnv = new DummyStreamExecutionEnvironment(getExecutionEnvironment)
    val executor = new StreamExecutor(dummyExecEnv)
    new StreamPlanner(executor, config, functionCatalog, catalogManager)
  }
}
