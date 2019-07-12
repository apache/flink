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
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment, TableException}
import org.apache.flink.table.calcite.{FlinkPlannerImpl, FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.catalog.{CatalogManager, CatalogManagerCalciteSchema, CatalogTable, ConnectorCatalogTable, FunctionCatalog}
import org.apache.flink.table.delegation.{Executor, Planner}
import org.apache.flink.table.executor.ExecutorBase
import org.apache.flink.table.factories.{TableFactoryService, TableFactoryUtil, TableSinkFactory}
import org.apache.flink.table.operations.OutputConversionModifyOperation.UpdateMode
import org.apache.flink.table.operations.{CatalogSinkModifyOperation, ModifyOperation, Operation, OutputConversionModifyOperation, PlannerQueryOperation, UnregisteredSinkModifyOperation}
import org.apache.flink.table.plan.nodes.calcite.LogicalSink
import org.apache.flink.table.plan.nodes.exec.ExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.optimize.Optimizer
import org.apache.flink.table.plan.reuse.SubplanReuser
import org.apache.flink.table.plan.util.SameRelObjectShuttle
import org.apache.flink.table.sinks.{DataStreamTableSink, TableSink, TableSinkUtils}
import org.apache.flink.table.sqlexec.SqlToOperationConverter
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter
import org.apache.flink.table.util.JavaScalaConversionUtil

import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.{RelTrait, RelTraitDef}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.{SqlIdentifier, SqlInsert, SqlKind}
import org.apache.calcite.tools.FrameworkConfig

import _root_.java.util.{List => JList}
import java.util

import _root_.scala.collection.JavaConversions._

/**
  * Implementation of [[Planner]] for legacy Flink planner. It supports only streaming use cases.
  * (The new [[org.apache.flink.table.sources.InputFormatTableSource]] should work, but will be
  * handled as streaming sources, and no batch specific optimizations will be applied).
  *
  * @param executor        instance of [[Executor]], needed to extract
  *                        [[StreamExecutionEnvironment]] for
  *                        [[org.apache.flink.table.sources.StreamTableSource.getDataStream]]
  * @param config          mutable configuration passed from corresponding [[TableEnvironment]]
  * @param functionCatalog catalog of functions
  * @param catalogManager  manager of catalog meta objects such as tables, views, databases etc.
  */
abstract class PlannerBase(
    executor: Executor,
    config: TableConfig,
    val functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager)
  extends Planner {

  executor.asInstanceOf[ExecutorBase].setTableConfig(config)

  private val plannerContext: PlannerContext =
    new PlannerContext(
      config,
      functionCatalog,
      asRootSchema(new CatalogManagerCalciteSchema(catalogManager)),
      getTraitDefs.toList
    )

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase
    plannerContext.createRelBuilder(currentCatalogName, currentDatabase)
  }

  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  @VisibleForTesting
  private[flink] def getFlinkPlanner: FlinkPlannerImpl = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase
    plannerContext.createFlinkPlanner(currentCatalogName, currentDatabase)
  }

  /** Returns the [[FlinkTypeFactory]] of this TableEnvironment. */
  private[flink] def getTypeFactory: FlinkTypeFactory = plannerContext.getTypeFactory

  /** Returns specific RelTraitDefs depends on the concrete type of this TableEnvironment. */
  protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]]

  /** Returns specific query [[Optimizer]] depends on the concrete type of this TableEnvironment. */
  protected def getOptimizer: Optimizer

  def getTableConfig: TableConfig = config

  private[flink] def getExecEnv: StreamExecutionEnvironment = {
    executor.asInstanceOf[ExecutorBase].getExecutionEnvironment
  }

  override def parse(stmt: String): util.List[Operation] = {
    val planner = getFlinkPlanner
    // parse the sql query
    val parsed = planner.parse(stmt)
    parsed match {
      case insert: SqlInsert =>
        // get name of sink table
        val targetTablePath = insert.getTargetTable.asInstanceOf[SqlIdentifier].names

        List(new CatalogSinkModifyOperation(
          targetTablePath,
          SqlToOperationConverter.convert(planner,
            insert.getSource).asInstanceOf[PlannerQueryOperation]).asInstanceOf[Operation])
      case query if query.getKind.belongsTo(SqlKind.QUERY) =>
        // validate the sql query
        val validated = planner.validate(query)
        // transform to a relational tree
        val relational = planner.rel(validated)
        // can not use SqlToOperationConverter because of the project()
        List(new PlannerQueryOperation(relational.project()))
      case ddl if ddl.getKind.belongsTo(SqlKind.DDL) =>
        List(SqlToOperationConverter.convert(planner, ddl))
      case _ =>
        throw new TableException(s"Unsupported query: $stmt")
    }
  }

  override def translate(
      modifyOperations: util.List[ModifyOperation]): util.List[Transformation[_]] = {
    if (modifyOperations.isEmpty) {
      return List.empty[Transformation[_]]
    }
    mergeParameters()
    val relNodes = modifyOperations.map(translateToRel)
    val optimizedRelNodes = optimize(relNodes)
    val execNodes = translateToExecNodePlan(optimizedRelNodes)
    translateToPlan(execNodes)
  }

  override def getCompletionHints(statement: String, position: Int): Array[String] = {
    val planner = getFlinkPlanner
    planner.getCompletionHints(statement, position)
  }

  /**
    * Converts a relational tree of [[ModifyOperation]] into a Calcite relational expression.
    */
  @VisibleForTesting
  private[flink] def translateToRel(modifyOperation: ModifyOperation): RelNode = {
    modifyOperation match {
      case s: UnregisteredSinkModifyOperation[_] =>
        val input = getRelBuilder.queryOperation(s.getChild).build()
        LogicalSink.create(input, s.getSink, "UnregisteredSink")

      case catalogSink: CatalogSinkModifyOperation =>
        val input = getRelBuilder.queryOperation(modifyOperation.getChild).build()
        getTableSink(catalogSink.getTablePath).map(sink => {
          TableSinkUtils.validateSink(catalogSink.getChild, catalogSink.getTablePath, sink)
          LogicalSink.create(input, sink, catalogSink.getTablePath.mkString("."))
        }) match {
          case Some(sinkRel) => sinkRel
          case None => throw new TableException(s"Sink ${catalogSink.getTablePath} does not exists")
        }

      case outputConversion: OutputConversionModifyOperation =>
        val input = getRelBuilder.queryOperation(outputConversion.getChild).build()
        val (updatesAsRetraction, withChangeFlag) = outputConversion.getUpdateMode match {
          case UpdateMode.RETRACT => (true, true)
          case UpdateMode.APPEND => (false, false)
          case UpdateMode.UPSERT => (false, true)
        }
        val typeInfo = LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(outputConversion.getType)
        val tableSink = new DataStreamTableSink(
          outputConversion.getChild, typeInfo, updatesAsRetraction, withChangeFlag)
        LogicalSink.create(input, tableSink, "DataStreamTableSink")

      case _ =>
        throw new TableException(s"Unsupported ModifyOperation: $modifyOperation")
    }
  }

  @VisibleForTesting
  private[flink] def optimize(relNodes: Seq[RelNode]): Seq[RelNode] = {
    val optimizedRelNodes = getOptimizer.optimize(relNodes)
    require(optimizedRelNodes.size == relNodes.size)
    optimizedRelNodes
  }

  @VisibleForTesting
  private[flink] def optimize(relNode: RelNode): RelNode = {
    val optimizedRelNodes = getOptimizer.optimize(Seq(relNode))
    require(optimizedRelNodes.size == 1)
    optimizedRelNodes.head
  }

  /**
    * Converts [[FlinkPhysicalRel]] DAG to [[ExecNode]] DAG, and tries to reuse duplicate sub-plans.
    */
  @VisibleForTesting
  private[flink] def translateToExecNodePlan(
      optimizedRelNodes: Seq[RelNode]): util.List[ExecNode[_, _]] = {
    require(optimizedRelNodes.forall(_.isInstanceOf[FlinkPhysicalRel]))
    // Rewrite same rel object to different rel objects
    // in order to get the correct dag (dag reuse is based on object not digest)
    val shuttle = new SameRelObjectShuttle()
    val relsWithoutSameObj = optimizedRelNodes.map(_.accept(shuttle))
    // reuse subplan
    val reusedPlan = SubplanReuser.reuseDuplicatedSubplan(relsWithoutSameObj, config)
    // convert FlinkPhysicalRel DAG to ExecNode DAG
    reusedPlan.map(_.asInstanceOf[ExecNode[_, _]])
  }

  /**
    * Translates a [[ExecNode]] DAG into a [[Transformation]] DAG.
    *
    * @param execNodes The node DAG to translate.
    * @return The [[Transformation]] DAG that corresponds to the node DAG.
    */
  protected def translateToPlan(execNodes: util.List[ExecNode[_, _]]): util.List[Transformation[_]]

  private def getTableSink(tablePath: JList[String]): Option[TableSink[_]] = {
    JavaScalaConversionUtil.toScala(catalogManager.resolveTable(tablePath: _*)) match {
      case Some(s) if s.getExternalCatalogTable.isPresent =>

        Option(TableFactoryUtil.findAndCreateTableSink(s.getExternalCatalogTable.get()))

      case Some(s) if JavaScalaConversionUtil.toScala(s.getCatalogTable)
        .exists(_.isInstanceOf[ConnectorCatalogTable[_, _]]) =>

        JavaScalaConversionUtil
          .toScala(s.getCatalogTable.get().asInstanceOf[ConnectorCatalogTable[_, _]].getTableSink)

      case Some(s) if JavaScalaConversionUtil.toScala(s.getCatalogTable)
        .exists(_.isInstanceOf[CatalogTable]) =>

        val sinkProperties = s.getCatalogTable.get().asInstanceOf[CatalogTable].toProperties
        Option(TableFactoryService.find(classOf[TableSinkFactory[_]], sinkProperties)
          .createTableSink(sinkProperties))

      case _ => None
    }
  }

  /**
    * Merge global job parameters and table config parameters,
    * and set the merged result to GlobalJobParameters
    */
  private def mergeParameters(): Unit = {
    val execEnv = getExecEnv
    if (execEnv != null && execEnv.getConfig != null) {
      val parameters = new Configuration()
      if (config != null && config.getConfiguration != null) {
        parameters.addAll(config.getConfiguration)
      }

      if (execEnv.getConfig.getGlobalJobParameters != null) {
        execEnv.getConfig.getGlobalJobParameters.toMap.foreach {
          kv => parameters.setString(kv._1, kv._2)
        }
      }

      execEnv.getConfig.setGlobalJobParameters(parameters)
    }
  }
}
