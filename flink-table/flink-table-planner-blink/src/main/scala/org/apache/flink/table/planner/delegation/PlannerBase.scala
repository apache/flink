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

package org.apache.flink.table.planner.delegation

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.{TableConfig, TableEnvironment, TableException, TableSchema}
import org.apache.flink.table.catalog._
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.delegation.{Executor, Parser, Planner}
import org.apache.flink.table.descriptors.{ConnectorDescriptorValidator, DescriptorProperties}
import org.apache.flink.table.factories.{FactoryUtil, TableFactoryUtil}
import org.apache.flink.table.operations.OutputConversionModifyOperation.UpdateMode
import org.apache.flink.table.operations._
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.calcite._
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema
import org.apache.flink.table.planner.expressions.PlannerTypeInferenceUtilImpl
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalLegacySink
import org.apache.flink.table.planner.plan.nodes.exec.{ExecGraphGenerator, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.optimize.Optimizer
import org.apache.flink.table.planner.plan.reuse.SubplanReuser
import org.apache.flink.table.planner.plan.utils.SameRelObjectShuttle
import org.apache.flink.table.planner.sinks.DynamicSinkUtils.validateSchemaAndApplyImplicitCast
import org.apache.flink.table.planner.sinks.TableSinkUtils.{inferSinkPhysicalSchema, validateLogicalPhysicalTypesCompatible, validateTableSink}
import org.apache.flink.table.planner.sinks.{DataStreamTableSink, DynamicSinkUtils, SelectTableSinkBase, SelectTableSinkSchemaConverter}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter
import org.apache.flink.table.utils.TableSchemaUtils

import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.{RelTrait, RelTraitDef}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.tools.FrameworkConfig

import java.util
import java.util.function.{Function => JFunction, Supplier => JSupplier}

import _root_.scala.collection.JavaConversions._

/**
  * Implementation of [[Planner]] for blink planner. It supports only streaming use cases.
  * (The new [[org.apache.flink.table.sources.InputFormatTableSource]] should work, but will be
  * handled as streaming sources, and no batch specific optimizations will be applied).
  *
  * @param executor        instance of [[Executor]], needed to extract
  *                        [[StreamExecutionEnvironment]] for
  *                        [[org.apache.flink.table.sources.StreamTableSource.getDataStream]]
  * @param config          mutable configuration passed from corresponding [[TableEnvironment]]
  * @param functionCatalog catalog of functions
  * @param catalogManager  manager of catalog meta objects such as tables, views, databases etc.
  * @param isStreamingMode Determines if the planner should work in a batch (false}) or
  *                        streaming (true) mode.
  */
abstract class PlannerBase(
    executor: Executor,
    config: TableConfig,
    val functionCatalog: FunctionCatalog,
    val catalogManager: CatalogManager,
    isStreamingMode: Boolean)
  extends Planner {

  // temporary utility until we don't use planner expressions anymore
  functionCatalog.setPlannerTypeInferenceUtil(PlannerTypeInferenceUtilImpl.INSTANCE)

  private val sqlExprToRexConverterFactory = new SqlExprToRexConverterFactory {
    override def create(tableRowType: RelDataType): SqlExprToRexConverter =
      plannerContext.createSqlExprToRexConverter(tableRowType)
  }

  private val parser: Parser = new ParserImpl(
    catalogManager,
    new JSupplier[FlinkPlannerImpl] {
      override def get(): FlinkPlannerImpl = createFlinkPlanner
    },
    // we do not cache the parser in order to use the most up to
    // date configuration. Users might change parser configuration in TableConfig in between
    // parsing statements
    new JSupplier[CalciteParser] {
      override def get(): CalciteParser = plannerContext.createCalciteParser()
    },
    new JFunction[TableSchema, SqlExprToRexConverter] {
      override def apply(t: TableSchema): SqlExprToRexConverter = {
        sqlExprToRexConverterFactory.create(plannerContext.getTypeFactory.buildRelNodeRowType(t))
      }
    }
  )

  @VisibleForTesting
  private[flink] val plannerContext: PlannerContext =
    new PlannerContext(
      config,
      functionCatalog,
      catalogManager,
      asRootSchema(new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
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
  private[flink] def createFlinkPlanner: FlinkPlannerImpl = {
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

  override def getParser: Parser = parser

  override def translate(
      modifyOperations: util.List[ModifyOperation]): util.List[Transformation[_]] = {
    if (modifyOperations.isEmpty) {
      return List.empty[Transformation[_]]
    }
    // prepare the execEnv before translating
    getExecEnv.configure(
      getTableConfig.getConfiguration,
      Thread.currentThread().getContextClassLoader)
    overrideEnvParallelism()

    val relNodes = modifyOperations.map(translateToRel)
    val optimizedRelNodes = optimize(relNodes)
    val execNodes = translateToExecNodePlan(optimizedRelNodes)
    translateToPlan(execNodes)
  }

  protected def overrideEnvParallelism(): Unit = {
    // Use config parallelism to override env parallelism.
    val defaultParallelism = getTableConfig.getConfiguration.getInteger(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM)
    if (defaultParallelism > 0) {
      getExecEnv.getConfig.setParallelism(defaultParallelism)
    }
  }

  override def getCompletionHints(statement: String, position: Int): Array[String] = {
    val planner = createFlinkPlanner
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
        val sinkSchema = s.getSink.getTableSchema
        // validate query schema and sink schema, and apply cast if possible
        val query = validateSchemaAndApplyImplicitCast(input, sinkSchema, null, getTypeFactory)
        LogicalLegacySink.create(
          query,
          s.getSink,
          "UnregisteredSink",
          ConnectorCatalogTable.sink(s.getSink, !isStreamingMode))

      case s: SelectSinkOperation =>
        val input = getRelBuilder.queryOperation(s.getChild).build()
        // convert query schema to sink schema
        val sinkSchema = SelectTableSinkSchemaConverter.convertTimeAttributeToRegularTimestamp(
          SelectTableSinkSchemaConverter.changeDefaultConversionClass(s.getChild.getTableSchema))
        // validate query schema and sink schema, and apply cast if possible
        val query = validateSchemaAndApplyImplicitCast(input, sinkSchema, null, getTypeFactory)
        val sink = createSelectTableSink(sinkSchema)
        s.setSelectResultProvider(sink.getSelectResultProvider)
        LogicalLegacySink.create(
          query,
          sink,
          "collect",
          ConnectorCatalogTable.sink(sink, !isStreamingMode))

      case catalogSink: CatalogSinkModifyOperation =>
        val input = getRelBuilder.queryOperation(modifyOperation.getChild).build()
        val identifier = catalogSink.getTableIdentifier
        val dynamicOptions = catalogSink.getDynamicOptions
        getTableSink(identifier, dynamicOptions).map {
          case (table, sink: TableSink[_]) =>
            // check the logical field type and physical field type are compatible
            val queryLogicalType = FlinkTypeFactory.toLogicalRowType(input.getRowType)
            // validate logical schema and physical schema are compatible
            validateLogicalPhysicalTypesCompatible(table, sink, queryLogicalType)
            // validate TableSink
            validateTableSink(catalogSink, identifier, sink, table.getPartitionKeys)
            // validate query schema and sink schema, and apply cast if possible
            val query = validateSchemaAndApplyImplicitCast(
              input,
              TableSchemaUtils.getPhysicalSchema(table.getSchema),
              catalogSink.getTableIdentifier,
              getTypeFactory)
            LogicalLegacySink.create(
              query,
              sink,
              identifier.toString,
              table,
              catalogSink.getStaticPartitions.toMap)

          case (table, sink: DynamicTableSink) =>
            DynamicSinkUtils.toRel(getRelBuilder, input, catalogSink, sink, table)
        } match {
          case Some(sinkRel) => sinkRel
          case None =>
            throw new TableException(s"Sink ${catalogSink.getTableIdentifier} does not exists")
        }

      case outputConversion: OutputConversionModifyOperation =>
        val input = getRelBuilder.queryOperation(outputConversion.getChild).build()
        val (needUpdateBefore, withChangeFlag) = outputConversion.getUpdateMode match {
          case UpdateMode.RETRACT => (true, true)
          case UpdateMode.APPEND => (false, false)
          case UpdateMode.UPSERT => (false, true)
        }
        val typeInfo = LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(outputConversion.getType)
        val inputLogicalType = FlinkTypeFactory.toLogicalRowType(input.getRowType)
        val sinkPhysicalSchema = inferSinkPhysicalSchema(
          outputConversion.getType,
          inputLogicalType,
          withChangeFlag)
        // validate query schema and sink schema, and apply cast if possible
        val query = validateSchemaAndApplyImplicitCast(
          input,
          sinkPhysicalSchema,
          null,
          getTypeFactory)
        val tableSink = new DataStreamTableSink(
          FlinkTypeFactory.toTableSchema(query.getRowType),
          typeInfo,
          needUpdateBefore,
          withChangeFlag)
        LogicalLegacySink.create(
          query,
          tableSink,
          "DataStreamTableSink",
          ConnectorCatalogTable.sink(tableSink, !isStreamingMode))

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
      optimizedRelNodes: Seq[RelNode]): util.List[ExecNode[_]] = {
    val nonPhysicalRel = optimizedRelNodes.filterNot(_.isInstanceOf[FlinkPhysicalRel])
    if (nonPhysicalRel.nonEmpty) {
      throw new TableException("The expected optimized plan is FlinkPhysicalRel plan, " +
        s"actual plan is ${nonPhysicalRel.head.getClass.getSimpleName} plan.")
    }

    require(optimizedRelNodes.forall(_.isInstanceOf[FlinkPhysicalRel]))
    // Rewrite same rel object to different rel objects
    // in order to get the correct dag (dag reuse is based on object not digest)
    val shuttle = new SameRelObjectShuttle()
    val relsWithoutSameObj = optimizedRelNodes.map(_.accept(shuttle))
    // reuse subplan
    val reusedPlan = SubplanReuser.reuseDuplicatedSubplan(relsWithoutSameObj, config)
    // convert FlinkPhysicalRel DAG to ExecNode DAG
    val generator = new ExecGraphGenerator()
    generator.generate(reusedPlan.map(_.asInstanceOf[FlinkPhysicalRel]))
  }

  /**
    * Translates a [[ExecNode]] DAG into a [[Transformation]] DAG.
    *
    * @param execNodes The node DAG to translate.
    * @return The [[Transformation]] DAG that corresponds to the node DAG.
    */
  protected def translateToPlan(execNodes: util.List[ExecNode[_]]): util.List[Transformation[_]]

  /**
   * Creates a [[SelectTableSinkBase]] for a select query.
   *
   * @param tableSchema the table schema of select result.
   * @return The sink to fetch the select result.
   */
  protected def createSelectTableSink(tableSchema: TableSchema): SelectTableSinkBase[_]

  private def getTableSink(
      objectIdentifier: ObjectIdentifier,
      dynamicOptions: JMap[String, String])
    : Option[(CatalogTable, Any)] = {
    val lookupResult = JavaScalaConversionUtil.toScala(catalogManager.getTable(objectIdentifier))
    lookupResult
      .map(_.getTable) match {
      case Some(table: ConnectorCatalogTable[_, _]) =>
        JavaScalaConversionUtil.toScala(table.getTableSink) match {
          case Some(sink) => Some(table, sink)
          case None => None
        }

      case Some(table: CatalogTable) =>
        val catalog = catalogManager.getCatalog(objectIdentifier.getCatalogName)
        val tableToFind = if (dynamicOptions.nonEmpty) {
          table.copy(FlinkHints.mergeTableOptions(dynamicOptions, table.getProperties))
        } else {
          table
        }
        val isTemporary = lookupResult.get.isTemporary
        if (isLegacyConnectorOptions(objectIdentifier, table, isTemporary)) {
          val tableSink = TableFactoryUtil.findAndCreateTableSink(
            catalog.orElse(null),
            objectIdentifier,
            tableToFind,
            getTableConfig.getConfiguration,
            isStreamingMode,
            isTemporary)
          Option(table, tableSink)
        } else {
          val tableSink = FactoryUtil.createTableSink(
            catalog.orElse(null),
            objectIdentifier,
            tableToFind,
            getTableConfig.getConfiguration,
            Thread.currentThread().getContextClassLoader,
            isTemporary)
          Option(table, tableSink)
        }

      case _ => None
    }
  }

  /**
   * Checks whether the [[CatalogTable]] uses legacy connector sink options.
   */
  private def isLegacyConnectorOptions(
      objectIdentifier: ObjectIdentifier,
      catalogTable: CatalogTable,
      isTemporary: Boolean) = {
    // normalize option keys
    val properties = new DescriptorProperties(true)
    properties.putProperties(catalogTable.getOptions)
    if (properties.containsKey(ConnectorDescriptorValidator.CONNECTOR_TYPE)) {
      true
    } else {
      val catalog = catalogManager.getCatalog(objectIdentifier.getCatalogName)
      try {
        // try to create legacy table source using the options,
        // some legacy factories uses the new 'connector' key
        TableFactoryUtil.findAndCreateTableSink(
          catalog.orElse(null),
          objectIdentifier,
          catalogTable,
          getTableConfig.getConfiguration,
          isStreamingMode,
          isTemporary)
        // success, then we will use the legacy factories
        true
      } catch {
        // fail, then we will use new factories
        case _: Throwable => false
      }
    }
  }
}
