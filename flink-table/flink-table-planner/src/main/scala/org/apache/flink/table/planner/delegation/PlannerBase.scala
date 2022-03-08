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
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, TableConfigOptions}
import org.apache.flink.table.catalog.ManagedTableListener.isManagedTable
import org.apache.flink.table.catalog._
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.delegation.{Executor, Parser, Planner}
import org.apache.flink.table.descriptors.{ConnectorDescriptorValidator, DescriptorProperties}
import org.apache.flink.table.factories.{DynamicTableSinkFactory, FactoryUtil, TableFactoryUtil}
import org.apache.flink.table.module.{Module, ModuleManager}
import org.apache.flink.table.operations.OutputConversionModifyOperation.UpdateMode
import org.apache.flink.table.operations._
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.calcite._
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema
import org.apache.flink.table.planner.connectors.DynamicSinkUtils
import org.apache.flink.table.planner.connectors.DynamicSinkUtils.validateSchemaAndApplyImplicitCast
import org.apache.flink.table.planner.delegation.ParserFactory.DefaultParserContext
import org.apache.flink.table.planner.expressions.PlannerTypeInferenceUtilImpl
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalLegacySink
import org.apache.flink.table.planner.plan.nodes.exec.processor.{ExecNodeGraphProcessor, ProcessorContext}
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNodeGraph, ExecNodeGraphGenerator}
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.optimize.Optimizer
import org.apache.flink.table.planner.plan.reuse.SubplanReuser
import org.apache.flink.table.planner.plan.utils.SameRelObjectShuttle
import org.apache.flink.table.planner.sinks.DataStreamTableSink
import org.apache.flink.table.planner.sinks.TableSinkUtils.{inferSinkPhysicalSchema, validateLogicalPhysicalTypesCompatible, validateTableSink}
import org.apache.flink.table.planner.utils.InternalConfigOptions.{TABLE_QUERY_START_EPOCH_TIME, TABLE_QUERY_START_LOCAL_TIME}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.{toJava, toScala}
import org.apache.flink.table.runtime.generated.CompileUtils
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter

import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.{RelTrait, RelTraitDef}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalTableModify
import org.apache.calcite.tools.FrameworkConfig

import java.lang.{Long => JLong}
import java.util
import java.util.{Collections, TimeZone}

import _root_.scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Implementation of a [[Planner]]. It supports only streaming use cases.
  * (The new [[org.apache.flink.table.sources.InputFormatTableSource]] should work, but will be
  * handled as streaming sources, and no batch specific optimizations will be applied).
  *
  * @param executor        instance of [[Executor]], needed to extract
  *                        [[StreamExecutionEnvironment]] for
  *                        [[org.apache.flink.table.sources.StreamTableSource.getDataStream]]
  * @param tableConfig     mutable configuration passed from corresponding [[TableEnvironment]]
  * @param moduleManager   manager for modules
  * @param functionCatalog catalog of functions
  * @param catalogManager  manager of catalog meta objects such as tables, views, databases etc.
  * @param isStreamingMode Determines if the planner should work in a batch (false}) or
  *                        streaming (true) mode.
  */
abstract class PlannerBase(
    executor: Executor,
    tableConfig: TableConfig,
    val moduleManager: ModuleManager,
    val functionCatalog: FunctionCatalog,
    val catalogManager: CatalogManager,
    isStreamingMode: Boolean)
  extends Planner {

  // temporary utility until we don't use planner expressions anymore
  functionCatalog.setPlannerTypeInferenceUtil(PlannerTypeInferenceUtilImpl.INSTANCE)

  private var parser: Parser = _
  private var currentDialect: SqlDialect = getTableConfig.getSqlDialect

  private val plannerConfiguration: ReadableConfig = new PlannerConfig(
    tableConfig.getConfiguration,
    executor.getConfiguration)

  @VisibleForTesting
  private[flink] val plannerContext: PlannerContext =
    new PlannerContext(
      !isStreamingMode,
      tableConfig,
      moduleManager,
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

  def getTableConfig: TableConfig = tableConfig

  def getFlinkContext: FlinkContext = plannerContext.getFlinkContext

  /**
   * Gives access to both API specific table configuration and executor configuration.
   *
   * This configuration should be the main source of truth in the planner module.
   */
  def getConfiguration: ReadableConfig = plannerConfiguration

  /**
   * @deprecated Do not use this method anymore. Use [[getConfiguration]] to access options.
   *             Create transformations without it. A [[StreamExecutionEnvironment]] is a mixture
   *             of executor and stream graph generator/builder. In the long term, we would like
   *             to avoid the need for it in the planner module.
   */
  @deprecated
  private[flink] def getExecEnv: StreamExecutionEnvironment = {
    executor.asInstanceOf[DefaultExecutor].getExecutionEnvironment
  }

  def createNewParser: Parser = {
    val factoryIdentifier = getTableConfig.getSqlDialect.name().toLowerCase
    val parserFactory = FactoryUtil.discoverFactory(
      getClass.getClassLoader, classOf[ParserFactory], factoryIdentifier)

    val context = new DefaultParserContext(catalogManager, plannerContext)
    parserFactory.create(context)
  }

  override def getParser: Parser = {
    if (parser == null || getTableConfig.getSqlDialect != currentDialect) {
      parser = createNewParser
      currentDialect = getTableConfig.getSqlDialect
    }
    parser
  }

  override def translate(
      modifyOperations: util.List[ModifyOperation]): util.List[Transformation[_]] = {
    beforeTranslation()
    if (modifyOperations.isEmpty) {
      return List.empty[Transformation[_]]
    }

    val relNodes = modifyOperations.map(translateToRel)
    val optimizedRelNodes = optimize(relNodes)
    val execGraph = translateToExecNodeGraph(optimizedRelNodes)
    val transformations = translateToPlan(execGraph)
    afterTranslation()
    transformations
  }

  /**
    * Converts a relational tree of [[ModifyOperation]] into a Calcite relational expression.
    */
  @VisibleForTesting
  private[flink] def translateToRel(modifyOperation: ModifyOperation): RelNode = {
    val dataTypeFactory = catalogManager.getDataTypeFactory
    modifyOperation match {
      case s: UnregisteredSinkModifyOperation[_] =>
        val input = getRelBuilder.queryOperation(s.getChild).build()
        val sinkSchema = s.getSink.getTableSchema
        // validate query schema and sink schema, and apply cast if possible
        val query = validateSchemaAndApplyImplicitCast(
          input,
          catalogManager.getSchemaResolver.resolve(sinkSchema.toSchema),
          null,
          dataTypeFactory,
          getTypeFactory)
        LogicalLegacySink.create(
          query,
          s.getSink,
          "UnregisteredSink",
          ConnectorCatalogTable.sink(s.getSink, !isStreamingMode))

      case collectModifyOperation: CollectModifyOperation =>
        val input = getRelBuilder.queryOperation(modifyOperation.getChild).build()
        DynamicSinkUtils.convertCollectToRel(
          getRelBuilder,
          input,
          collectModifyOperation,
          getTableConfig.getConfiguration,
          getFlinkContext.getClassLoader
        )

      case catalogSink: SinkModifyOperation =>
        val input = getRelBuilder.queryOperation(modifyOperation.getChild).build()
        val dynamicOptions = catalogSink.getDynamicOptions
        getTableSink(catalogSink.getContextResolvedTable, dynamicOptions).map {
          case (table, sink: TableSink[_]) =>
            // Legacy tables can't be anonymous
            val identifier = catalogSink.getContextResolvedTable.getIdentifier
            // check the logical field type and physical field type are compatible
            val queryLogicalType = FlinkTypeFactory.toLogicalRowType(input.getRowType)
            // validate logical schema and physical schema are compatible
            validateLogicalPhysicalTypesCompatible(table, sink, queryLogicalType)
            // validate TableSink
            validateTableSink(catalogSink, identifier, sink, table.getPartitionKeys)
            // validate query schema and sink schema, and apply cast if possible
            val query = validateSchemaAndApplyImplicitCast(
              input,
              table.getResolvedSchema,
              identifier.asSummaryString,
              dataTypeFactory,
              getTypeFactory)
            val hints = new util.ArrayList[RelHint]
            if (!dynamicOptions.isEmpty) {
              hints.add(RelHint.builder("OPTIONS").hintOptions(dynamicOptions).build)
            }
            LogicalLegacySink.create(
              query,
              hints,
              sink,
              identifier.toString,
              table,
              catalogSink.getStaticPartitions.toMap)

          case (table, sink: DynamicTableSink) =>
            DynamicSinkUtils.convertSinkToRel(getRelBuilder, input, catalogSink, sink)
        } match {
          case Some(sinkRel) => sinkRel
          case None =>
            throw new TableException(
              s"Sink '${catalogSink.getContextResolvedTable}' does not exists")
        }

      case externalModifyOperation: ExternalModifyOperation =>
        val input = getRelBuilder.queryOperation(modifyOperation.getChild).build()
        DynamicSinkUtils.convertExternalToRel(getRelBuilder, input, externalModifyOperation)

      // legacy
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
          catalogManager.getSchemaResolver.resolve(sinkPhysicalSchema.toSchema),
          null,
          dataTypeFactory,
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
   * Converts [[FlinkPhysicalRel]] DAG to [[ExecNodeGraph]],
   * tries to reuse duplicate sub-plans and transforms the graph based on the given processors.
   */
  @VisibleForTesting
  private[flink] def translateToExecNodeGraph(optimizedRelNodes: Seq[RelNode]): ExecNodeGraph = {
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
    val reusedPlan = SubplanReuser.reuseDuplicatedSubplan(relsWithoutSameObj, tableConfig)
    // convert FlinkPhysicalRel DAG to ExecNodeGraph
    val generator = new ExecNodeGraphGenerator()
    val execGraph = generator.generate(reusedPlan.map(_.asInstanceOf[FlinkPhysicalRel]))

    // process the graph
    val context = new ProcessorContext(this)
    val processors = getExecNodeGraphProcessors
    processors.foldLeft(execGraph)((graph, processor) => processor.process(graph, context))
  }

  protected def getExecNodeGraphProcessors: Seq[ExecNodeGraphProcessor]

  /**
    * Translates an [[ExecNodeGraph]] into a [[Transformation]] DAG.
    *
    * @param execGraph The node graph to translate.
    * @return The [[Transformation]] DAG that corresponds to the node DAG.
    */
  protected def translateToPlan(execGraph: ExecNodeGraph): util.List[Transformation[_]]

  private def getTableSink(
      contextResolvedTable: ContextResolvedTable,
      dynamicOptions: JMap[String, String])
    : Option[(ResolvedCatalogTable, Any)] = {
    contextResolvedTable.getTable[CatalogBaseTable] match {
      case connectorTable: ConnectorCatalogTable[_, _] =>
        val resolvedTable = contextResolvedTable.getResolvedTable[ResolvedCatalogTable]
        toScala(connectorTable.getTableSink) match {
          case Some(sink) => Some(resolvedTable, sink)
          case None => None
        }

      case regularTable: CatalogTable =>
        val resolvedTable = contextResolvedTable.getResolvedTable[ResolvedCatalogTable]
        val tableToFind = if (dynamicOptions.nonEmpty) {
          resolvedTable.copy(FlinkHints.mergeTableOptions(dynamicOptions, resolvedTable.getOptions))
        } else {
          resolvedTable
        }
        val catalog = toScala(contextResolvedTable.getCatalog)
        val objectIdentifier = contextResolvedTable.getIdentifier
        val isTemporary = contextResolvedTable.isTemporary

        if (isStreamingMode && isManagedTable(catalog.orNull, resolvedTable) &&
          !executor.isCheckpointingEnabled) {
          throw new TableException(
            s"You should enable the checkpointing for sinking to managed table " +
              s"'${contextResolvedTable}', managed table relies on checkpoint to commit and " +
              s"the data is visible only after commit.")
        }

        if (!contextResolvedTable.isAnonymous &&
          isLegacyConnectorOptions(objectIdentifier, resolvedTable.getOrigin, isTemporary)) {
          val tableSink = TableFactoryUtil.findAndCreateTableSink(
            catalog.orNull,
            objectIdentifier,
            tableToFind.getOrigin,
            getTableConfig.getConfiguration,
            isStreamingMode,
            isTemporary)
          Option(resolvedTable, tableSink)
        } else {
          val factoryFromCatalog = catalog.flatMap(f => toScala(f.getFactory)) match {
            case Some(f: DynamicTableSinkFactory) => Some(f)
            case _ => None
          }

          val factoryFromModule = toScala(plannerContext.getFlinkContext.getModuleManager
            .getFactory(toJava((m: Module) => m.getTableSinkFactory)))

          // Since the catalog is more specific, we give it precedence over a factory provided by
          // any modules.
          val factory = factoryFromCatalog.orElse(factoryFromModule).orNull

          val tableSink = FactoryUtil.createDynamicTableSink(
            factory,
            objectIdentifier,
            tableToFind,
            Collections.emptyMap(),
            getTableConfig.getConfiguration,
            getFlinkContext.getClassLoader,
            isTemporary)
          Option(resolvedTable, tableSink)
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

  protected def createSerdeContext: SerdeContext = {
    val planner = createFlinkPlanner
    new SerdeContext(
      getParser,
      planner.config.getContext.asInstanceOf[FlinkContext],
      getFlinkContext.getClassLoader,
      plannerContext.getTypeFactory,
      planner.operatorTable
    )
  }

  protected def beforeTranslation(): Unit = {
    val configuration = tableConfig.getConfiguration

    // Add query start time to TableConfig, these config are used internally,
    // these configs will be used by temporal functions like CURRENT_TIMESTAMP,LOCALTIMESTAMP.
    val epochTime :JLong = System.currentTimeMillis()
    configuration.set(TABLE_QUERY_START_EPOCH_TIME, epochTime)
    val localTime :JLong =  epochTime +
      TimeZone.getTimeZone(tableConfig.getLocalTimeZone).getOffset(epochTime)
    configuration.set(TABLE_QUERY_START_LOCAL_TIME, localTime)

    getExecEnv.configure(
      configuration,
      Thread.currentThread().getContextClassLoader)

    // Use config parallelism to override env parallelism.
    val defaultParallelism = getTableConfig.getConfiguration.getInteger(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM)
    if (defaultParallelism > 0) {
      getExecEnv.getConfig.setParallelism(defaultParallelism)
    }
  }

  protected def afterTranslation(): Unit = {
    // Cleanup all internal configuration after plan translation finished.
    val configuration = tableConfig.getConfiguration
    configuration.removeConfig(TABLE_QUERY_START_EPOCH_TIME)
    configuration.removeConfig(TABLE_QUERY_START_LOCAL_TIME)

    // Clean caches that might have filled up during optimization
    CompileUtils.cleanUp()
  }

  /**
   * Returns all the graphs required to execute EXPLAIN
   */
  private[flink] def getExplainGraphs(operations: util.List[Operation]
      ): (mutable.Buffer[RelNode], Seq[RelNode], ExecNodeGraph, StreamGraph) = {
    require(operations.nonEmpty, "operations should not be empty")
    beforeTranslation()
    val sinkRelNodes = operations.map {
      case queryOperation: QueryOperation =>
        val relNode = getRelBuilder.queryOperation(queryOperation).build()
        relNode match {
          // SQL: explain plan for insert into xx
          case modify: LogicalTableModify =>
            // convert LogicalTableModify to SinkModifyOperation
            val qualifiedName = modify.getTable.getQualifiedName
            require(qualifiedName.size() == 3, "the length of qualified name should be 3.")
            val objectIdentifier = ObjectIdentifier.of(
              qualifiedName.get(0), qualifiedName.get(1), qualifiedName.get(2))
            val contextResolvedTable = catalogManager.getTableOrError(objectIdentifier)
            val modifyOperation = new SinkModifyOperation(
              contextResolvedTable,
              new PlannerQueryOperation(modify.getInput)
            )
            translateToRel(modifyOperation)
          case _ =>
            relNode
        }
      case modifyOperation: ModifyOperation =>
        translateToRel(modifyOperation)
      case o => throw new TableException(s"Unsupported operation: ${o.getClass.getCanonicalName}")
    }
    val optimizedRelNodes = optimize(sinkRelNodes)
    val execGraph = translateToExecNodeGraph(optimizedRelNodes)

    val transformations = translateToPlan(execGraph)
    afterTranslation()

    val streamGraph = executor.createPipeline(transformations, tableConfig.getConfiguration, null)
      .asInstanceOf[StreamGraph]

    (sinkRelNodes, optimizedRelNodes, execGraph, streamGraph)
  }
}
