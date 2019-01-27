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

import org.apache.flink.annotation.{InterfaceStability, VisibleForTesting}
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.{ExecutionMode, JobExecutionResult}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.schedule.VertexInputTracker.{InputDependencyConstraint, VertexInputTrackerOptions}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.{StreamGraph, StreamGraphGenerator}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, RowType}
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.descriptors.{BatchTableDescriptor, ConnectorDescriptor}
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.expressions.{Expression, TimeAttribute}
import org.apache.flink.table.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.plan.cost.{FlinkBatchCost, FlinkCostFactory}
import org.apache.flink.table.plan.logical.{LogicalNode, SinkNode}
import org.apache.flink.table.plan.nodes.calcite.LogicalSink
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecSink, BatchPhysicalRel}
import org.apache.flink.table.plan.nodes.process.DAGProcessContext
import org.apache.flink.table.plan.optimize.{BatchOptimizeContext, FlinkBatchPrograms}
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.subplan.BatchDAGOptimizer
import org.apache.flink.table.plan.util.{DeadlockBreakupProcessor, FlinkNodeOptUtil, FlinkRelOptUtil, SameRelObjectShuttle, SubplanReuseUtil}
import org.apache.flink.table.resource.batch.RunningUnitKeeper
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{BatchTableSource, _}
import org.apache.flink.table.temptable.TableServiceException
import org.apache.flink.table.util.PlanUtil._
import org.apache.flink.table.util._
import org.apache.flink.util.{AbstractID, ExceptionUtils, Preconditions}
import org.apache.calcite.plan.{Context, ConventionTraitDef, RelOptPlanner}
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.sql2rel.SqlToRelConverter.Config
import _root_.java.util.{ArrayList => JArrayList, List => JList, Map => JMap, Set => JSet}

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ArrayBuffer
import _root_.scala.util.{Failure, Success, Try}

/**
 *  A session to construct between [[Table]] and [[DataStream]], its main function is:
  *
  *  1. Get a table from [[DataStream]], or through registering a [[TableSource]];
  *  2. Transform current already construct table to [[DataStream]];
  *  3. Add [[TableSink]] to the [[Table]].
 * @param config The [[TableConfig]] of this [[BatchTableEnvironment]].
 */
@InterfaceStability.Evolving
class BatchTableEnvironment(
    val streamEnv: StreamExecutionEnvironment,
    config: TableConfig)
    extends TableEnvironment(streamEnv, config) {

  private val ruKeeper = new RunningUnitKeeper(this)

  /** Fetch [[RunningUnitKeeper]] bond with this table env. */
  private[table] def getRUKeeper: RunningUnitKeeper = ruKeeper

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  override protected def createRelBuilder: FlinkRelBuilder = FlinkRelBuilder.create(
    frameworkConfig,
    config,
    getTypeFactory,
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE),
    catalogManager
  )

  // prefix for unique table names.
  override private[flink] val tableNamePrefix = "_DataStreamTable_"

  /**
   * `expand` is set as false, and each sub-query becomes a [[org.apache.calcite.rex.RexSubQuery]].
   */
  override protected def getSqlToRelConverterConfig: Config =
    SqlToRelConverter.configBuilder()
        .withTrimUnusedFields(false)
        .withConvertTableAccess(false)
        .withExpand(false)
        .withInSubQueryThreshold(Int.MaxValue)
        .build()

  /**
   * Returns specific FlinkCostFactory of this Environment.
   */
  override protected def getFlinkCostFactory: FlinkCostFactory = FlinkBatchCost.FACTORY

  /**
    * Triggers the program execution with specific job name.
    * @param jobName name for the job
    */
  override def execute(jobName: String): JobExecutionResult = {
    tableServiceManager.startTableServiceJob()
    val sinkNodesBak = new mutable.MutableList[SinkNode]
    sinkNodesBak ++= sinkNodes
    Try {
      val streamGraph = generateStreamGraph(jobName)
      val res = executeStreamGraph(streamGraph)
      tableServiceManager.markAllTablesCached()
      res
    } match {
      case Success(value) => value
      case Failure(ex) => ex match {
        case je: JobExecutionException
          if ExceptionUtils.findThrowable(je, classOf[TableServiceException]).isPresent =>
          sinkNodes ++= sinkNodesBak
          tableServiceManager.invalidateCachedTable()
          val streamGraph = generateStreamGraph(jobName)
          executeStreamGraph(streamGraph)
        case _ => throw ex
      }
    }
  }

  private[flink] override def collect[T](
      table: Table,
      sink: CollectTableSink[T],
      jobName: Option[String]): Seq[T] = {
    val outType = sink.getOutputType
    val typeSerializer = DataTypes.createExternalSerializer(outType)
        .asInstanceOf[TypeSerializer[T]]
    val id = new AbstractID().toString
    sink.init(typeSerializer, id)
    writeToSink(table, sink)
    val res = execute(jobName.getOrElse(DEFAULT_JOB_NAME))
    val accResult: JArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    SerializedListAccumulator.deserializeList(accResult, typeSerializer).asScala
  }

  protected override def translateStreamGraph(
      streamingTransformations: ArrayBuffer[StreamTransformation[_]],
      jobName: Option[String]): StreamGraph = {
    mergeParameters()
    val context = StreamGraphGenerator.Context.buildBatchProperties(streamEnv)
    if (getConfig.getConf.getBoolean(TableConfigOptions.SQL_EXEC_DATA_EXCHANGE_MODE_ALL_BATCH)) {
      val constraint = getConfig.getConf.getValue(
        VertexInputTrackerOptions.INPUT_DEPENDENCY_CONSTRAINT)
      // When the user does not set this value,
      // in batch mode, use ALL to avoid deadlock and resource utilization rate.
      if (constraint == null) {
        getConfig.getConf.setString(
          VertexInputTrackerOptions.INPUT_DEPENDENCY_CONSTRAINT,
          InputDependencyConstraint.ALL.toString)
      }
      context.getExecutionConfig.setExecutionMode(ExecutionMode.BATCH)
    } else {
      context.getExecutionConfig.setExecutionMode(ExecutionMode.PIPELINED)
    }
    context.setSlotSharingEnabled(false)

    ruKeeper.setScheduleConfig(context)
    jobName match {
      case Some(jn) => context.setJobName(jn)
      case None => context.setJobName(DEFAULT_JOB_NAME)
    }
    val streamGraph = StreamGraphGenerator.generate(context, streamingTransformations)

    setupOperatorMetricCollect()
    ruKeeper.clear()

    streamingTransformations.clear()
    streamGraph
  }

  private def executeStreamGraph(streamGraph: StreamGraph): JobExecutionResult = {
    val result = streamEnv.execute(streamGraph)
    dumpPlanWithMetricsIfNeed(streamGraph, result)
    result
  }

  /**
    * Set up operator metric collect to be true.
    */
  @VisibleForTesting
  private[flink] def setupOperatorMetricCollect(): Unit = {
    if (streamEnv != null &&
        streamEnv.getConfig != null &&
        config.getConf.getBoolean(TableConfigOptions.SQL_EXEC_OPERATOR_METRIC_DUMP_ENABLED)) {
      val parameters = new Configuration()
      Option(streamEnv.getConfig.getGlobalJobParameters).foreach(gb =>
        gb.toMap.foreach(kv => parameters.setString(kv._1, kv._2))
      )
      parameters.setString(
        AbstractStreamOperatorWithMetrics.METRICS_CONF_KEY,
        AbstractStreamOperatorWithMetrics.METRICS_CONF_VALUE)
      streamEnv.getConfig.setGlobalJobParameters(parameters)
    }
  }

  override private[table] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      sinkName: String): Unit = {
    val sinkNode = SinkNode(table.logicalPlan, sink, sinkName)
    if (config.getSubsectionOptimization) {
      sinkNodes += sinkNode
    } else {
      val transformation = translate(table, sink, sinkName)
      transformations.add(transformation)
    }
  }

  /**
   * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
   * catalog.
   *
   * @param name     The name under which the table is registered in the catalog.
   * @param boundedStream The [[DataStream]] to register as table in the catalog.
   * @tparam T the type of the [[DataStream]].
   */
  protected def registerBoundedStreamInternal[T](
      name: String, boundedStream: DataStream[T], replace: Boolean): Unit = {
    val (fieldNames, fieldIdxs) = getFieldInfo(boundedStream.getTransformation.getOutputType)
    val boundedStreamTable = new DataStreamTable[T](boundedStream, fieldIdxs, fieldNames)
    registerTableInternal(name, boundedStreamTable, replace)
  }

  /**
   * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
   * catalog.
   *
   * @param name           The name under which the table is registered in the catalog.
   * @param boundedStream       The [[DataStream]] to register as table in the catalog.
   * @param fieldNullables The field isNullables attributes of boundedStream.
   * @tparam T the type of the [[DataStream]].
   */
  protected def registerBoundedStreamInternal[T](
      name: String,
      boundedStream: DataStream[T],
      fieldNullables: Array[Boolean],
      replace: Boolean): Unit = {
    val dataType = boundedStream.getTransformation.getOutputType
    val (fieldNames, fieldIndexes) = getFieldInfo(dataType)
    val fieldTypes = TableEnvironment.getFieldTypes(dataType)
    val relDataType = getTypeFactory.buildRelDataType(fieldNames, fieldTypes, fieldNullables)
    val boundedStreamTable = new IntermediateBoundedStreamTable[T](
      relDataType, boundedStream, fieldIndexes, fieldNames)
    registerTableInternal(name, boundedStreamTable, replace)
  }

  /**
   * Registers a [[DataStream]] as a table under a given name with field names as specified by
   * field expressions in the [[TableEnvironment]]'s catalog.
   *
   * @param name     The name under which the table is registered in the catalog.
   * @param boundedStream The [[DataStream]] to register as table in the catalog.
   * @param fields   The field expressions to define the field names of the table.
   * @tparam T The type of the [[DataStream]].
   */
  protected def registerBoundedStreamInternal[T](
      name: String, boundedStream: DataStream[T],
      fields: Array[Expression], replace: Boolean): Unit = {

    if (fields.exists(_.isInstanceOf[TimeAttribute])) {
      throw new ValidationException(
        ".rowtime and .proctime time indicators are not allowed in a batch exec environment.")
    }

    val dataType = boundedStream.getTransformation.getOutputType
    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataType, fields)
    val boundedStreamTable = new DataStreamTable[T](boundedStream, fieldIndexes, fieldNames)
    registerTableInternal(name, boundedStreamTable, replace)
  }

  /**
   * Registers a [[DataStream]] as a table under a given name with field names as specified by
   * field expressions in the [[TableEnvironment]]'s catalog.
   *
   * @param name           The name under which the table is registered in the catalog.
   * @param boundedStream       The [[DataStream]] to register as table in the catalog.
   * @param fields         The field expressions to define the field names of the table.
   * @param fieldNullables The field isNullables attributes of boundedStream.
   * @tparam T The type of the [[DataStream]].
   */
  protected def registerBoundedStreamInternal[T](
      name: String,
      boundedStream: DataStream[T],
      fields: Array[Expression],
      fieldNullables: Array[Boolean],
      replace: Boolean): Unit = {

    if (fields.exists(_.isInstanceOf[TimeAttribute])) {
      throw new ValidationException(
        ".rowtime and .proctime time indicators are not allowed in a batch exec environment.")
    }
    val dataType = boundedStream.getTransformation.getOutputType
    val (fieldNames, fieldIndexes) = getFieldInfo(dataType, fields)
    val physicalFieldTypes = TableEnvironment.getFieldTypes(dataType)
    val fieldTypes = fieldIndexes.map(physicalFieldTypes.apply)
    val relDataType = getTypeFactory.buildRelDataType(fieldNames, fieldTypes, fieldNullables)
    val boundedStreamTable = new IntermediateBoundedStreamTable[T](
      relDataType, boundedStream, fieldIndexes, fieldNames)
    registerTableInternal(name, boundedStreamTable, replace)
  }

  private def translate[A](
      table: Table,
      sink: TableSink[A],
      sinkName: String,
      dagOptimizeEnabled: Boolean = false): StreamTransformation[_] = {
    val sinkNode = SinkNode(table.logicalPlan, sink, sinkName)
    val nodeDag = optimizeAndTranslateNodeDag(dagOptimizeEnabled, sinkNode)
    nodeDag.head match {
      case batchExecSink: BatchExecSink[A] => translate(batchExecSink, sink.getOutputType)
      case _ => throw new TableException(TableErrors.INST.sqlCompileSinkNodeRequired())
    }
  }

  /**
    * Translates a [[ExecNode]] DAG into a [[StreamTransformation]] DAG.
    *
    * @param sinks The node DAG to translate.
    * @return The [[StreamTransformation]] DAG that corresponds to the node DAG.
    */
  override protected def translate(sinks: Seq[ExecNode[_, _]]): Seq[StreamTransformation[_]] = {
    sinks.map {
      case n: BatchExecSink[_] => translate(n, n.sink.getOutputType)
      case _ => throw new TableException(TableErrors.INST.sqlCompileSinkNodeRequired())
    }
  }

  /**
   * Translates a [[BatchExecNode]] plan into a [[StreamTransformation]].
   * Converts to target type if necessary.
   *
   * @param node        The plan to translate.
   * @param resultType  The [[DataType]] of the elements that result from resulting
   *                    [[StreamTransformation]].
   * @return The [[StreamTransformation]] that corresponds to the given node.
   */
  private def translate[OUT](
      node: ExecNode[_, OUT],
      resultType: DataType): StreamTransformation[OUT] = {
    TableEnvironment.validateType(resultType)

    node match {
      case node: BatchExecNode[OUT] => node.translateToPlan(this)
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
   * Generates the optimized [[RelNode]] tree from the original relational node tree.
   *
   * @param relNode The original [[RelNode]] tree
   * @return The optimized [[RelNode]] tree
   */
  private[flink] def optimize(relNode: RelNode): RelNode = {
    val programs = config.getCalciteConfig.getBatchPrograms
      .getOrElse(FlinkBatchPrograms.buildPrograms(config.getConf))
    Preconditions.checkNotNull(programs)

    val optimizedPlan = programs.optimize(relNode, new BatchOptimizeContext {
      override def getContext: Context = getFrameworkConfig.getContext

      override def getRelOptPlanner: RelOptPlanner = getPlanner
    })

    // Rewrite same rel object to different rel objects
    // in order to get the correct dag (dag reuse is based on object not digest)
    optimizedPlan.accept(new SameRelObjectShuttle())
  }

  /**
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param resultType The [[DataType]] of the resulting [[DataStream]].
    * @tparam T The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translateToDataStream[T](
    table: Table,
    resultType: DataType): DataStream[T] = {
    val sink = new DataStreamTableSink[T](table, resultType, false, false)
    val sinkName = createUniqueTableName()
    val sinkNode = LogicalSink.create(table.getRelNode, sink, sinkName)
    val optimizedPlan = optimize(sinkNode)
    val optimizedNodes = translateNodeDag(Seq(optimizedPlan))
    require(optimizedNodes.size() == 1)
    val transformation = translate(optimizedNodes.head, resultType)
    new DataStream(execEnv, transformation).asInstanceOf[DataStream[T]]
  }

  /**
    * Convert [[BatchPhysicalRel]] DAG to [[BatchExecNode]] DAG and translate them.
    */
  @VisibleForTesting
  private[flink] def translateNodeDag(rels: Seq[RelNode]): Seq[BatchExecNode[_]] = {
    require(rels.nonEmpty && rels.forall(_.isInstanceOf[BatchPhysicalRel]))
    // reuse subplan
    val reusedPlan = SubplanReuseUtil.reuseSubplan(rels, config)
    // convert BatchPhysicalRel DAG to BatchExecNode DAG
    val nodeDag = reusedPlan.map(_.asInstanceOf[BatchExecNode[_]])
    // breakup deadlock
    // TODO move DeadlockBreakupProcessor into batch DAGProcessors
    val nodeDagWithoutDeadlock = new DeadlockBreakupProcessor().process(
      nodeDag, new DAGProcessContext(this))
    // build running units
    nodeDagWithoutDeadlock.foreach(n => ruKeeper.buildRUs(n.asInstanceOf[BatchExecNode[_]]))
    // call processors
    val dagProcessors = getConfig.getBatchDAGProcessors
    require(dagProcessors != null)
    val postNodeDag = dagProcessors.process(
      nodeDagWithoutDeadlock, new DAGProcessContext(this, ruKeeper.getRunningUnitMap))

    dumpOptimizedPlanIfNeed(postNodeDag)
    postNodeDag.map(_.asInstanceOf[BatchExecNode[_]])
  }

  /**
    * Optimize the RelNode tree (or DAG), and translate the result to ExecNode tree (or DAG).
    */
  @VisibleForTesting
  private[flink] override def optimizeAndTranslateNodeDag(
      dagOptimizeEnabled: Boolean,
      logicalNodes: LogicalNode*): Seq[ExecNode[_, _]] = {
    if (logicalNodes.isEmpty) {
      throw new TableException(TableErrors.INST.sqlCompileNoSinkTblError())
    }
    val nodeDag = if (dagOptimizeEnabled) {
      val optLogicalNodes = tableServiceManager.cachePlanBuilder.buildPlanIfNeeded(logicalNodes)
      // optimize dag
      val optRelNodes = BatchDAGOptimizer.optimize(optLogicalNodes, this)
      // translate node dag
      translateNodeDag(optRelNodes)
    } else {
      require(logicalNodes.size == 1)
      val sinkTable = new Table(this, logicalNodes.head)
      val originTree = sinkTable.getRelNode
      // optimize tree
      val optimizedTree = optimize(originTree)
      // translate node tree
      translateNodeDag(Seq(optimizedTree))
    }
    require(nodeDag.size() == logicalNodes.size)
    nodeDag
  }

  /**
    * Registers an internal [[BatchTableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    * @param replace     Whether to replace the registered table.
    */
  override protected def registerTableSourceInternal(
    name: String,
    tableSource: TableSource,
    statistic: FlinkStatistic,
    replace: Boolean = false): Unit = {

    tableSource match {

      // check for proper batch table source
      case batchTableSource: BatchTableSource[_] =>
        // check if a table (source or sink) is registered
        getTable(name) match {

          // table source and/or sink is registered
          case Some(table: TableSourceSinkTable[_]) => table.tableSourceTable match {

            // wrapper contains source
            case Some(_: TableSourceTable) if !replace =>
              throw new TableException(s"Table '$name' already exists. " +
                s"Please choose a different name.")

            // wrapper contains only sink (not source)
            case _ =>
              val enrichedTable = new TableSourceSinkTable(
                Some(new BatchTableSourceTable(batchTableSource, statistic)),
                table.tableSinkTable)
              replaceRegisteredTable(name, enrichedTable)
          }

          // no table is registered
          case _ =>
            val newTable = new TableSourceSinkTable(
              Some(new BatchTableSourceTable(batchTableSource, statistic)),
              None)
            registerTableInternal(name, newTable)
        }

      // not a batch table source
      case _ =>
        throw new TableException("Only BatchTableSource can be " +
          "registered in BatchTableEnvironment.")
    }
  }

  /**
    * Creates a table source and/or table sink from a descriptor.
    *
    * Descriptors allow for declaring the communication to external systems in an
    * implementation-agnostic way. The classpath is scanned for suitable table factories that match
    * the desired configuration.
    *
    * The following example shows how to read from a connector using a JSON format and
    * registering a table source as "MyTable":
    *
    * {{{
    *
    * tableEnv
    *   .connect(
    *     new ExternalSystemXYZ()
    *       .version("0.11"))
    *   .withFormat(
    *     new Json()
    *       .jsonSchema("{...}")
    *       .failOnMissingField(false))
    *   .withSchema(
    *     new Schema()
    *       .field("user-name", "VARCHAR").from("u_name")
    *       .field("count", "DECIMAL")
    *   .registerSource("MyTable")
    * }}}
    *
    * @param connectorDescriptor connector descriptor describing the external system
    */
  def connect(connectorDescriptor: ConnectorDescriptor): BatchTableDescriptor = {
    new BatchTableDescriptor(this, connectorDescriptor)
  }

  override def registerTableSinkInternal(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[DataType],
      tableSink: TableSink[_],
      replace: Boolean): Unit = {
    checkValidTableName(name)
    if (fieldNames == null) throw new TableException("fieldNames must not be null.")
    if (fieldTypes == null) throw new TableException("fieldTypes must not be null.")
    if (fieldNames.length == 0) throw new TableException("fieldNames must not be empty.")
    if (fieldNames.length != fieldTypes.length) {
      throw new TableException("Same number of field names and types required.")
    }

    // configure and register
    val configuredSink = tableSink.configure(fieldNames, fieldTypes)
    registerTableSinkInternal(name, configuredSink, replace)
  }

  protected def registerTableSinkInternal(
      name: String,
      configuredSink: TableSink[_],
      replace: Boolean): Unit = {
    // validate
    checkValidTableName(name)
    if (configuredSink.getFieldNames == null || configuredSink.getFieldTypes == null) {
      throw new TableException("Table sink is not configured.")
    }
    if (configuredSink.getFieldNames.length == 0) {
      throw new TableException("Field names must not be empty.")
    }
    if (configuredSink.getFieldNames.length != configuredSink.getFieldTypes.length) {
      throw new TableException("Same number of field names and types required.")
    }

    // register
    configuredSink match {

      // check for proper batch table sink
      case _: BatchTableSink[_] | _: BatchCompatibleStreamTableSink[_] =>

        // check if a table (source or sink) is registered
        getTable(name) match {

          // table source and/or sink is registered
          case Some(table: TableSourceSinkTable[_]) => table.tableSinkTable match {

            // wrapper contains sink
            case Some(_: TableSinkTable[_]) if !replace =>
              throw new TableException(s"Table '$name' already exists. " +
                s"Please choose a different name.")

            // wrapper contains only source (not sink)
            case _ =>
              val enrichedTable = new TableSourceSinkTable(
                table.tableSourceTable,
                Some(new TableSinkTable(configuredSink)))
              replaceRegisteredTable(name, enrichedTable)
          }

          // no table is registered
          case _ =>
            val newTable = new TableSourceSinkTable(
              None,
              Some(new TableSinkTable(configuredSink)))
            registerTableInternal(name, newTable)
        }

      // not a batch table sink
      case _ =>
        throw new TableException("Only BatchTableSink can be registered in BatchTableEnvironment.")
    }
  }

  /**
    * Merge global job parameters and table config parameters,
    * and set the merged result to GlobalJobParameters
    */
  private def mergeParameters(): Unit = {
    if (streamEnv != null && streamEnv.getConfig != null) {
      val parameters = new Configuration()
      if (config != null && config.getConf != null) {
        parameters.addAll(config.getConf)
      }

      if (streamEnv.getConfig.getGlobalJobParameters != null) {
        streamEnv.getConfig.getGlobalJobParameters.toMap.asScala.foreach {
          kv => parameters.setString(kv._1, kv._2)
        }
      }

      streamEnv.getConfig.setGlobalJobParameters(parameters)
    }
  }

  /**
   * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
   * the result of the given [[Table]].
   *
   * @param table    The table for which the AST and execution plan will be returned.
   * @param extended Flag to include detailed optimizer estimates.
   */
  private[flink] def explain(table: Table, extended: Boolean): String = {
    val ast = table.getRelNode
    // explain as simple tree, ignore dag optimization if it's enabled
    val optimizedNode = optimizeAndTranslateNodeDag(false, table.logicalPlan).head

    val fieldTypes = ast.getRowType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toInternalType(field.getType))
    val transformation = translate(optimizedNode, new RowType(fieldTypes: _*))
    val streamGraph = translateStreamGraph(ArrayBuffer(transformation), None)

    val executionPlan = PlanUtil.explainPlan(streamGraph)

    val (explainLevel, withResource, withMemCost) = if (extended) {
      (SqlExplainLevel.ALL_ATTRIBUTES, true, true)
    } else {
      (SqlExplainLevel.EXPPLAN_ATTRIBUTES, false, false)
    }

    s"== Abstract Syntax Tree ==" +
      System.lineSeparator +
      s"${FlinkRelOptUtil.toString(ast)}" +
      System.lineSeparator +
      s"== Optimized Logical Plan ==" +
      System.lineSeparator +
      s"${FlinkNodeOptUtil.treeToString(
        optimizedNode, explainLevel, withResource, withMemCost)}" +
      System.lineSeparator +
      s"== Physical Execution Plan ==" +
      System.lineSeparator +
      s"$executionPlan"
  }

  def explain(table: Table): String = explain(table: Table, extended = false)

  def explain(extended: Boolean = false): String = {
    if (!config.getSubsectionOptimization) {
      throw new TableException("Can not explain due to subsection optimization is not supported, " +
        "please check your TableConfig.")
    }

    val sinkExecNodes = optimizeAndTranslateNodeDag(true, sinkNodes: _*)

    val sb = new StringBuilder
    sb.append("== Abstract Syntax Tree ==")
    sb.append(System.lineSeparator)
    sinkNodes.foreach { sink =>
      val table = new Table(this, sink.children.head)
      val ast = table.getRelNode
      sb.append(FlinkRelOptUtil.toString(ast))
      sb.append(System.lineSeparator)
    }

    sb.append("== Optimized Logical Plan ==")
    sb.append(System.lineSeparator)
    val (explainLevel, withResource, withMemCost) = if (extended) {
      (SqlExplainLevel.ALL_ATTRIBUTES, true, true)
    } else {
      (SqlExplainLevel.EXPPLAN_ATTRIBUTES, false, false)
    }
    sb.append(FlinkNodeOptUtil.dagToString(sinkExecNodes, explainLevel, withResource, withMemCost))

    val sinkTransformations = translate(sinkExecNodes)
    val streamGraph = StreamGraphGenerator.generate(
      StreamGraphGenerator.Context.buildBatchProperties(streamEnv), sinkTransformations)
    val sqlPlan = PlanUtil.explainPlan(streamGraph)
    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(sqlPlan)
    sb.toString()
  }

  /**
   * Dump stream graph plan with accumulated operator metrics if config enabled.
   *
   * @param streamGraph streamGraph
   * @param jobResult   job result of stream graph
   */
  private[this] def dumpPlanWithMetricsIfNeed(
      streamGraph: StreamGraph,
      jobResult: JobExecutionResult): Unit = {
    val dumpFilePath = config.getConf.getString(
      TableConfigOptions.SQL_EXEC_OPERATOR_METRIC_DUMP_PATH)
    if (config.getConf.getBoolean(TableConfigOptions.SQL_EXEC_OPERATOR_METRIC_DUMP_ENABLED)
        && dumpFilePath != null) {
      streamGraph.dumpPlanWithMetrics(dumpFilePath, jobResult)
    }
  }

  /**
    * Dump optimized plan if config enabled.
    *
    * @param optimizedNodes optimized plan
    */
  private[this] def dumpOptimizedPlanIfNeed(optimizedNodes: Seq[ExecNode[_, _]]): Unit = {
    val dumpFilePath = config.getConf.getString(TableConfigOptions.SQL_OPTIMIZER_PLAN_DUMP_PATH)
    val planDump = config.getConf.getBoolean(TableConfigOptions.SQL_OPTIMIZER_PLAN_DUMP_ENABLED)

    if (planDump && dumpFilePath != null) {
      dumpExecNodes(optimizedNodes, dumpFilePath)
    }
  }
}
