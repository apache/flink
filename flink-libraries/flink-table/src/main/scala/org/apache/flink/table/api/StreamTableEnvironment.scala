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
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.{StreamGraph, StreamGraphGenerator}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType, RowType}
import org.apache.flink.table.calcite.{FlinkChainContext, FlinkRelBuilder}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.descriptors.{ConnectorDescriptor, StreamTableDescriptor}
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.`trait`._
import org.apache.flink.table.plan.cost.{FlinkCostFactory, FlinkStreamCost}
import org.apache.flink.table.plan.logical.{LogicalNode, LogicalRelNode, SinkNode}
import org.apache.flink.table.plan.nodes.calcite._
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.nodes.process.DAGProcessContext
import org.apache.flink.table.plan.optimize.{FlinkStreamPrograms, StreamOptimizeContext}
import org.apache.flink.table.plan.schema.{TableSourceSinkTable, _}
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.subplan.StreamDAGOptimizer
import org.apache.flink.table.plan.util.{FlinkNodeOptUtil, FlinkRelOptUtil, SameRelObjectShuttle}
import org.apache.flink.table.sinks.{DataStreamTableSink, _}
import org.apache.flink.table.sources._
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.table.util._
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan._
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql2rel.SqlToRelConverter

import _root_.java.util

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable.ArrayBuffer

/**
  * The base class for stream TableEnvironments.
  *
  * A TableEnvironment can be used to:
  * - convert [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] as a table in the catalog
  * - register a [[Table]] in the catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  *
  * @param execEnv The [[StreamExecutionEnvironment]] which is wrapped in this
  *                [[StreamTableEnvironment]].
  * @param config The [[TableConfig]] of this [[StreamTableEnvironment]].
  */
@InterfaceStability.Evolving
abstract class StreamTableEnvironment(
    execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(execEnv, config) {

  // prefix  for unique table names.
  override private[flink] val tableNamePrefix = "_DataStreamTable_"

  private var isConfigMerged: Boolean = false

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  override protected def createRelBuilder: FlinkRelBuilder = FlinkRelBuilder.create(
    frameworkConfig,
    config,
    getTypeFactory,
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE,
      UpdateAsRetractionTraitDef.INSTANCE,
      AccModeTraitDef.INSTANCE),
    catalogManager
  )

  /**
    * `inSubQueryThreshold` is set to Integer.MAX_VALUE which forces usage of OR
    * for `in` or `not in` clause.
    */
  override protected def getSqlToRelConverterConfig: SqlToRelConverter.Config =
    SqlToRelConverter.configBuilder()
      .withTrimUnusedFields(false)
      .withConvertTableAccess(false)
      .withInSubQueryThreshold(Integer.MAX_VALUE)
      .withExpand(false)
      .build()

  /**
    * Triggers the program execution with jobName.
    * @param jobName The job name.
    */
  override def execute(jobName: String): JobExecutionResult = {
    val streamGraph = generateStreamGraph(jobName)
    execEnv.execute(streamGraph)
  }

  protected override def translateStreamGraph(
      streamingTransformations: ArrayBuffer[StreamTransformation[_]],
      jobName: Option[String]): StreamGraph = {
    mergeParameters()
    val context = StreamGraphGenerator.Context.buildStreamProperties(execEnv)
    context.setSlotSharingEnabled(false)

    jobName match {
      case Some(jn) => context.setJobName(jn)
      case None => context.setJobName(DEFAULT_JOB_NAME)
    }
    val streamGraph = StreamGraphGenerator.generate(context, streamingTransformations)

    streamingTransformations.clear()
    streamGraph
  }

  protected override def compile(): Unit = {
    mergeParameters()
    super.compile()
  }

  /**
    * Merge global job parameters and table config parameters,
    * and set the merged result to GlobalJobParameters
    */
  private def mergeParameters(): Unit = {
    if (!isConfigMerged && execEnv != null && execEnv.getConfig != null) {
      val parameters = new Configuration()
      if (config != null && config.getConf != null) {
        parameters.addAll(config.getConf)
      }

      if (execEnv.getConfig.getGlobalJobParameters != null) {
        execEnv.getConfig.getGlobalJobParameters.toMap.asScala.foreach {
          kv => parameters.setString(kv._1, kv._2)
        }
      }
      parameters.setBoolean(
        StateUtil.STATE_BACKEND_ON_HEAP,
        StateUtil.isHeapState(execEnv.getStateBackend))
      execEnv.getConfig.setGlobalJobParameters(parameters)
      isConfigMerged = true
    }
  }

  /**
    * Registers an internal [[StreamTableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  override protected def registerTableSourceInternal(
    name: String,
    tableSource: TableSource,
    statistic: FlinkStatistic,
    replace: Boolean = false)
  : Unit = {

    // check that event-time is enabled if table source includes rowtime attributes
    tableSource match {
      case tableSource: TableSource if TableSourceUtil.hasRowtimeAttribute(tableSource) &&
        execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime =>

        throw new TableException(
          s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
            s"But is: ${execEnv.getStreamTimeCharacteristic}")
      case _ => // ok
    }

    tableSource match {

      // check for proper stream table source
      case streamTableSource: StreamTableSource[_] =>
        // register
        getTable(name) match {

          // check if a table (source or sink) is registered
          case Some(table: TableSourceSinkTable[_]) => table.tableSourceTable match {

            // wrapper contains source
            case Some(_: TableSourceTable) if !replace =>
              throw new TableException(s"Table '$name' already exists. " +
                s"Please choose a different name.")

            // wrapper contains only sink (not source)
            case Some(_: StreamTableSourceTable[_]) =>
              val enrichedTable = new TableSourceSinkTable(
                Some(new StreamTableSourceTable(streamTableSource)),
                table.tableSinkTable)
              replaceRegisteredTable(name, enrichedTable)
          }

          // no table is registered
          case _ =>
            val newTable = new TableSourceSinkTable(
              Some(new StreamTableSourceTable(streamTableSource)),
              None)
            registerTableInternal(name, newTable)
        }

      // not a stream table source
      case _ =>
        throw new TableException(
          "Only StreamTableSource can be registered in StreamTableEnvironment")
    }
  }

  /**
    * Creates a table source and/or table sink from a descriptor.
    *
    * Descriptors allow for declaring the communication to external systems in an
    * implementation-agnostic way. The classpath is scanned for suitable table factories that match
    * the desired configuration.
    *
    * The following example shows how to read from a Kafka connector using a JSON format and
    * registering a table source "MyTable" in append mode:
    *
    * {{{
    *
    * tableEnv
    *   .connect(
    *     new Kafka()
    *       .version("0.11")
    *       .topic("clicks")
    *       .property("zookeeper.connect", "localhost")
    *       .property("group.id", "click-group")
    *       .startFromEarliest())
    *   .withFormat(
    *     new Json()
    *       .jsonSchema("{...}")
    *       .failOnMissingField(false))
    *   .withSchema(
    *     new Schema()
    *       .field("user-name", "VARCHAR").from("u_name")
    *       .field("count", "DECIMAL")
    *       .field("proc-time", "TIMESTAMP").proctime())
    *   .inAppendMode()
    *   .registerSource("MyTable")
    * }}}
    *
    * @param connectorDescriptor connector descriptor describing the external system
    */
  def connect(connectorDescriptor: ConnectorDescriptor): StreamTableDescriptor = {
    new StreamTableDescriptor(this, connectorDescriptor)
  }

  /**
    * Registers an external [[TableSink]] with given field names and types in this
    * [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * Example:
    *
    * {{{
    *   // create a table sink and its field names and types
    *   val fieldNames: Array[String] = Array("a", "b", "c")
    *   val fieldTypes: Array[InternalType] = Array(STRING, INT, LONG)
    *   val tableSink: StreamTableSink = new YourTableSinkImpl(...)
    *
    *   // register the table sink in the catalog
    *   tableEnv.registerTableSink("output_table", fieldNames, fieldsTypes, tableSink)
    *
    *   // use the registered sink
    *   tableEnv.sqlUpdate("INSERT INTO output_table SELECT a, b, c FROM sourceTable")
    * }}}
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param fieldNames The field names to register with the [[TableSink]].
    * @param fieldTypes The field types to register with the [[TableSink]].
    * @param tableSink The [[TableSink]] to register.
    * @param replace  Whether to replace the registered table.
    */
  protected def registerTableSinkInternal(
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

    val configuredSink = tableSink.configure(fieldNames, fieldTypes)
    registerTableSinkInternal(name, configuredSink, replace)
  }

  protected def registerTableSinkInternal(name: String,
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

      // check for proper stream table sink
      case _: StreamTableSink[_] =>

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

      // not a stream table sink
      case _ =>
        throw new TableException(
          "Only AppendStreamTableSink, UpsertStreamTableSink, and RetractStreamTableSink can be " +
            "registered in StreamTableEnvironment.")
    }
  }

  /**
    * Returns specific FlinkCostFactory of this Environment.
    * Currently we use DataSetCostFactory, and will change this later.
    */
  override protected def getFlinkCostFactory: FlinkCostFactory = FlinkStreamCost.FACTORY

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataStream]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @tparam T The expected type of the [[DataStream]] which represents the [[Table]].
    */
  override private[table] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      sinkName: String): Unit = {
    mergeParameters()

    val sinkNode = SinkNode(table.logicalPlan, sink, sinkName)
    if (config.getSubsectionOptimization) {
      sinkNodes += sinkNode
    } else {
      val optimizedNode = optimizeAndTranslateNodeDag(false, sinkNode).head
      transformations.add(translate(optimizedNode))
    }
  }

  protected def registerDataStreamInternal[T](
    name: String,
    dataStream: DataStream[T],
    replace: Boolean): Unit = {

    // get field names and types for all non-replaced fields
    val (fieldNames, fieldIndexes) = getFieldInfo(dataStream.getType)
    val dataStreamTable = new DataStreamTable[T](
      dataStream,
      fieldIndexes,
      fieldNames
    )
    registerTableInternal(name, dataStreamTable, replace)
  }

  /**
    * Registers a [[DataStream]] as a table under a given name with field names as specified by
    * field expressions in the [[TableEnvironment]]'s catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataStream The [[DataStream]] to register as table in the catalog.
    * @param fields The field expressions to define the field names of the table.
    * @tparam T The type of the [[DataStream]].
    */
  protected def registerDataStreamInternal[T](
      name: String,
      dataStream: DataStream[T],
      fields: Array[Expression],
      replace: Boolean)
    : Unit = {

    if (fields.exists(_.isInstanceOf[RowtimeAttribute])
        && execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw new TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
            s"But is: ${execEnv.getStreamTimeCharacteristic}")
    }


    val (fieldNames, fieldIndexes) = getFieldInfo(dataStream.getType, fields)

    // validate and extract time attributes
    val (rowtime, proctime) = validateAndExtractTimeAttributes(dataStream.getType, fields)

    // check if event-time is enabled
    if (rowtime.isDefined && execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw new TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
            s"But is: ${execEnv.getStreamTimeCharacteristic}")
    }

    // adjust field indexes and field names
    val indexesWithIndicatorFields = adjustFieldIndexes(fieldIndexes, rowtime, proctime)
    val namesWithIndicatorFields = adjustFieldNames(fieldNames, rowtime, proctime)


    val dataStreamTable = new DataStreamTable(
      dataStream,
      false,
      false,
      indexesWithIndicatorFields,
      namesWithIndicatorFields)
    registerTableInternal(name, dataStreamTable, replace)
  }

  /**
    * Checks for at most one rowtime and proctime attribute.
    * Returns the time attributes.
    *
    * @return rowtime attribute and proctime attribute
    */
  private[flink] def validateAndExtractTimeAttributes(
    streamType: DataType,
    exprs: Array[Expression])
  : (Option[(Int, String)], Option[(Int, String)]) = {

    val (isRefByPos, fieldTypes) = streamType.toInternalType match {
      case c: RowType =>
        // determine schema definition mode (by position or by name)
        (isReferenceByPosition(c, exprs),
            (0 until c.getArity).map(i => c.getInternalTypeAt(i)).toArray)
      case t: InternalType =>
        (false, Array(t))
    }

    var fieldNames: List[String] = Nil
    var rowtime: Option[(Int, String)] = None
    var proctime: Option[(Int, String)] = None

    def checkRowtimeType(t: InternalType): Unit = {
      if (!(t.equals(DataTypes.LONG) || TypeCheckUtils.isTimePoint(t))) {
        throw new TableException(
          s"The rowtime attribute can only replace a field with a valid time type, " +
          s"such as Timestamp or Long. But was: $t")
      }
    }

    def extractRowtime(idx: Int, name: String, origName: Option[String]): Unit = {
      if (rowtime.isDefined) {
        throw new TableException(
          "The rowtime attribute can only be defined once in a table schema.")
      } else {
        // if the fields are referenced by position,
        // it is possible to replace an existing field or append the time attribute at the end
        if (isRefByPos) {
          // aliases are not permitted
          if (origName.isDefined) {
            throw new TableException(
              s"Invalid alias '${origName.get}' because fields are referenced by position.")
          }
          // check type of field that is replaced
          if (idx < fieldTypes.length) {
            checkRowtimeType(fieldTypes(idx).toInternalType)
          }
        }
        // check reference-by-name
        else {
          val aliasOrName = origName.getOrElse(name)
          streamType.toInternalType match {
            // both alias and reference must have a valid type if they replace a field
            case ct: RowType if ct.getFieldIndex(aliasOrName) >= 0 =>
              val t = ct.getInternalTypeAt(ct.getFieldIndex(aliasOrName))
              checkRowtimeType(t.toInternalType)
            // alias could not be found
            case _ if origName.isDefined =>
              throw new TableException(s"Alias '${origName.get}' must reference an existing field.")
            case _ => // ok
          }
        }

        rowtime = Some(idx, name)
      }
    }

    def extractProctime(idx: Int, name: String): Unit = {
      if (proctime.isDefined) {
          throw new TableException(
            "The proctime attribute can only be defined once in a table schema.")
      } else {
        // if the fields are referenced by position,
        // it is only possible to append the time attribute at the end
        if (isRefByPos) {

          // check that proctime is only appended
          if (idx < fieldTypes.length) {
            throw new TableException(
              "The proctime attribute can only be appended to the table schema and not replace " +
                s"an existing field. Please move '$name' to the end of the schema.")
          }
        }
        // check reference-by-name
        else {
          streamType.toInternalType match {
            case ct: RowType if
            ct.getFieldIndex(name) < 0 =>
            case ct: RowType if
              ct.getInternalTypeAt(ct.getFieldIndex(name)).equals(DataTypes.PROCTIME_INDICATOR) =>
            // proctime attribute must not replace a field
            case ct: RowType if ct.getFieldIndex(name) >= 0 =>
              throw new TableException(
                s"The proctime attribute '$name' must not replace an existing field.")
            case _ => // ok
          }
        }
        proctime = Some(idx, name)
      }
    }

    exprs.zipWithIndex.foreach {
      case (RowtimeAttribute(UnresolvedFieldReference(name)), idx) =>
        extractRowtime(idx, name, None)

      case (RowtimeAttribute(Alias(UnresolvedFieldReference(origName), name, _)), idx) =>
        extractRowtime(idx, name, Some(origName))

      case (ProctimeAttribute(UnresolvedFieldReference(name)), idx) =>
        extractProctime(idx, name)

      case (UnresolvedFieldReference(name), _) => fieldNames = name :: fieldNames

      case (Alias(UnresolvedFieldReference(_), name, _), _) => fieldNames = name :: fieldNames

      case (e, _) =>
        throw new TableException(s"Time attributes can only be defined on field references or " +
          s"aliases of valid field references. Rowtime attributes can replace existing fields, " +
          s"proctime attributes can not. " +
          s"But was: $e")
    }

    if (rowtime.isDefined && fieldNames.contains(rowtime.get._2)) {
      throw new TableException(
        "The rowtime attribute may not have the same name as an another field.")
    }

    if (proctime.isDefined && fieldNames.contains(proctime.get._2)) {
      throw new TableException(
        "The proctime attribute may not have the same name as an another field.")
    }

    (rowtime, proctime)
  }

  /**
   * Injects markers for time indicator fields into the field indexes.
   *
   * @param fieldIndexes The field indexes into which the time indicators markers are injected.
   * @param rowtime An optional rowtime indicator
   * @param proctime An optional proctime indicator
   * @return An adjusted array of field indexes.
   */
  private[flink] def adjustFieldIndexes(
      fieldIndexes: Array[Int],
      rowtime: Option[(Int, String)],
      proctime: Option[(Int, String)]): Array[Int] = {

    // inject rowtime field
    val withRowtime = rowtime match {
      case Some(rt) =>
        fieldIndexes.patch(rt._1, Seq(DataTypes.ROWTIME_STREAM_MARKER), 0)
      case _ =>
        fieldIndexes
    }

    // inject proctime field
    val withProctime = proctime match {
      case Some(pt) =>
        withRowtime.patch(pt._1, Seq(DataTypes.PROCTIME_STREAM_MARKER), 0)
      case _ =>
        withRowtime
    }

    withProctime
  }

  /**
   * Injects names of time indicator fields into the list of field names.
   *
   * @param fieldNames The array of field names into which the time indicator field names are
   *                   injected.
   * @param rowtime An optional rowtime indicator
   * @param proctime An optional proctime indicator
   * @return An adjusted array of field names.
   */
  private[flink] def adjustFieldNames(
      fieldNames: Array[String],
      rowtime: Option[(Int, String)],
      proctime: Option[(Int, String)]): Array[String] = {

    // inject rowtime field
    val withRowtime = rowtime match {
      case Some(rt) => fieldNames.patch(rt._1, Seq(rowtime.get._2), 0)
      case _ => fieldNames
    }

    // inject proctime field
    val withProctime = proctime match {
      case Some(pt) => withRowtime.patch(pt._1, Seq(proctime.get._2), 0)
      case _ => withRowtime
    }

    withProctime
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The root node of the relational expression tree.
    * @param updatesAsRetraction True if request updates as retraction messages.
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(
      relNode: RelNode,
      updatesAsRetraction: Boolean = false,
      isSinkBlock: Boolean = true): RelNode = {

    val programs = config.getCalciteConfig.getStreamPrograms
      .getOrElse(FlinkStreamPrograms.buildPrograms(config.getConf))
    Preconditions.checkNotNull(programs)

    val flinkChainContext = getPlanner.getContext.asInstanceOf[FlinkChainContext]

    val optimizeNode = programs.optimize(relNode, new StreamOptimizeContext() {
      override def getContext: Context = flinkChainContext

      override def getRelOptPlanner: RelOptPlanner = getPlanner

      override def getRexBuilder: RexBuilder = getRelBuilder.getRexBuilder

      override def updateAsRetraction(): Boolean = updatesAsRetraction

      override def isSinkNode: Boolean = isSinkBlock
    })

    // Rewrite same rel object to different rel objects
    // in order to get the correct dag (dag reuse is based on object not digest)
    optimizeNode.accept(new SameRelObjectShuttle())
  }

  /**
    * Convert [[StreamPhysicalRel]] DAG to [[StreamExecNode]] DAG and translate them.
    */
  private[flink] def translateNodeDag(rels: Seq[RelNode]): Seq[StreamExecNode[_]] = {
    require(rels.nonEmpty && rels.forall(_.isInstanceOf[StreamExecNode[_]]))
    // convert StreamPhysicalRel DAG to StreamExecNode DAG
    val nodeDag = rels.map(_.asInstanceOf[StreamExecNode[_]])
    // call processors
    val dagProcessors = getConfig.getStreamDAGProcessors
    require(dagProcessors != null)
    val postNodeDag = dagProcessors.process(nodeDag,  new DAGProcessContext(this))

    postNodeDag.map(_.asInstanceOf[StreamExecNode[_]])
  }

  /**
    * Optimize the RelNode tree (or DAG), and translate the result to ExecNode tree (or DAG).
    */
  private[flink] override def optimizeAndTranslateNodeDag(
      dagOptimizeEnabled: Boolean,
      logicalNodes: LogicalNode*): Seq[ExecNode[_, _]] = {
    if (logicalNodes.isEmpty) {
      throw new TableException(TableErrors.INST.sqlCompileNoSinkTblError())
    }
    val nodeDag = if (dagOptimizeEnabled) {
      // optimize dag
      val optRelNodes = StreamDAGOptimizer.optimize(logicalNodes, this)
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
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param updatesAsRetraction Set to true to encode updates as retraction messages.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param resultType The [[DataType]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translateToDataStream[A](
      table: Table,
      updatesAsRetraction: Boolean,
      withChangeFlag: Boolean,
      resultType: DataType): DataStream[A] = {

    mergeParameters()
    val sink = new DataStreamTableSink[A](table, resultType, updatesAsRetraction, withChangeFlag)
    val sinkName = createUniqueTableName()
    val sinkNode = LogicalSink.create(table.getRelNode, sink, sinkName)
    val optimizedPlan = optimize(sinkNode)
    val optimizedNodes = translateNodeDag(Seq(optimizedPlan))
    require(optimizedNodes.size() == 1)
    val transformation = translate(optimizedNodes.head)
    new DataStream(execEnv, transformation).asInstanceOf[DataStream[A]]
  }

  /**
    * Translates a [[ExecNode]] DAG into a [[StreamTransformation]] DAG.
    *
    * @param sinks The node DAG to translate.
    * @return The [[StreamTransformation]] DAG that corresponds to the node DAG.
    */
  protected override def translate(sinks: Seq[ExecNode[_, _]]): Seq[StreamTransformation[_]] = {
    // translates ExecNodes into transformations
    sinks.map {
      case sink: StreamExecSink[_] => translate(sink)
      case _ => throw new TableException(TableErrors.INST.sqlCompileSinkNodeRequired())
    }
  }

  /**
    * Translates a [[StreamExecNode]] plan into a [[StreamTransformation]].
    *
    * @param node The plan to translate.
    * @return The [[StreamTransformation]] of type [[BaseRow]].
    */
  private def translate(node: ExecNode[_, _]): StreamTransformation[_] = {
    node match {
      case node: StreamExecNode[_] => node.translateToPlan(this)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String = {
    val ast = table.getRelNode
    // explain as simple tree, ignore dag optimization if it's enabled
    val optimizedNode = optimizeAndTranslateNodeDag(false, table.logicalPlan).head
    val transformStream = translate(optimizedNode)
    val streamGraph = translateStreamGraph(ArrayBuffer(transformStream), None)
    val executionPlan = PlanUtil.explainPlan(streamGraph)

    s"== Abstract Syntax Tree ==" +
      System.lineSeparator +
      s"${FlinkRelOptUtil.toString(ast)}" +
      System.lineSeparator +
      s"== Optimized Logical Plan ==" +
      System.lineSeparator +
      s"${FlinkNodeOptUtil.treeToString(optimizedNode)}" +
      System.lineSeparator +
      s"== Physical Execution Plan ==" +
      System.lineSeparator +
      s"$executionPlan"
  }

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
    val explainLevel = if (extended) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.EXPPLAN_ATTRIBUTES
    }
    // TODO print resource
    sb.append(FlinkNodeOptUtil.dagToString(sinkExecNodes, explainLevel, withRetractTraits = true))
    sb.append(System.lineSeparator)

    // translate relNodes to StreamTransformations
    val sinkTransformations = translate(sinkExecNodes)
    val sqlPlan = PlanUtil.explainPlan(StreamGraphGenerator.generate(
      StreamGraphGenerator.Context.buildStreamProperties(execEnv), sinkTransformations))
    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(sqlPlan)
    sb.toString()
  }

  /**
    * Register a table with specific row time field and offset.
    * @param tableName table name
    * @param sourceTable table to register
    * @param rowtimeField row time field
    * @param offset offset to the row time field value
    */
  @VisibleForTesting
  def registerTableWithWatermark(
      tableName: String,
      sourceTable: Table,
      rowtimeField: String,
      offset: Long): Unit = {

    val source = sourceTable.getRelNode
    registerTable(
      tableName,
      new Table(
        this,
        new LogicalRelNode(
          new LogicalWatermarkAssigner(
            source.getCluster,
            source.getTraitSet,
            source,
            rowtimeField,
            offset
          )
        )
      )
    )
  }

  /**
    * Register a table with specific list of primary keys.
    * @param tableName table name
    * @param sourceTable table to register
    * @param primaryKeys table primary field name list
    */
  @VisibleForTesting
  def registerTableWithPk(
    tableName: String,
    sourceTable: Table,
    primaryKeys: util.List[String]): Unit = {

    val source = sourceTable.getRelNode
    registerTable(tableName, new Table(this,
      new LogicalRelNode(
      new LogicalLastRow(
        source.getCluster,
        source.getTraitSet,
        source,
        primaryKeys
      ))))
  }
}
