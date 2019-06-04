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

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.{StreamGraph, StreamGraphGenerator}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{AccModeTraitDef, FlinkRelDistributionTraitDef, MiniBatchIntervalTraitDef, UpdateAsRetractionTraitDef}
import org.apache.flink.table.plan.nodes.calcite.LogicalSink
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.optimize.{Optimizer, StreamCommonSubGraphBasedOptimizer}
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.util.{ExecNodePlanDumper, FlinkRelOptUtil}
import org.apache.flink.table.sinks.DataStreamTableSink
import org.apache.flink.table.sources.{StreamTableSource, TableSource}
import org.apache.flink.table.types.{DataType, LogicalTypeDataTypeConverter}
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.{TimeIndicatorTypeInfo, TypeCheckUtils}
import org.apache.flink.table.util.PlanUtil

import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.sql.SqlExplainLevel

import _root_.scala.collection.JavaConversions._

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
abstract class StreamTableEnvironment(
    private[flink] val execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(config) {

  // prefix  for unique table names.
  override private[flink] val tableNamePrefix = "_DataStreamTable_"

  // the naming pattern for internally registered tables.
  private val internalNamePattern = "^_DataStreamTable_[0-9]+$".r

  private var isConfigMerged: Boolean = false

  override def queryConfig: StreamQueryConfig = new StreamQueryConfig

  override protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]] = {
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      MiniBatchIntervalTraitDef.INSTANCE,
      UpdateAsRetractionTraitDef.INSTANCE,
      AccModeTraitDef.INSTANCE)
  }

  override protected def getOptimizer: Optimizer = new StreamCommonSubGraphBasedOptimizer(this)

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  override protected def checkValidTableName(name: String): Unit = {
    val m = internalNamePattern.findFirstIn(name)
    m match {
      case Some(_) =>
        throw new TableException(s"Illegal Table name. " +
          s"Please choose a name that does not contain the pattern $internalNamePattern")
      case None =>
    }
  }

  override def execute(jobName: String): JobExecutionResult = {
    generateStreamGraph(jobName)
    // TODO supports execEnv.execute(streamGraph)
    execEnv.execute(jobName)
  }

  protected override def translateStreamGraph(
      streamingTransformations: Seq[StreamTransformation[_]],
      jobName: Option[String] = None): StreamGraph = {
    mergeParameters()

    val streamGraph = StreamGraphGenerator.generate(execEnv, streamingTransformations.toList)
    streamGraph.setJobName(jobName.getOrElse(DEFAULT_JOB_NAME))
    streamGraph
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
        execEnv.getConfig.getGlobalJobParameters.toMap.foreach {
          kv => parameters.setString(kv._1, kv._2)
        }
      }
      val isHeapState = Option(execEnv.getStateBackend) match {
        case Some(backend) if backend.isInstanceOf[MemoryStateBackend] ||
          backend.isInstanceOf[FsStateBackend]=> true
        case None => true
        case _ => false
      }
      parameters.setBoolean(TableConfigOptions.SQL_EXEC_STATE_BACKEND_ON_HEAP, isHeapState)
      execEnv.getConfig.setGlobalJobParameters(parameters)
      isConfigMerged = true
    }
  }

  /**
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table               The root node of the relational expression tree.
    * @param updatesAsRetraction Set to true to encode updates as retraction messages.
    * @param withChangeFlag      Set to true to emit records with change flags.
    * @param resultType          The [[org.apache.flink.api.common.typeinfo.TypeInformation[_]] of
    *                            the resulting [[DataStream]].
    * @tparam T The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translateToDataStream[T](
      table: Table,
      updatesAsRetraction: Boolean,
      withChangeFlag: Boolean,
      resultType: TypeInformation[T]): DataStream[T] = {
    val sink = new DataStreamTableSink[T](table, resultType, updatesAsRetraction, withChangeFlag)
    val sinkName = createUniqueTableName()
    val sinkNode = LogicalSink.create(table.asInstanceOf[TableImpl].getRelNode, sink, sinkName)
    val transformation = translateSink(sinkNode)
    new DataStream(execEnv, transformation).asInstanceOf[DataStream[T]]
  }

  private def translateSink(sink: LogicalSink): StreamTransformation[_] = {
    mergeParameters()

    val optimizedPlan = optimize(sink)
    val optimizedNodes = translateToExecNodeDag(Seq(optimizedPlan))
    require(optimizedNodes.size() == 1)
    translateToPlan(optimizedNodes.head)
  }

  override protected def translateToPlan(
      sinks: Seq[ExecNode[_, _]]): Seq[StreamTransformation[_]] = sinks.map(translateToPlan)

  /**
    * Translates a [[StreamExecNode]] plan into a [[StreamTransformation]].
    *
    * @param node The plan to translate.
    * @return The [[StreamTransformation]] of type [[BaseRow]].
    */
  private def translateToPlan(node: ExecNode[_, _]): StreamTransformation[_] = {
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
  def explain(table: Table): String = explain(table, extended = false)

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table    The table for which the AST and execution plan will be returned.
    * @param extended Flag to include detailed optimizer estimates.
    */
  def explain(table: Table, extended: Boolean): String = {
    val ast = table.asInstanceOf[TableImpl].getRelNode
    val execNodeDag = compileToExecNodePlan(ast)
    val transformations = translateToPlan(execNodeDag)
    val streamGraph = translateStreamGraph(transformations)
    val executionPlan = PlanUtil.explainStreamGraph(streamGraph)

    val (explainLevel, withRetractTraits) = if (extended) {
      (SqlExplainLevel.ALL_ATTRIBUTES, true)
    } else {
      (SqlExplainLevel.EXPPLAN_ATTRIBUTES, false)
    }

    s"== Abstract Syntax Tree ==" +
      System.lineSeparator +
      s"${FlinkRelOptUtil.toString(ast)}" +
      System.lineSeparator +
      s"== Optimized Logical Plan ==" +
      System.lineSeparator +
      s"${
        ExecNodePlanDumper.dagToString(
          execNodeDag,
          explainLevel,
          withRetractTraits = withRetractTraits)
      }" +
      System.lineSeparator +
      s"== Physical Execution Plan ==" +
      System.lineSeparator +
      s"$executionPlan"
  }

  /**
    * Explain the whole plan, and returns the AST(s) of the specified Table API and SQL queries
    * and the execution plan.
    */
  def explain(): String = explain(extended = false)

  /**
    * Explain the whole plan, and returns the AST(s) of the specified Table API and SQL queries
    * and the execution plan.
    *
    * @param extended Flag to include detailed optimizer estimates.
    */
  def explain(extended: Boolean): String = {
    val sinkExecNodes = compileToExecNodePlan(sinkNodes: _*)
    // translate relNodes to StreamTransformations
    val sinkTransformations = translateToPlan(sinkExecNodes)
    val streamGraph = translateStreamGraph(sinkTransformations)
    val sqlPlan = PlanUtil.explainStreamGraph(streamGraph)

    val sb = new StringBuilder
    sb.append("== Abstract Syntax Tree ==")
    sb.append(System.lineSeparator)
    sinkNodes.foreach { sink =>
      sb.append(FlinkRelOptUtil.toString(sink))
      sb.append(System.lineSeparator)
    }

    sb.append("== Optimized Logical Plan ==")
    sb.append(System.lineSeparator)
    val (explainLevel, withRetractTraits) = if (extended) {
      (SqlExplainLevel.ALL_ATTRIBUTES, true)
    } else {
      (SqlExplainLevel.EXPPLAN_ATTRIBUTES, false)
    }
    sb.append(ExecNodePlanDumper.dagToString(
      sinkExecNodes,
      explainLevel,
      withRetractTraits = withRetractTraits))
    sb.append(System.lineSeparator)

    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(sqlPlan)
    sb.toString()
  }

  /**
    * Registers a [[DataStream]] as a table under a given name in the [[TableEnvironment]]'s
    * catalog.
    *
    * @param name The name under which the table is registered in the catalog.
    * @param dataStream The [[DataStream]] to register as table in the catalog.
    * @tparam T the type of the [[DataStream]].
    */
  protected def registerDataStreamInternal[T](
    name: String,
    dataStream: DataStream[T]): Unit = {

    val (fieldNames, fieldIndexes) = getFieldInfo[T](fromLegacyInfoToDataType(dataStream.getType))
    val dataStreamTable = new DataStreamTable[T](
      dataStream,
      fieldIndexes,
      fieldNames
    )
    registerTableInternal(name, dataStreamTable)
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
      fields: Array[String]): Unit = {

    // get field names and types for all non-replaced fields
    val (fieldNames, fieldIndexes) = getFieldInfo(
      fromLegacyInfoToDataType(dataStream.getType), fields)

    // TODO: validate and extract time attributes after we introduce [Expression],
    //  return None currently
    val (rowtime, proctime) = validateAndExtractTimeAttributes(
      fromLegacyInfoToDataType(dataStream.getType), fields)

    // check if event-time is enabled
    if (rowtime.isDefined && execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime) {
      throw new TableException(
        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
          s"But is: ${execEnv.getStreamTimeCharacteristic}")
    }

    // adjust field indexes and field names
    val indexesWithIndicatorFields = adjustFieldIndexes(fieldIndexes, rowtime, proctime)
    val namesWithIndicatorFields = adjustFieldNames(fieldNames, rowtime, proctime)

    val dataStreamTable = new DataStreamTable[T](
      dataStream,
      indexesWithIndicatorFields,
      namesWithIndicatorFields
    )
    registerTableInternal(name, dataStreamTable)
  }

  /**
    * Checks for at most one rowtime and proctime attribute.
    * Returns the time attributes.
    *
    * @return rowtime attribute and proctime attribute
    */
  // TODO: we should support Expression fields after we introduce [Expression]
  private[flink] def validateAndExtractTimeAttributes(
      streamType: DataType,
      fields: Array[String]): (Option[(Int, String)], Option[(Int, String)]) = {

    val streamLogicalType = LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(streamType)

    val (isRefByPos, fieldTypes) = streamLogicalType match {
      case c: RowType =>
        // determine schema definition mode (by position or by name)
        (isReferenceByPosition(c, fields),
            (0 until c.getFieldCount).map(i => c.getTypeAt(i)).toArray)
      case t =>
        (false, Array(t))
    }

    var fieldNames: List[String] = Nil
    var rowtime: Option[(Int, String)] = None
    var proctime: Option[(Int, String)] = None

    def checkRowtimeType(t: LogicalType): Unit = {
      if (!(TypeCheckUtils.isLong(t) || TypeCheckUtils.isTimePoint(t))) {
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
            checkRowtimeType(fieldTypes(idx))
          }
        }
        // check reference-by-name
        else {
          val aliasOrName = origName.getOrElse(name)
          streamLogicalType match {
            // both alias and reference must have a valid type if they replace a field
            case ct: RowType if ct.getFieldIndex(aliasOrName) != -1 =>
              val t = ct.getTypeAt(ct.getFieldIndex(aliasOrName))
              checkRowtimeType(t)
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
          streamLogicalType match {
            // proctime attribute must not replace a field
            case ct: RowType if ct.getFieldIndex(name) != -1 =>
              throw new TableException(
                s"The proctime attribute '$name' must not replace an existing field.")
            case _ => // ok
          }
        }
        proctime = Some(idx, name)
      }
    }

    fields.zipWithIndex.foreach {
      case ("rowtime", idx) =>
        extractRowtime(idx, "rowtime", None)

      case ("proctime", idx) =>
        extractProctime(idx, "proctime")

      case (name, _) => fieldNames = name :: fieldNames
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
    * Registers an internal [[StreamTableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  override protected def registerTableSourceInternal(
      name: String,
      tableSource: TableSource[_],
      statistic: FlinkStatistic,
      replace: Boolean = false): Unit = {

    // TODO `TableSourceUtil.hasRowtimeAttribute` depends on [Expression]
    // check that event-time is enabled if table source includes rowtime attributes
    //tableSource match {
    //  case tableSource: TableSource[_] if TableSourceUtil.hasRowtimeAttribute(tableSource) &&
    //    execEnv.getStreamTimeCharacteristic != TimeCharacteristic.EventTime =>
    //
    //    throw new TableException(
    //      s"A rowtime attribute requires an EventTime time characteristic in stream environment
    //      . " +
    //        s"But is: ${execEnv.getStreamTimeCharacteristic}")
    //  case _ => // ok
    //}

    tableSource match {

      // check for proper stream table source
      case streamTableSource: StreamTableSource[_] =>
        // register
        getTable(name) match {

          // check if a table (source or sink) is registered
          case Some(table: TableSourceSinkTable[_, _]) => table.tableSourceTable match {

            // wrapper contains source
            case Some(_: TableSourceTable[_]) if !replace =>
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
    * Injects markers for time indicator fields into the field indexes.
    *
    * @param fieldIndexes The field indexes into which the time indicators markers are injected.
    * @param rowtime An optional rowtime indicator
    * @param proctime An optional proctime indicator
    * @return An adjusted array of field indexes.
    */
  private def adjustFieldIndexes(
    fieldIndexes: Array[Int],
    rowtime: Option[(Int, String)],
    proctime: Option[(Int, String)]): Array[Int] = {

    // inject rowtime field
    val withRowtime = rowtime match {
      case Some(rt) =>
        fieldIndexes.patch(rt._1, Seq(TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER), 0)
      case _ =>
        fieldIndexes
    }

    // inject proctime field
    val withProctime = proctime match {
      case Some(pt) =>
        withRowtime.patch(pt._1, Seq(TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER), 0)
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
  private def adjustFieldNames(
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

}

