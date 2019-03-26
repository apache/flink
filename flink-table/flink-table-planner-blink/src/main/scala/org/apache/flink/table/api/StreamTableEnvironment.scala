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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.calcite.FlinkRelBuilder
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.calcite.LogicalSink
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.optimize.{Optimizer, StreamOptimizer}
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.`trait`.{AccModeTraitDef, FlinkRelDistributionTraitDef, MiniBatchIntervalTraitDef, UpdateAsRetractionTraitDef}
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.sinks.{DataStreamTableSink, TableSink}
import org.apache.flink.table.sources.{StreamTableSource, TableSource}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.rel.RelCollationTraitDef
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

  override protected def getOptimizer: Optimizer = new StreamOptimizer(this)

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

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  override protected def createRelBuilder: FlinkRelBuilder = FlinkRelBuilder.create(
    frameworkConfig,
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE,
      MiniBatchIntervalTraitDef.INSTANCE,
      UpdateAsRetractionTraitDef.INSTANCE,
      AccModeTraitDef.INSTANCE)
  )

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
    val sinkNode = LogicalSink.create(table.asInstanceOf[TableImpl].getRelNode, sink, sinkName)
    translateSink(sinkNode)
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
    val optimizedNodes = translateNodeDag(Seq(optimizedPlan))
    require(optimizedNodes.size() == 1)
    translateToPlan(optimizedNodes.head)
  }

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
    val optimizedNode = optimize(ast)

    val explainLevel = if (extended) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.EXPPLAN_ATTRIBUTES
    }

    s"== Abstract Syntax Tree ==" +
      System.lineSeparator +
      s"${FlinkRelOptUtil.toString(ast)}" +
      System.lineSeparator +
      s"== Optimized Logical Plan ==" +
      System.lineSeparator +
      s"${FlinkRelOptUtil.toString(optimizedNode, explainLevel)}" +
      System.lineSeparator
    // TODO show Physical Execution Plan
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
    // TODO implements this method when supports multi-sinks
    throw new TableException("Unsupported now")
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

    val (fieldNames, fieldIndexes) = getFieldInfo[T](dataStream.getType)
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

    val streamType = dataStream.getType

    // get field names and types for all non-replaced fields
    val (fieldNames, fieldIndexes) = getFieldInfo[T](streamType, fields)

    // TODO: validate and extract time attributes after we introduce [Expression],
    //  return None currently
    val (rowtime, proctime) = (None, None)

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

