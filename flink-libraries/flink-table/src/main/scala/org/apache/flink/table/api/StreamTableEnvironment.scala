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

import _root_.java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.api.common.typeinfo.AtomicType
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, TupleTypeInfo}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.calcite.RelTimeIndicatorConverter
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.{Expression, ProctimeAttribute, RowtimeAttribute, UnresolvedFieldReference}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.{DataStreamRel, UpdateAsRetractionTrait}
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.StreamTableSourceTable
import org.apache.flink.table.plan.schema.DataStreamTable
import org.apache.flink.table.runtime.{CRowInputMapRunner, CRowInputTupleOutputMapRunner}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.sinks.{StreamRetractSink, StreamTableSink, TableSink}
import org.apache.flink.table.sources.{StreamTableSource, TableSource}
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.types.Row

import _root_.scala.collection.JavaConverters._

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

  // a counter for unique table names
  private val nameCntr: AtomicInteger = new AtomicInteger(0)

  // the naming pattern for internally registered tables.
  private val internalNamePattern = "^_DataStreamTable_[0-9]+$".r

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

  /** Returns a unique table name according to the internal naming pattern. */
  protected def createUniqueTableName(): String = "_DataStreamTable_" + nameCntr.getAndIncrement()

  /**
    * Registers an external [[StreamTableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  override def registerTableSource(name: String, tableSource: TableSource[_]): Unit = {
    checkValidTableName(name)

    tableSource match {
      case streamTableSource: StreamTableSource[_] =>
        registerTableInternal(name, new StreamTableSourceTable(streamTableSource))
      case _ =>
        throw new TableException("Only StreamTableSource can be registered in " +
            "StreamTableEnvironment")
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
  override private[flink] def writeToSink[T](table: Table, sink: TableSink[T]): Unit = {

    sink match {
      case streamSink: StreamTableSink[T] =>
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] = translate(table)(outputType)
        // Give the DataSet to the TableSink to emit it.
        streamSink.emitDataStream(result)

      case streamRetractSink: StreamRetractSink[T] =>
        val outputType = sink.getOutputType
        this.config.setNeedsUpdatesAsRetractionForSink(streamRetractSink.needsUpdatesAsRetraction)
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[JTuple2[Boolean, T]] = translate(table, true)(outputType)
        // Give the DataSet to the TableSink to emit it.
        streamRetractSink.emitDataStreamWithChange(result)
      case _ =>
        throw new TableException("StreamTableSink required to emit streaming Table")
    }
  }


  /**
    * Creates a final converter that maps the internal row type to external type.
    *
    * @param physicalTypeInfo the input of the sink
    * @param logicalRowType the logical type with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  override protected def getConversionMapper[IN, OUT](
      physicalTypeInfo: TypeInformation[IN],
      logicalRowType: RelDataType,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String):
  Option[MapFunction[IN, OUT]] = {

    if (requestedTypeInfo.getTypeClass == classOf[CRow]) {
      // only used to explain table
      None
    } else if (requestedTypeInfo.getTypeClass == classOf[Row]) {
      // CRow to Row, only needs to be unwrapped
      Some(
        new MapFunction[CRow, Row] {
          override def map(value: CRow): Row = value.row
        }.asInstanceOf[MapFunction[IN, OUT]]
      )
    } else {
      // Some type that is neither CRow nor Row
      val converterFunction = generateRowConverterFunction[OUT](
        physicalTypeInfo.asInstanceOf[CRowTypeInfo].rowType,
        logicalRowType,
        requestedTypeInfo,
        functionName
      )

      Some(new CRowInputMapRunner[OUT](
        converterFunction.name,
        converterFunction.code,
        converterFunction.returnType)
        .asInstanceOf[MapFunction[IN, OUT]])
    }
  }

  /**
    * Creates a final converter that maps the internal CRow type to external Tuple2 type.
    *
    * @param physicalTypeInfo the input of the sink
    * @param logicalRowType the logical type with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  protected def getTupleConversionMapper[IN, OUT](
      physicalTypeInfo: TypeInformation[IN],
      logicalRowType: RelDataType,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String):
  Option[MapFunction[IN, JTuple2[Boolean, OUT]]] = {

    val converterFunction = generateRowConverterFunction(
      physicalTypeInfo.asInstanceOf[CRowTypeInfo].rowType,
      logicalRowType,
      requestedTypeInfo,
      functionName
    )

    Some(new CRowInputTupleOutputMapRunner[OUT](
      converterFunction.name,
      converterFunction.code,
      new TupleTypeInfo(BasicTypeInfo.BOOLEAN_TYPE_INFO, requestedTypeInfo))
           .asInstanceOf[MapFunction[IN, JTuple2[Boolean, OUT]]])
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
      fieldNames,
      None,
      None
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
      fields: Array[Expression])
    : Unit = {

    val streamType = dataStream.getType

    // get field names and types for all non-replaced fields
    val (fieldNames, fieldIndexes) = getFieldInfo[T](streamType, fields)

    // validate and extract time attributes
    val (rowtime, proctime) = validateAndExtractTimeAttributes(streamType, fields)

    val dataStreamTable = new DataStreamTable[T](
      dataStream,
      fieldIndexes,
      fieldNames,
      rowtime,
      proctime
    )
    registerTableInternal(name, dataStreamTable)
  }

  /**
    * Checks for at most one rowtime and proctime attribute.
    * Returns the time attributes.
    *
    * @return rowtime attribute and proctime attribute
    */
  private def validateAndExtractTimeAttributes(
    streamType: TypeInformation[_],
    exprs: Array[Expression])
  : (Option[(Int, String)], Option[(Int, String)]) = {

    val fieldTypes: Array[TypeInformation[_]] = streamType match {
      case c: CompositeType[_] => (0 until c.getArity).map(i => c.getTypeAt(i)).toArray
      case a: AtomicType[_] => Array(a)
    }

    var fieldNames: List[String] = Nil
    var rowtime: Option[(Int, String)] = None
    var proctime: Option[(Int, String)] = None

    exprs.zipWithIndex.foreach {
      case (RowtimeAttribute(reference@UnresolvedFieldReference(name)), idx) =>
        if (rowtime.isDefined) {
          throw new TableException(
            "The rowtime attribute can only be defined once in a table schema.")
        } else {
          // check type of field that is replaced
          if (idx < fieldTypes.length &&
            !(TypeCheckUtils.isLong(fieldTypes(idx)) ||
              TypeCheckUtils.isTimePoint(fieldTypes(idx)))) {
            throw new TableException(
              "The rowtime attribute can only be replace a field with a valid time type, such as " +
                "Timestamp or Long.")
          }
          rowtime = Some(idx, name)
        }
      case (ProctimeAttribute(reference@UnresolvedFieldReference(name)), idx) =>
        if (proctime.isDefined) {
          throw new TableException(
            "The proctime attribute can only be defined once in a table schema.")
        } else {
          // check that proctime is only appended
          if (idx < fieldTypes.length) {
            throw new TableException(
              "The proctime attribute can only be appended to the table schema and not replace " +
                "an existing field. Please move it to the end of the schema.")
          }
          proctime = Some(idx, name)
        }
      case (u: UnresolvedFieldReference, _) => fieldNames = u.name :: fieldNames
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
    * Returns the decoration rule set for this environment
    * including a custom RuleSet configuration.
    */
  protected def getDecoRuleSet: RuleSet = {
    val calciteConfig = config.getCalciteConfig
    calciteConfig.getDecoRuleSet match {

      case None =>
        getBuiltInDecoRuleSet

      case Some(ruleSet) =>
        if (calciteConfig.replacesDecoRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInDecoRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the built-in normalization rules that are defined by the environment.
    */
  protected def getBuiltInNormRuleSet: RuleSet = FlinkRuleSets.DATASTREAM_NORM_RULES

  /**
    * Returns the built-in optimization rules that are defined by the environment.
    */
  protected def getBuiltInPhysicalOptRuleSet: RuleSet = FlinkRuleSets.DATASTREAM_OPT_RULES

  /**
    * Returns the built-in decoration rules that are defined by the environment.
    */
  protected def getBuiltInDecoRuleSet: RuleSet = FlinkRuleSets.DATASTREAM_DECO_RULES

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The root node of the relational expression tree.
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(relNode: RelNode): RelNode = {

    // 1. decorrelate
    val decorPlan = RelDecorrelator.decorrelateQuery(relNode)

    // 2. convert time indicators
    val convPlan = RelTimeIndicatorConverter.convert(decorPlan, getRelBuilder.getRexBuilder)

    // 3. normalize the logical plan
    val normRuleSet = getNormRuleSet
    val normalizedPlan = if (normRuleSet.iterator().hasNext) {
      runHepPlanner(HepMatchOrder.BOTTOM_UP, normRuleSet, decorPlan, decorPlan.getTraitSet)
    } else {
      decorPlan
    }

    // 4. optimize the logical Flink plan
    val logicalOptRuleSet = getLogicalOptRuleSet
    val logicalOutputProps = relNode.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()
    val logicalPlan = if (logicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(logicalOptRuleSet, normalizedPlan, logicalOutputProps)
    } else {
      normalizedPlan
    }

    // 5. optimize the physical Flink plan
    val physicalOptRuleSet = getPhysicalOptRuleSet
    val physicalOutputProps = relNode.getTraitSet.replace(FlinkConventions.DATASTREAM).simplify()
    var physicalPlan = if (physicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(physicalOptRuleSet, logicalPlan, physicalOutputProps)
    } else {
      logicalPlan
    }

    // 6. decorate the optimized plan
    val decoRuleSet = getDecoRuleSet
    val decoratedPlan = if (decoRuleSet.iterator().hasNext) {

      if (this.config.getNeedsUpdatesAsRetractionForSink) {
        physicalPlan = physicalPlan.copy(
          physicalPlan.getTraitSet.plus(new UpdateAsRetractionTrait(true)),
          physicalPlan.getInputs)
      }
      runHepPlanner(HepMatchOrder.BOTTOM_UP, decoRuleSet, physicalPlan, physicalPlan.getTraitSet)
    } else {
      physicalPlan
    }

    decoratedPlan
  }

  /**
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param tpe The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](table: Table)(implicit tpe: TypeInformation[A]): DataStream[A] = {
    val relNode = table.getRelNode
    val dataStreamPlan = optimize(relNode)
    translate(dataStreamPlan, relNode.getRowType)
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataStream]].
    *
    * @param logicalPlan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    *                    field naming during optimization we pass the row type separately.
    * @param tpe         The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      logicalPlan: RelNode,
      logicalType: RelDataType)
      (implicit tpe: TypeInformation[A]): DataStream[A] = {

    TableEnvironment.validateType(tpe)

    logicalPlan match {
      case node: DataStreamRel =>
        val plan = node.translateToPlan(this)
        val conversion =
          getConversionMapper(plan.getType, logicalType, tpe, "DataStreamSinkConversion")
        conversion match {
          case None => plan.asInstanceOf[DataStream[A]] // no conversion necessary
          case Some(mapFunction: MapFunction[CRow, A]) =>
            plan.map(mapFunction)
              .returns(tpe)
              .name(s"to: ${tpe.getTypeClass.getSimpleName}")
              .asInstanceOf[DataStream[A]]
        }

      case _ =>
        throw TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
    * Translates a [[Table]] into a [[DataStream]] with change information.
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table       The root node of the relational expression tree.
    * @param wrapToTuple True, if want to output chang information
    * @param tpe         The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](table: Table, wrapToTuple: Boolean)(implicit tpe: TypeInformation[A])
  : DataStream[JTuple2[Boolean, A]] = {
    val relNode = table.getRelNode
    val dataStreamPlan = optimize(relNode)
    translate(dataStreamPlan, relNode.getRowType, wrapToTuple)
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataStream]] with change information.
    *
    * @param logicalPlan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    * @param wrapToTuple True, if want to output chang information
    * @param tpe         The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      logicalPlan: RelNode,
      logicalType: RelDataType,
      wrapToTuple: Boolean)(implicit tpe: TypeInformation[A]): DataStream[JTuple2[Boolean, A]] = {

    TableEnvironment.validateType(tpe)

    logicalPlan match {
      case node: DataStreamRel =>
        val plan = node.translateToPlan(this)
        val conversion =
          getTupleConversionMapper(plan.getType, logicalType, tpe, "DataStreamSinkConversion")
        conversion match {
          case None => plan.asInstanceOf[DataStream[JTuple2[Boolean, A]]] // no conversion necessary
          case Some(mapFunction: MapFunction[CRow, JTuple2[Boolean, A]]) =>
            plan.map(mapFunction)
              .returns(new TupleTypeInfo[JTuple2[Boolean, A]](BasicTypeInfo.BOOLEAN_TYPE_INFO, tpe))
              .name(s"to: ${tpe.getTypeClass.getSimpleName}")
              .asInstanceOf[DataStream[JTuple2[Boolean, A]]]
        }

      case _ =>
        throw TableException("Cannot generate DataStream due to an invalid logical plan. " +
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
    val optimizedPlan = optimize(ast)
    val dataStream = translate[CRow](
      optimizedPlan,
      ast.getRowType)(new GenericTypeInfo(classOf[CRow]))

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
