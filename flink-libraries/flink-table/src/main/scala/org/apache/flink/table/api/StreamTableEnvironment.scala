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
import _root_.java.lang.{Boolean => JBool}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.api.common.typeinfo.AtomicType
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.calcite.RelTimeIndicatorConverter
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.{Expression, ProctimeAttribute, RowtimeAttribute, UnresolvedFieldReference}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.{DataStreamRel, UpdateAsRetractionTrait}
import org.apache.flink.table.plan.nodes.datastream._
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.{DataStreamTable, RowSchema, StreamTableSourceTable}
import org.apache.flink.table.runtime.{CRowInputJavaTupleOutputMapRunner, CRowInputMapRunner, CRowInputScalaTupleOutputMapRunner}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.sinks.{AppendStreamTableSink, RetractStreamTableSink, TableSink, UpsertStreamTableSink}
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

  def queryConfig: StreamQueryConfig = new StreamQueryConfig

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
    * @param queryConfig The configuration for the query to generate.
    * @tparam T The expected type of the [[DataStream]] which represents the [[Table]].
    */
  override private[flink] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      queryConfig: QueryConfig): Unit = {

    // Check query configuration
    val streamQueryConfig = queryConfig match {
      case streamConfig: StreamQueryConfig => streamConfig
      case _ =>
        throw new TableException("StreamQueryConfig required to configure stream query.")
    }

    sink match {

      case retractSink: RetractStreamTableSink[_] =>
        // retraction sink can always be used
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] =
          translate(
            table,
            streamQueryConfig,
            updatesAsRetraction = true,
            withChangeFlag = true)(outputType)
        // Give the DataStream to the TableSink to emit it.
        retractSink.asInstanceOf[RetractStreamTableSink[Any]]
          .emitDataStream(result.asInstanceOf[DataStream[JTuple2[JBool, Any]]])

      case upsertSink: UpsertStreamTableSink[_] =>
        // optimize plan
        val optimizedPlan = optimize(table.getRelNode, updatesAsRetraction = false)
        // check for append only table
        val isAppendOnlyTable = isAppendOnly(optimizedPlan)
        upsertSink.setIsAppendOnly(isAppendOnlyTable)
        // extract unique key fields
        val tableKeys: Option[Array[String]] = getUniqueKeyFields(optimizedPlan)
        // check that we have keys if the table has changes (is not append-only)
        tableKeys match {
          case Some(keys) => upsertSink.setKeyFields(keys)
          case None if isAppendOnlyTable => upsertSink.setKeyFields(null)
          case None if !isAppendOnlyTable => throw new TableException(
            "UpsertStreamTableSink requires that Table has a full primary keys if it is updated.")
        }
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] =
          translate(
            optimizedPlan,
            table.getRelNode.getRowType,
            streamQueryConfig,
            withChangeFlag = true)(outputType)
        // Give the DataStream to the TableSink to emit it.
        upsertSink.asInstanceOf[UpsertStreamTableSink[Any]]
          .emitDataStream(result.asInstanceOf[DataStream[JTuple2[JBool, Any]]])

      case appendSink: AppendStreamTableSink[_] =>
        // optimize plan
        val optimizedPlan = optimize(table.getRelNode, updatesAsRetraction = false)
        // verify table is an insert-only (append-only) table
        if (!isAppendOnly(optimizedPlan)) {
          throw new TableException(
            "AppendStreamTableSink requires that Table has only insert changes.")
        }
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] =
          translate(
            optimizedPlan,
            table.getRelNode.getRowType,
            streamQueryConfig,
            withChangeFlag = false)(outputType)
        // Give the DataStream to the TableSink to emit it.
        appendSink.asInstanceOf[AppendStreamTableSink[T]].emitDataStream(result)

      case _ =>
        throw new TableException("Stream Tables can only be emitted by AppendStreamTableSink, " +
          "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
  }

  /**
    * Creates a final converter that maps the internal row type to external type.
    *
    * @param physicalTypeInfo the input of the sink
    * @param schema the input schema with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  protected def getConversionMapper[IN, OUT](
      physicalTypeInfo: TypeInformation[IN],
      schema: RowSchema,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String):
    MapFunction[IN, OUT] = {

    if (requestedTypeInfo.getTypeClass == classOf[Row]) {
      // CRow to Row, only needs to be unwrapped
      new MapFunction[CRow, Row] {
        override def map(value: CRow): Row = value.row
      }.asInstanceOf[MapFunction[IN, OUT]]
    } else {
      // Some type that is neither CRow nor Row
      val converterFunction = generateRowConverterFunction[OUT](
        physicalTypeInfo.asInstanceOf[CRowTypeInfo].rowType,
        schema,
        requestedTypeInfo,
        functionName
      )

      new CRowInputMapRunner[OUT](
        converterFunction.name,
        converterFunction.code,
        converterFunction.returnType)
        .asInstanceOf[MapFunction[IN, OUT]]
    }
  }

  /** Validates that the plan produces only append changes. */
  protected def isAppendOnly(plan: RelNode): Boolean = {
    val appendOnlyValidator = new AppendOnlyValidator
    appendOnlyValidator.go(plan)

    appendOnlyValidator.isAppendOnly
  }

  /** Extracts the unique keys of the table produced by the plan. */
  protected def getUniqueKeyFields(plan: RelNode): Option[Array[String]] = {
    val keyExtractor = new UniqueKeyExtractor
    keyExtractor.go(plan)
    keyExtractor.keys
  }

  /**
    * Creates a converter that maps the internal CRow type to Scala or Java Tuple2 with change flag.
    *
    * @param physicalTypeInfo the input of the sink
    * @param schema the input schema with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink.
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  private def getConversionMapperWithChanges[OUT](
    physicalTypeInfo: TypeInformation[CRow],
    schema: RowSchema,
    requestedTypeInfo: TypeInformation[OUT],
    functionName: String):
  MapFunction[CRow, OUT] = {

    requestedTypeInfo match {

      // Scala tuple
      case t: CaseClassTypeInfo[_]
        if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>

        val reqType = t.getTypeAt(1).asInstanceOf[TypeInformation[Any]]
        if (reqType.getTypeClass == classOf[Row]) {
          // Requested type is Row. Just rewrap CRow in Tuple2
          new MapFunction[CRow, (Boolean, Row)] {
            override def map(cRow: CRow): (Boolean, Row) = {
              (cRow.change, cRow.row)
            }
          }.asInstanceOf[MapFunction[CRow, OUT]]
        } else {
          // Use a map function to convert Row into requested type and wrap result in Tuple2
          val converterFunction = generateRowConverterFunction(
            physicalTypeInfo.asInstanceOf[CRowTypeInfo].rowType,
            schema,
            reqType,
            functionName
          )

          new CRowInputScalaTupleOutputMapRunner(
            converterFunction.name,
            converterFunction.code,
            requestedTypeInfo.asInstanceOf[TypeInformation[(Boolean, Any)]])
            .asInstanceOf[MapFunction[CRow, OUT]]

        }

      // Java tuple
      case t: TupleTypeInfo[_]
        if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>

        val reqType = t.getTypeAt(1).asInstanceOf[TypeInformation[Any]]
        if (reqType.getTypeClass == classOf[Row]) {
          // Requested type is Row. Just rewrap CRow in Tuple2
          new MapFunction[CRow, JTuple2[JBool, Row]] {
            val outT = new JTuple2(true.asInstanceOf[JBool], null.asInstanceOf[Row])
            override def map(cRow: CRow): JTuple2[JBool, Row] = {
              outT.f0 = cRow.change
              outT.f1 = cRow.row
              outT
            }
          }.asInstanceOf[MapFunction[CRow, OUT]]
        } else {
          // Use a map function to convert Row into requested type and wrap result in Tuple2
          val converterFunction = generateRowConverterFunction(
            physicalTypeInfo.asInstanceOf[CRowTypeInfo].rowType,
            schema,
            reqType,
            functionName
          )

          new CRowInputJavaTupleOutputMapRunner(
            converterFunction.name,
            converterFunction.code,
            requestedTypeInfo.asInstanceOf[TypeInformation[JTuple2[JBool, Any]]])
            .asInstanceOf[MapFunction[CRow, OUT]]
        }
    }
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
    * @param updatesAsRetraction True if the sink requests updates as retraction messages.
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(relNode: RelNode, updatesAsRetraction: Boolean): RelNode = {

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
    val physicalPlan = if (physicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(physicalOptRuleSet, logicalPlan, physicalOutputProps)
    } else {
      logicalPlan
    }

    // 6. decorate the optimized plan
    val decoRuleSet = getDecoRuleSet
    val decoratedPlan = if (decoRuleSet.iterator().hasNext) {
      val planToDecorate = if (updatesAsRetraction) {
        physicalPlan.copy(
          physicalPlan.getTraitSet.plus(new UpdateAsRetractionTrait(true)),
          physicalPlan.getInputs)
      } else {
        physicalPlan
      }
      runHepPlanner(
        HepMatchOrder.BOTTOM_UP,
        decoRuleSet,
        planToDecorate,
        planToDecorate.getTraitSet)
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
    * @param queryConfig The configuration for the query to generate.
    * @param updatesAsRetraction Set to true to encode updates as retraction messages.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param tpe The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      table: Table,
      queryConfig: StreamQueryConfig,
      updatesAsRetraction: Boolean,
      withChangeFlag: Boolean)(implicit tpe: TypeInformation[A]): DataStream[A] = {
    val relNode = table.getRelNode
    val dataStreamPlan = optimize(relNode, updatesAsRetraction)
    translate(dataStreamPlan, relNode.getRowType, queryConfig, withChangeFlag)
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataStream]].
    *
    * @param logicalPlan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    *                    field naming during optimization we pass the row type separately.
    * @param queryConfig     The configuration for the query to generate.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param tpe         The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  protected def translate[A](
      logicalPlan: RelNode,
      logicalType: RelDataType,
      queryConfig: StreamQueryConfig,
      withChangeFlag: Boolean)
      (implicit tpe: TypeInformation[A]): DataStream[A] = {

    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !isAppendOnly(logicalPlan)) {
      throw new TableException(
        "Table is not an append-only table. " +
          "Output needs to handle update and delete changes.")
    }

    // get CRow plan
    val plan: DataStream[CRow] = translateToCRow(logicalPlan, queryConfig)

    // convert CRow to output type
    val conversion = if (withChangeFlag) {
      getConversionMapperWithChanges(
        plan.getType,
        new RowSchema(logicalType),
        tpe,
        "DataStreamSinkConversion")
    } else {
      getConversionMapper(
        plan.getType,
        new RowSchema(logicalType),
        tpe,
        "DataStreamSinkConversion")
    }

    val rootParallelism = plan.getParallelism

    conversion match {
      case mapFunction: MapFunction[CRow, A] =>
        plan.map(mapFunction)
          .returns(tpe)
          .name(s"to: ${tpe.getTypeClass.getSimpleName}")
          .setParallelism(rootParallelism)
    }
  }

  /**
    * Translates a logical [[RelNode]] plan into a [[DataStream]] of type [[CRow]].
    *
    * @param logicalPlan The logical plan to translate.
    * @param queryConfig  The configuration for the query to generate.
    * @return The [[DataStream]] of type [[CRow]].
    */
  protected def translateToCRow(
    logicalPlan: RelNode,
    queryConfig: StreamQueryConfig): DataStream[CRow] = {

    logicalPlan match {
      case node: DataStreamRel =>
        node.translateToPlan(this, queryConfig)
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
    val optimizedPlan = optimize(ast, updatesAsRetraction = false)
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

  private class AppendOnlyValidator extends RelVisitor {

    var isAppendOnly = true

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case s: DataStreamRel if s.producesUpdates =>
          isAppendOnly = false
        case _ =>
          super.visit(node, ordinal, parent)
      }
    }
  }

  /** Identifies unique key fields in the output of a RelNode. */
  private class UniqueKeyExtractor extends RelVisitor {

    var keys: Option[Array[String]] = None

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case c: DataStreamCalc =>
          super.visit(node, ordinal, parent)
          // check if input has keys
          if (keys.isDefined) {
            // track keys forward
            val inNames = c.getInput.getRowType.getFieldNames
            val inOutNames = c.getProgram.getNamedProjects.asScala
              .map(p => {
                c.getProgram.expandLocalRef(p.left) match {
                    // output field is forwarded input field
                  case i: RexInputRef => (i.getIndex, p.right)
                    // output field is renamed input field
                  case a: RexCall if a.getKind.equals(SqlKind.AS) =>
                    a.getOperands.get(0) match {
                      case ref: RexInputRef =>
                        (ref.getIndex, p.right)
                      case _ =>
                        (-1, p.right)
                    }
                    // output field is not forwarded from input
                  case _: RexNode => (-1, p.right)
                }
              })
              // filter all non-forwarded fields
              .filter(_._1 >= 0)
              // resolve names of input fields
              .map(io => (inNames.get(io._1), io._2))

            // filter by input keys
            val outKeys = inOutNames.filter(io => keys.get.contains(io._1)).map(_._2)
            // check if all keys have been preserved
            if (outKeys.nonEmpty && outKeys.length == keys.get.length) {
              // all key have been preserved (but possibly renamed)
              keys = Some(outKeys.toArray)
            } else {
              // some (or all) keys have been removed. Keys are no longer unique and removed
              keys = None
            }
          }
        case _: DataStreamOverAggregate =>
          super.visit(node, ordinal, parent)
          // keys are always forwarded by Over aggregate
        case a: DataStreamGroupAggregate =>
          // get grouping keys
          val groupKeys = a.getRowType.getFieldNames.asScala.take(a.getGroupings.length)
          keys = Some(groupKeys.toArray)
        case w: DataStreamGroupWindowAggregate =>
          // get grouping keys
          val groupKeys =
            w.getRowType.getFieldNames.asScala.take(w.getGroupings.length).toArray
          // get window start and end time
          val windowStartEnd = w.getWindowProperties.map(_.name)
          // we have only a unique key if at least one window property is selected
          if (windowStartEnd.nonEmpty) {
            keys = Some(groupKeys ++ windowStartEnd)
          }
        case _: DataStreamRel =>
          // anything else does not forward keys or might duplicate key, so we can stop
          keys = None
      }
    }

  }

}

