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

package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.functions.Function
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, RichIterativeCondition}
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils.{generateNullLiteral, generateRowtimeAccess}
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.codegen.MatchCodeGenerator._
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable._
import org.apache.flink.table.planner.plan.utils.AggregateUtil
import org.apache.flink.table.planner.plan.utils.MatchUtil.AggregationPatternVariableFinder
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.`match`.{IterativeConditionRunner, PatternProcessFunctionRunner}
import org.apache.flink.table.types.logical.{LocalZonedTimestampType, RowType, TimestampKind, TimestampType}
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.util.Collector
import org.apache.flink.util.MathUtils.checkedDownCast

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.tools.RelBuilder

import java.lang.{Long => JLong}
import java.util

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * A code generator for generating CEP related functions.
  *
  * Aggregates are generated as follows:
  * 1. all aggregate [[RexCall]]s are grouped by corresponding pattern variable
  * 2. even if the same aggregation is used multiple times in an expression
  *    (e.g. SUM(A.price) > SUM(A.price) + 1) it will be calculated once. To do so [[AggBuilder]]
  *    keeps set of already seen different aggregation calls, and reuses the code to access
  *    appropriate field of aggregation result
  * 3. after translating every expression (either in [[generateCondition]] or in
  *    [[generateOneRowPerMatchExpression]]) there will be generated code for
  *       - [[GeneratedFunction]], which will be an inner class
  *       - said [[GeneratedFunction]] will be instantiated in the ctor and opened/closed
  *         in corresponding methods of top level generated classes
  *       - function that transforms input rows (row by row) into aggregate input rows
  *       - function that calculates aggregates for variable, that uses the previous method
  *    The generated code will look similar to this:
  *
  * @param ctx the cotext of the code generator
  * @param nullableInput input(s) can be null.
  * @param patternNames sorted sequence of pattern variables
  * @param currentPattern if generating condition the name of pattern, which the condition will
  *                       be applied to
  */
class MatchCodeGenerator(
    ctx: CodeGeneratorContext,
    relBuilder: RelBuilder,
    nullableInput: Boolean,
    patternNames: Seq[String],
    currentPattern: Option[String] = None,
    collectorTerm: String = CodeGenUtils.DEFAULT_COLLECTOR_TERM)
  extends ExprCodeGenerator(ctx, nullableInput) {

  private case class GeneratedPatternList(resultTerm: String, code: String)

  /**
    * Used to assign unique names for list of events per pattern variable name. Those lists
    * are treated as inputs and are needed by input access code.
    */
  private val reusablePatternLists: mutable.HashMap[String, GeneratedPatternList] =
    mutable.HashMap[String, GeneratedPatternList]()

  /**
    * Used to deduplicate aggregations calculation. The deduplication is performed by
    * [[RexNode#toString]]. Those expressions needs to be accessible from splits, if such exists.
    */
  private val reusableAggregationExpr = new mutable.HashMap[String, GeneratedExpression]()

  /**
    * Context information used by Pattern reference variable to index rows mapped to it.
    * Indexes element at offset either from beginning or the end based on the value of first.
    */
  private var offset: Int = 0
  private var first : Boolean = false

  /**
    * Flags that tells if we generate expressions inside an aggregate. It tells how to access input
    * row.
    */
  private var isWithinAggExprState: Boolean = false

  /**
    * Used to collect all aggregates per pattern variable.
    */
  private val aggregatesPerVariable = new mutable.HashMap[String, AggBuilder]

  /**
    * Name of term in function used to transform input row into aggregate input row.
    */
  private val inputAggRowTerm = "inAgg"

  /** Term for row for key extraction */
  private val keyRowTerm = "keyRow"

  /**
    * @return term of pattern names
    */
  private val patternNamesTerm = newName("patternNames")

  private lazy val eventTypeTerm = boxedTypeTermForType(input1Type)

  /**
    * Sets the new reference variable indexing context. This should be used when resolving logical
    * offsets = LAST/FIRST
    *
    * @param first  true if indexing from the beginning, false otherwise
    * @param offset offset from either beginning or the end
    */
  private def updateOffsets(first: Boolean, offset: Int): Unit = {
    this.first = first
    this.offset = offset
  }

  /** Resets indexing context of Pattern variable. */
  private def resetOffsets(): Unit = {
    first = false
    offset = 0
  }

  private def reusePatternLists(): String = {
    reusablePatternLists.values.map(_.code).mkString("\n")
  }

  private def addReusablePatternNames(): Unit = {
    ctx.addReusableMember(s"private String[] $patternNamesTerm = new String[] { ${
      patternNames.map(p => s""""${EncodingUtils.escapeJava(p)}"""").mkString(", ")
    } };")
  }

  /**
    * Generates a wrapper [[IterativeConditionRunner]] around code generated [[IterativeCondition]]
    * for a single pattern definition defined in DEFINE clause.
    *
    * @param patternDefinition pattern definition as defined in DEFINE clause
    * @return a code generated condition that can be used in constructing a
    *         [[org.apache.flink.cep.pattern.Pattern]]
    */
  def generateIterativeCondition(patternDefinition: RexNode): IterativeCondition[RowData] = {
    val condition = generateCondition(patternDefinition)
    val body =
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin

    val genCondition = generateMatchFunction(
      "MatchRecognizeCondition",
      classOf[RichIterativeCondition[RowData]],
      body)
    new IterativeConditionRunner(genCondition)
  }

  /**
    * Generates a wrapper [[PatternProcessFunctionRunner]] around code generated
    * [[PatternProcessFunction]] that transform found matches into expected output as defined
    * in the MEASURES. It also accounts for fields used in PARTITION BY.
    *
    * @param returnType the row type of output row
    * @param partitionKeys keys used for partitioning incoming data, they will be included in the
    *                      output
    * @param measures definitions from MEASURE clause
    * @return a process function that can be applied to [[org.apache.flink.cep.PatternStream]]
    */
  def generateOneRowPerMatchExpression(
      returnType: RowType,
      partitionKeys: Array[Int],
      measures: util.Map[String, RexNode])
    : PatternProcessFunctionRunner = {
    val resultExpression = generateOneRowPerMatchExpression(
      partitionKeys,
      measures,
      returnType)
    val body =
      s"""
         |${resultExpression.code}
         |$collectorTerm.collect(${resultExpression.resultTerm});
         |""".stripMargin

    val genFunction = generateMatchFunction(
      "MatchRecognizePatternProcessFunction",
      classOf[PatternProcessFunction[RowData, RowData]],
      body)
    new PatternProcessFunctionRunner(genFunction)
  }

  /**
    * Generates a [[org.apache.flink.api.common.functions.Function]] that can be passed to Java
    * compiler.
    *
    * @param name Class name of the Function. Must not be unique but has to be a valid Java class
    *             identifier.
    * @param clazz Flink Function to be generated.
    * @param bodyCode code contents of the SAM (Single Abstract Method). Inputs, collector, or
    *                 output record can be accessed via the given term methods.
    * @tparam F Flink Function to be generated.
    * @tparam T Return type of the Flink Function.
    * @return instance of GeneratedFunction
    */
  private def generateMatchFunction[F <: Function, T <: Any](
      name: String,
      clazz: Class[F],
      bodyCode: String)
    : GeneratedFunction[F] = {
    val funcName = newName(name)
    val collectorTypeTerm = classOf[Collector[Any]].getCanonicalName
    val (functionClass, signature, inputStatements) =
      if (clazz == classOf[RichIterativeCondition[_]]) {
        val inputTypeTerm = boxedTypeTermForType(input1Type)
        val baseClass = classOf[RichIterativeCondition[_]]
        val contextType = classOf[IterativeCondition.Context[_]].getCanonicalName

        (baseClass,
          s"boolean filter(Object _in1, $contextType $contextTerm)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
      } else if (clazz == classOf[PatternProcessFunction[_, _]]) {
        val baseClass = classOf[PatternProcessFunction[_, _]]
        val inputTypeTerm =
          s"java.util.Map<String, java.util.List<${boxedTypeTermForType(input1Type)}>>"
        val contextTypeTerm = classOf[PatternProcessFunction.Context].getCanonicalName

        (baseClass,
          s"void processMatch($inputTypeTerm $input1Term, $contextTypeTerm $contextTerm, " +
            s"$collectorTypeTerm $collectorTerm)",
          List())
      } else throw new CodeGenException("Unsupported Function.")

    val funcCode =
      j"""
        public class $funcName extends ${functionClass.getCanonicalName} {
          ${ctx.reuseMemberCode()}
          ${ctx.reuseLocalVariableCode()}

          public $funcName(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public void open(${classOf[Configuration].getCanonicalName} parameters) throws Exception {
            ${ctx.reuseOpenCode()}
          }

          @Override
          public $signature throws Exception {
            ${inputStatements.mkString("\n")}
            ${reusePatternLists()}
            ${ctx.reusePerRecordCode()}
            ${ctx.reuseInputUnboxingCode()}
            $bodyCode
          }

          @Override
          public void close() throws Exception {
            ${ctx.reuseCloseCode()}
          }
        }
      """.stripMargin

    new GeneratedFunction[F](funcName, funcCode, ctx.references.toArray)
  }

  private def generateOneRowPerMatchExpression(
      partitionKeys: Array[Int],
      measures: java.util.Map[String, RexNode],
      returnType: RowType): GeneratedExpression = {

    // For "ONE ROW PER MATCH", the output columns include:
    // 1) the partition columns;
    // 2) the columns defined in the measures clause.
    val resultExprs =
    partitionKeys.map(generatePartitionKeyAccess) ++
      returnType.getFieldNames
        .filter(measures.containsKey(_))
        .map { fieldName =>
          generateExpression(measures.get(fieldName))
        }

    val resultCodeGenerator = new ExprCodeGenerator(ctx, nullableInput)
      .bindInput(input1Type, inputTerm = input1Term)
    val resultExpression = resultCodeGenerator.generateResultExpression(
      resultExprs,
      returnType,
      classOf[GenericRowData])

    aggregatesPerVariable.values.foreach(_.generateAggFunction())

    resultExpression
  }

  private def generateCondition(call: RexNode): GeneratedExpression = {
    val exp = call.accept(this)
    aggregatesPerVariable.values.foreach(_.generateAggFunction())
    exp
  }

  override def visitCall(call: RexCall): GeneratedExpression = {
    call.getOperator match {
      case PREV | NEXT =>
        val countLiteral = call.getOperands.get(1).asInstanceOf[RexLiteral]
        val count = checkedDownCast(countLiteral.getValueAs(classOf[JLong]))
        if (count != 0) {
          throw new TableException("Flink does not support physical offsets within partition.")
        } else {
          updateOffsets(first = false, 0)
          val exp = call.getOperands.get(0).accept(this)
          resetOffsets()
          exp
        }

      case FIRST | LAST =>
        val countLiteral = call.getOperands.get(1).asInstanceOf[RexLiteral]
        val offset = checkedDownCast(countLiteral.getValueAs(classOf[JLong]))
        updateOffsets(call.getOperator == FIRST, offset)
        val patternExp = call.operands.get(0).accept(this)
        resetOffsets()
        patternExp

      case FINAL => call.getOperands.get(0).accept(this)

      case _: SqlAggFunction =>
        val variable = call.accept(new AggregationPatternVariableFinder)
          .getOrElse(throw new TableException("No pattern variable specified in aggregate"))

        val matchAgg = aggregatesPerVariable.get(variable) match {
          case Some(agg) => agg
          case None =>
            val agg = new AggBuilder(variable)
            aggregatesPerVariable(variable) = agg
            agg
        }

        matchAgg.generateDeduplicatedAggAccess(call)

      case MATCH_PROCTIME =>
        // attribute is proctime indicator.
        // We use a null literal and generate a timestamp when we need it.
        generateNullLiteral(
          new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3),
          ctx.nullCheck)

      case MATCH_ROWTIME =>
        generateRowtimeAccess(
          ctx,
          contextTerm,
          FlinkTypeFactory.isTimestampLtzIndicatorType(call.getType))

      case PROCTIME_MATERIALIZE =>
        // override proctime materialize code generation
        // because there is no timerService in PatternProcessFunction#Context
        generateProctimeTimestamp()

      case _ => super.visitCall(call)
    }
  }

  private def generateProctimeTimestamp(): GeneratedExpression = {
    val resultType = new LocalZonedTimestampType(3)
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    val resultTerm = ctx.addReusableLocalVariable(resultTypeTerm, "result")
    val resultCode =
      s"""
         |$resultTerm = $TIMESTAMP_DATA.fromEpochMillis($contextTerm.currentProcessingTime());
         |""".stripMargin.trim
    // the proctime has been materialized, so it's TIMESTAMP now, not PROCTIME_INDICATOR
    GeneratedExpression(resultTerm, NEVER_NULL, resultCode, resultType)
  }

  /**
    * Extracts partition keys from any element of the match
    *
    * @param partitionKeyIdx partition key index
    * @return generated code for the given key
    */
  private def generatePartitionKeyAccess(partitionKeyIdx: Int): GeneratedExpression = {
    val keyRow = generateKeyRow()
    GenerateUtils.generateFieldAccess(
      ctx,
      keyRow.resultType,
      keyRow.resultTerm,
      partitionKeyIdx
    )
  }

  private def generateKeyRow(): GeneratedExpression = {
    val exp = ctx.getReusableInputUnboxingExprs(keyRowTerm, 0) match {
      case Some(expr) =>
        expr

      case None =>
        val nullTerm = newName("isNull")

        ctx.addReusableMember(s"$eventTypeTerm $keyRowTerm;")

        val keyCode =
          j"""
             |boolean $nullTerm = true;
             |for (java.util.Map.Entry entry : $input1Term.entrySet()) {
             |  java.util.List value = (java.util.List) entry.getValue();
             |  if (value != null && value.size() > 0) {
             |    $keyRowTerm = ($eventTypeTerm) value.get(0);
             |    $nullTerm = false;
             |    break;
             |  }
             |}
             |""".stripMargin

        val exp = GeneratedExpression(keyRowTerm, nullTerm, keyCode, input1Type)
        ctx.addReusableInputUnboxingExprs(keyRowTerm, 0, exp)
        exp
    }
    exp.copy(code = NO_CODE)
  }

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression = {
    if (isWithinAggExprState) {
      GenerateUtils.generateFieldAccess(ctx, input1Type, inputAggRowTerm, fieldRef.getIndex)
    } else {
      if (fieldRef.getAlpha.equals(ALL_PATTERN_VARIABLE) &&
        currentPattern.isDefined && offset == 0 && !first) {
        GenerateUtils.generateInputAccess(
          ctx, input1Type, input1Term, fieldRef.getIndex, nullableInput)
      } else {
        generatePatternFieldRef(fieldRef)
      }
    }
  }

  private def generateDefinePatternVariableExp(
      patternName: String,
      currentPattern: String)
    : GeneratedPatternList = {
    val Seq(listName, eventNameTerm) = newNames("patternEvents", "event")

    ctx.addReusableMember(s"java.util.List $listName;")

    val addCurrent = if (currentPattern == patternName || patternName == ALL_PATTERN_VARIABLE) {
      j"""
         |$listName.add($input1Term);
         |""".stripMargin
    } else {
      ""
    }

    val listCode = if (patternName == ALL_PATTERN_VARIABLE) {
      addReusablePatternNames()
      val patternTerm = newName("pattern")

      j"""
         |$listName = new java.util.ArrayList();
         |for (String $patternTerm : $patternNamesTerm) {
         |  for ($eventTypeTerm $eventNameTerm :
         |  $contextTerm.getEventsForPattern($patternTerm)) {
         |    $listName.add($eventNameTerm);
         |  }
         |}
         |""".stripMargin
    } else {
      val escapedPatternName = EncodingUtils.escapeJava(patternName)
      j"""
         |$listName = new java.util.ArrayList();
         |for ($eventTypeTerm $eventNameTerm :
         |  $contextTerm.getEventsForPattern("$escapedPatternName")) {
         |    $listName.add($eventNameTerm);
         |}
         |""".stripMargin
    }

    val code =
      j"""
         |$listCode
         |$addCurrent
         |""".stripMargin

    GeneratedPatternList(listName, code)
  }

  private def generateMeasurePatternVariableExp(patternName: String): GeneratedPatternList = {
    val Seq(listName, patternTerm) = newNames("patternEvents", "pattern")
    ctx.addReusableMember(s"java.util.List $listName;")

    val code = if (patternName == ALL_PATTERN_VARIABLE) {
      addReusablePatternNames()

      j"""
         |$listName = new java.util.ArrayList();
         |for (String $patternTerm : $patternNamesTerm) {
         |  java.util.List rows = (java.util.List) $input1Term.get($patternTerm);
         |  if (rows != null) {
         |    $listName.addAll(rows);
         |  }
         |}
         |""".stripMargin
    } else {
      val escapedPatternName = EncodingUtils.escapeJava(patternName)

      j"""
         |$listName = (java.util.List) $input1Term.get("$escapedPatternName");
         |if ($listName == null) {
         |  $listName = java.util.Collections.emptyList();
         |}
         |""".stripMargin
    }

    GeneratedPatternList(listName, code)
  }

  private def findEventByLogicalPosition(patternFieldAlpha: String): GeneratedExpression = {
    val Seq(rowNameTerm, isRowNull) = newNames("row", "isRowNull")

    val listName = findEventsByPatternName(patternFieldAlpha).resultTerm
    val resultIndex = if (first) {
      j"""$offset"""
    } else {
      j"""$listName.size() - $offset - 1"""
    }

    val funcCode =
      j"""
         |$eventTypeTerm $rowNameTerm = null;
         |boolean $isRowNull = true;
         |if ($listName.size() > $offset) {
         |  $rowNameTerm = (($eventTypeTerm) $listName.get($resultIndex));
         |  $isRowNull = false;
         |}
         |""".stripMargin

    GeneratedExpression(rowNameTerm, "", funcCode, input1Type)
  }

  private def findEventsByPatternName(patternFieldAlpha: String): GeneratedPatternList = {
    reusablePatternLists.get(patternFieldAlpha) match {
      case Some(expr) =>
        expr

      case None =>
        val exp = currentPattern match {
          case Some(p) => generateDefinePatternVariableExp(patternFieldAlpha, p)
          case None => generateMeasurePatternVariableExp(patternFieldAlpha)
        }
        reusablePatternLists(patternFieldAlpha) = exp
        exp
    }
  }

  private def generatePatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression = {
    val escapedAlpha = EncodingUtils.escapeJava(fieldRef.getAlpha)

    val patternVariableRef = ctx.getReusableInputUnboxingExprs(
      s"$escapedAlpha#$first", offset) match {
      case Some(expr) =>
        expr

      case None =>
        val exp = findEventByLogicalPosition(fieldRef.getAlpha)
        ctx.addReusableInputUnboxingExprs(s"$escapedAlpha#$first", offset, exp)
        exp
    }

    GenerateUtils.generateNullableInputFieldAccess(
      ctx,
      patternVariableRef.resultType,
      patternVariableRef.resultTerm,
      fieldRef.getIndex)
  }

  class AggBuilder(variable: String) {

    private val aggregates = new mutable.ListBuffer[RexCall]()

    private val variableUID = newName("variable")

    private val calculateAggFuncName = s"calculateAgg_$variableUID"

    def generateDeduplicatedAggAccess(aggCall: RexCall): GeneratedExpression = {
      reusableAggregationExpr.get(aggCall.toString) match  {
        case Some(expr) =>
          expr

        case None =>
          val exp: GeneratedExpression = generateAggAccess(aggCall)
          aggregates += aggCall
          reusableAggregationExpr(aggCall.toString) = exp
          ctx.addReusablePerRecordStatement(exp.code)
          exp.copy(code = NO_CODE)
      }
    }

    private def generateAggAccess(aggCall: RexCall): GeneratedExpression = {
      val singleAggResultTerm = newName("result")
      val singleAggNullTerm = newName("nullTerm")
      val singleAggResultType = FlinkTypeFactory.toLogicalType(aggCall.`type`)
      val primitiveSingleAggResultTypeTerm = primitiveTypeTermForType(singleAggResultType)
      val boxedSingleAggResultTypeTerm = boxedTypeTermForType(singleAggResultType)

      val allAggRowTerm = s"aggRow_$variableUID"

      val rowsForVariableCode = findEventsByPatternName(variable)
      val codeForAgg =
        j"""
           |$GENERIC_ROW $allAggRowTerm = $calculateAggFuncName(${rowsForVariableCode.resultTerm});
           |""".stripMargin

      ctx.addReusablePerRecordStatement(codeForAgg)

      val defaultValue = primitiveDefaultValue(singleAggResultType)
      val codeForSingleAgg = if (ctx.nullCheck) {
        j"""
           |boolean $singleAggNullTerm;
           |$primitiveSingleAggResultTypeTerm $singleAggResultTerm;
           |if ($allAggRowTerm.getField(${aggregates.size}) != null) {
           |  $singleAggResultTerm = ($boxedSingleAggResultTypeTerm) $allAggRowTerm
           |    .getField(${aggregates.size});
           |  $singleAggNullTerm = false;
           |} else {
           |  $singleAggNullTerm = true;
           |  $singleAggResultTerm = $defaultValue;
           |}
           |""".stripMargin
      } else {
        j"""
           |$primitiveSingleAggResultTypeTerm $singleAggResultTerm =
           |    ($boxedSingleAggResultTypeTerm) $allAggRowTerm.getField(${aggregates.size});
           |""".stripMargin
      }

      ctx.addReusablePerRecordStatement(codeForSingleAgg)

      GeneratedExpression(singleAggResultTerm, singleAggNullTerm, NO_CODE, singleAggResultType)
    }

    def generateAggFunction(): Unit = {
      val matchAgg = extractAggregatesAndExpressions

      val aggCalls = matchAgg.aggregations.map(a => AggregateCall.create(
        a.sqlAggFunction,
        false,
        false,
        a.exprIndices,
        -1,
        a.resultType,
        a.sqlAggFunction.getName))

      val needRetraction = matchAgg.aggregations.map(_ => false).toArray

      val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
      val inputRelType = typeFactory.createStructType(
        matchAgg.inputExprs.map(_.getType),
        matchAgg.inputExprs.indices.map(i => s"TMP$i"))

      val aggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
        FlinkTypeFactory.toLogicalRowType(inputRelType),
        aggCalls,
        needRetraction,
        needInputCount = false,
        isStateBackendDataViews = false,
        needDistinctInfo = false)

      val inputFieldTypes = matchAgg.inputExprs
        .map(expr => FlinkTypeFactory.toLogicalType(expr.getType))

      val aggsHandlerCodeGenerator = new AggsHandlerCodeGenerator(
        CodeGeneratorContext(new TableConfig),
        relBuilder,
        inputFieldTypes,
        copyInputField = false).needAccumulate()
      val generatedAggsHandler = aggsHandlerCodeGenerator.generateAggsHandler(
        s"AggFunction_$variableUID",
        aggInfoList)

      val generatedTerm = ctx.addReusableObject(generatedAggsHandler, "generatedAggHandler")
      val aggsHandlerTerm = s"aggregator_$variableUID"

      val declareCode = s"private $AGGS_HANDLER_FUNCTION $aggsHandlerTerm;"
      val initCode = s"$aggsHandlerTerm = ($AGGS_HANDLER_FUNCTION) " +
        s"$generatedTerm.newInstance($CURRENT_CLASS_LOADER);"

      ctx.addReusableMember(declareCode)
      ctx.addReusableInitStatement(initCode)

      val transformFuncName = s"transformRowForAgg_$variableUID"
      val inputTransform: String = generateAggInputExprEvaluation(
        matchAgg.inputExprs,
        transformFuncName)

      generateAggCalculation(aggsHandlerTerm, transformFuncName, inputTransform)
    }

    private def extractAggregatesAndExpressions: MatchAgg = {
      val inputRows = new mutable.LinkedHashMap[String, (RexNode, Int)]

      val singleAggregates = aggregates.map { aggCall =>
        val callsWithIndices = aggCall.operands.asScala.map(innerCall => {
          inputRows.get(innerCall.toString) match {
            case Some(x) =>
              x

            case None =>
              val callWithIndex = (innerCall, inputRows.size)
              inputRows(innerCall.toString) = callWithIndex
              callWithIndex
          }
        })

        SingleAggCall(
          aggCall.getOperator.asInstanceOf[SqlAggFunction],
          aggCall.`type`,
          callsWithIndices.map(callsWithIndice => Integer.valueOf(callsWithIndice._2)))
      }

      MatchAgg(singleAggregates, inputRows.values.map(_._1).toSeq)
    }

    private def generateAggCalculation(
      aggsHandlerTerm: String,
      transformFuncName: String,
      inputTransformFunc: String): Unit = {
      val code =
        j"""
           |$inputTransformFunc
           |
           |private $GENERIC_ROW $calculateAggFuncName(java.util.List input)
           |    throws Exception {
           |  $aggsHandlerTerm.setAccumulators($aggsHandlerTerm.createAccumulators());
           |  for ($ROW_DATA row : input) {
           |    $aggsHandlerTerm.accumulate($transformFuncName(row));
           |  }
           |  $GENERIC_ROW result = ($GENERIC_ROW) $aggsHandlerTerm.getValue();
           |  return result;
           |}
           |""".stripMargin

      ctx.addReusableMember(code)
      ctx.addReusableOpenStatement(
        s"$aggsHandlerTerm.open(new $AGGS_HANDLER_CONTEXT(getRuntimeContext()));")
      ctx.addReusableCloseStatement(s"$aggsHandlerTerm.close();")
    }

    private def generateAggInputExprEvaluation(
        inputExprs: Seq[RexNode],
        funcName: String): String = {
      isWithinAggExprState = true
      val resultTerm = newName("result")
      val exprs = inputExprs.zipWithIndex.map {
        case (inputExpr, outputIndex) =>
          val expr = generateExpression(inputExpr)
          s"""
             |  ${expr.code}
             |  if (${expr.nullTerm}) {
             |    $resultTerm.setField($outputIndex, null);
             |  } else {
             |    $resultTerm.setField($outputIndex, ${expr.resultTerm});
             |  }
         """.stripMargin
      }.mkString("\n")
      isWithinAggExprState = false

      j"""
         |private $GENERIC_ROW $funcName($ROW_DATA $inputAggRowTerm) {
         |  $GENERIC_ROW $resultTerm = new $GENERIC_ROW(${inputExprs.size});
         |  $exprs
         |  return $resultTerm;
         |}
         |""".stripMargin
    }

    private case class SingleAggCall(
      sqlAggFunction: SqlAggFunction,
      resultType: RelDataType,
      exprIndices: Seq[Integer]
    )

    private case class MatchAgg(
      aggregations: Seq[SingleAggCall],
      inputExprs: Seq[RexNode]
    )
  }
}

object MatchCodeGenerator {
  val ALL_PATTERN_VARIABLE = "*"

  val AGGS_HANDLER_CONTEXT: String = className[PerKeyStateDataViewStore]

  val CURRENT_CLASS_LOADER = "Thread.currentThread().getContextClassLoader()"
}
