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

package org.apache.flink.table.codegen

import java.lang.{Long => JLong}
import java.util

import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, RichIterativeCondition}
import org.apache.flink.cep.RichPatternFlatSelectFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.dataview.DataViewSpec
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.GeneratedExpression.{ALWAYS_NULL, NEVER_NULL, NO_CODE}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.functions.{AggregateFunction => TableAggregateFunction}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.`match`.{IterativeConditionRunner, PatternFlatSelectFunctionRunner}
import org.apache.flink.table.runtime.aggregate.AggregateUtil
import org.apache.flink.table.util.MatchUtil.{ALL_PATTERN_VARIABLE, AggregationPatternVariableFinder}
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.util.MathUtils.checkedDownCast

import scala.collection.JavaConverters._
import scala.collection.mutable

object MatchCodeGenerator {

  def generateIterativeCondition(
    config: TableConfig,
    patternDefinition: RexNode,
    inputTypeInfo: TypeInformation[_],
    patternName: String,
    names: Seq[String])
  : IterativeConditionRunner = {
    val generator = new MatchCodeGenerator(config, inputTypeInfo, names, Some(patternName))
    val condition = generator.generateCondition(patternDefinition)
    val body =
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin

    val genCondition = generator
      .generateMatchFunction("MatchRecognizeCondition",
        classOf[RichIterativeCondition[Row]],
        body,
        condition.resultType)
    new IterativeConditionRunner(genCondition.name, genCondition.code)
  }

  def generatePatternFlatSelectFunction(
    config: TableConfig,
    returnType: RowSchema,
    partitionKeys: util.List[RexNode],
    orderKeys: util.List[RelFieldCollation],
    measures: util.Map[String, RexNode],
    inputTypeInfo: TypeInformation[_],
    patternNames: Seq[String],
    allRows: Boolean)
  : PatternFlatSelectFunctionRunner = {
    val generator = new MatchCodeGenerator(config, inputTypeInfo, patternNames)

    val resultExpression = if (allRows) {
      generator.generateAllRowsPerMatchExpression(
        partitionKeys,
        orderKeys,
        measures,
        returnType)
    } else {
      generator.generateOneRowPerMatchExpression(
        partitionKeys,
        measures,
        returnType)
    }
    val body =
      s"""
         |${resultExpression.code}
         |""".stripMargin

    val genFunction = generator.generateMatchFunction(
      "MatchRecognizePatternFlatSelectFunction",
      classOf[RichPatternFlatSelectFunction[Row, Row]],
      body,
      resultExpression.resultType)
    new PatternFlatSelectFunctionRunner(genFunction.name, genFunction.code)
  }
}

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
  *
  * {{{
  *
  * public class MatchRecognizePatternSelectFunction$175 extends RichPatternSelectFunction {
  *
  *     // Class used to calculate aggregates for a single pattern variable
  *     public final class AggFunction_variable$115$151 extends GeneratedAggregations {
  *       ...
  *     }
  *
  *     private final AggFunction_variable$115$151 aggregator_variable$115;
  *
  *     public MatchRecognizePatternSelectFunction$175() {
  *       aggregator_variable$115 = new AggFunction_variable$115$151();
  *     }
  *
  *     public void open() {
  *       aggregator_variable$115.open();
  *       ...
  *     }
  *
  *     // Function to transform incoming row into aggregate specific row. It can e.g calculate
  *     // inner expression of said aggregate
  *     private Row transformRowForAgg_variable$115(Row inAgg) {
  *         ...
  *     }
  *
  *     // Function to calculate all aggregates for a single pattern variable
  *     private Row calculateAgg_variable$115(List<Row> input) {
  *       Acc accumulator = aggregator_variable$115.createAccumulator();
  *       for (Row row : input) {
  *         aggregator_variable$115.accumulate(accumulator, transformRowForAgg_variable$115(row));
  *       }
  *
  *       return aggregator_variable$115.getResult(accumulator);
  *     }
  *
  *     @Override
  *     public Object select(Map<String, List<Row>> in1) throws Exception {
  *
  *       // Extract list of rows assigned to a single pattern variable
  *       java.util.List patternEvents$130 = (java.util.List) in1.get("A");
  *       ...
  *
  *       // Calculate aggregates
  *       Row aggRow_variable$110$111 = calculateAgg_variable$110(patternEvents$114);
  *
  *       // Every aggregation (e.g SUM(A.price) and AVG(A.price)) will be extracted to a variable
  *       double result$135 = aggRow_variable$126$127.getField(0);
  *       long result$137 = aggRow_variable$126$127.getField(1);
  *
  *       // Result of aggregation will be used in expression evaluation
  *       out.setField(0, result$135)
  *
  *       long result$140 = result$137 * 2;
  *       out.setField(1, result$140);
  *
  *       double result$144 = $result135 + result$137;
  *       out.setField(2, result$144);
  *     }
  *
  *     public void close() {
  *       aggregator_variable$115.close();
  *       ...
  *     }
  *
  * }
  * }}}
  *
  * @param config configuration that determines runtime behavior
  * @param patternNames sorted sequence of pattern variables
  * @param input type information about the first input of the Function
  * @param currentPattern if generating condition the name of pattern, which the condition will
  *                       be applied to
  */
class MatchCodeGenerator(
    config: TableConfig,
    input: TypeInformation[_],
    patternNames: Seq[String],
    currentPattern: Option[String] = None)
  extends CodeGenerator(config, false, input){

  private case class GeneratedPatternList(
      resultTerm: String, code: String, running: Boolean = false)

  /**
    * Used to assign unique names for list of events per pattern variable name. Those lists
    * are treated as inputs and are needed by input access code.
    */
  private val reusablePatternLists: mutable.HashMap[(Boolean, String), GeneratedPatternList] =
    mutable.HashMap[(Boolean, String), GeneratedPatternList]()

  /**
    * Used to deduplicate aggregations calculation. The deduplication is performed by
    * [[RexNode.toString]]. Those expressions needs to be accessible from splits, if such exists.
    */
  private val reusableAggregationExpr =
    new mutable.HashMap[(Boolean, String), GeneratedExpression]()

  private val reusableRunningInputUnboxingExprs: mutable.Map[(String, Int), GeneratedExpression] =
    mutable.Map[(String, Int), GeneratedExpression]()

  private val reusableRunningPerRecordStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  /**
    * Context information used by Pattern reference variable to index rows mapped to it.
    * Indexes element at offset either from beginning or the end based on the value of first.
    */
  private var offset: Int = 0
  private var first : Boolean = false
  private var allRows: Boolean = false
  private var running: Boolean = false

  /**
    * Flags that tells if we generate expressions inside an aggregate. It tells how to access input
    * row.
    */
  private var isWithinAggExprState: Boolean = false

  /**
    * Name of term in function used to transform input row into aggregate input row.
    */
  private val inputAggRowTerm = "inAgg"

  /** Term for row for key extraction */
  private val keyRowTerm = "keyRow"

  /** Term for list of all pattern names */
  private val patternNamesTerm = "patternNames"

  /**
    * Used to collect all aggregates per pattern variable.
    */
  private val aggregatesPerVariable = new mutable.HashMap[(Boolean, String), AggBuilder]

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

  /**
    * Sets the RUNNING/FINAL semantics.
    *
    * @param running true if RUNNING semantics is used
    */
  private def setRunning(running: Boolean): Unit = {
    this.running = running
  }

  /**
    * Resets the RUNNING/FINAL semantics.
    */
  private def resetRunning(): Unit = {
    this.running = false
  }

  private def setAllRows(allRows: Boolean): Unit = {
    this.allRows = allRows
  }

  private def reusePatternLists(): String = {
    reusablePatternLists.values.map(_.code).mkString("\n")
  }

  private def addReusablePatternNames() : Unit = {
    reusableMemberStatements
      .add(s"private String[] $patternNamesTerm = new String[] { ${
        patternNames.map(p => s""""${EncodingUtils.escapeJava(p)}"""").mkString(", ")
      } };")
  }

  private def reuseRunningInputUnboxingCode(): String = {
    reusableRunningInputUnboxingExprs.values.map(_.code).mkString("\n")
  }

  private def reuseRunningPerRecordCode(): String = {
    reusableRunningPerRecordStatements.mkString("\n")
  }

  private def addReusablePerRecordStatement(s: String): Unit = {
    if (running) {
      reusableRunningPerRecordStatements.add(s)
    } else {
      reusablePerRecordStatements.add(s)
    }
  }

  private def addReusableInputUnboxingExprs(
      inputTerm: String, index: Int, expr: GeneratedExpression): Unit =
    if (running) {
      reusableRunningInputUnboxingExprs((inputTerm, index)) = expr
    } else {
      reusableInputUnboxingExprs((inputTerm, index)) = expr
    }

  private def getReusableInputUnboxingExprs(inputTerm: String, index: Int)
      : Option[GeneratedExpression] =
    if (running) {
      reusableRunningInputUnboxingExprs.get((inputTerm, index))
    } else {
      reusableInputUnboxingExprs.get((inputTerm, index))
    }

  private def makeReusableInSplitsForRunningInputs(
      exprsWithRunningFlag: Iterable[GeneratedExpression]): Unit = {
    // add results of expressions to member area such that all split functions can access it
    exprsWithRunningFlag.foreach { expr =>

      // declaration
      val resultTypeTerm = primitiveTypeTermForTypeInfo(expr.resultType)
      if (nullCheck && !expr.nullTerm.equals(NEVER_NULL) && !expr.nullTerm.equals(ALWAYS_NULL)) {
        reusableMemberStatements.add(s"private boolean ${expr.nullTerm};")
      }
      reusableMemberStatements.add(s"private $resultTypeTerm ${expr.resultTerm};")

      // assignment
      if (nullCheck && !expr.nullTerm.equals(NEVER_NULL) && !expr.nullTerm.equals(ALWAYS_NULL)) {
        reusableRunningPerRecordStatements.add(s"this.${expr.nullTerm} = ${expr.nullTerm};")
      }
      reusableRunningPerRecordStatements.add(s"this.${expr.resultTerm} = ${expr.resultTerm};")
    }
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
    * @param returnType expected return type
    * @tparam F Flink Function to be generated.
    * @tparam T Return type of the Flink Function.
    * @return instance of GeneratedFunction
    */
  def generateMatchFunction[F <: Function, T <: Any](
      name: String,
      clazz: Class[F],
      bodyCode: String,
      returnType: TypeInformation[T])
    : GeneratedFunction[F, T] = {
    val funcName = newName(name)
    val collectorTypeTerm = classOf[Collector[Any]].getCanonicalName
    val (functionClass, signature, inputStatements) =
      if (clazz == classOf[RichIterativeCondition[_]]) {
        val baseClass = classOf[RichIterativeCondition[_]]
        val inputTypeTerm = boxedTypeTermForTypeInfo(input)
        val contextType = classOf[IterativeCondition.Context[_]].getCanonicalName

        (baseClass,
          s"boolean filter(Object _in1, $contextType $contextTerm)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"))
      } else if (clazz == classOf[RichPatternFlatSelectFunction[_, _]]) {
        val baseClass = classOf[RichPatternFlatSelectFunction[_, _]]
        val inputTypeTerm =
          s"java.util.Map<String, java.util.List<${boxedTypeTermForTypeInfo(input)}>>"

        (baseClass,
          s"void flatSelect($inputTypeTerm $input1Term, $collectorTypeTerm $collectorTerm)",
          List())
      } else {
        throw new CodeGenException("Unsupported Function.")
      }

    val funcCode = j"""
      |public class $funcName extends ${functionClass.getCanonicalName} {
      |
      |  ${reuseMemberCode()}
      |
      |  public $funcName() throws Exception {
      |    ${reuseInitCode()}
      |  }
      |
      |  @Override
      |  public void open(${classOf[Configuration].getCanonicalName} parameters) throws Exception {
      |    ${reuseOpenCode()}
      |  }
      |
      |  @Override
      |  public $signature throws Exception {
      |    ${inputStatements.mkString("\n")}
      |    ${reusePatternLists()}
      |    ${reuseInputUnboxingCode()}
      |    ${reusePerRecordCode()}
      |    $bodyCode
      |  }
      |
      |  @Override
      |  public void close() throws Exception {
      |    ${reuseCloseCode()}
      |  }
      |}
    """.stripMargin

    GeneratedFunction(funcName, returnType, funcCode)
  }

  private def generateKeyRow() : GeneratedExpression = {
    val exp = reusableInputUnboxingExprs
      .get((keyRowTerm, 0)) match {
      case Some(expr) =>
        expr

      case None =>

        val eventTypeTerm = boxedTypeTermForTypeInfo(input)
        val nullTerm = newName("isNull")

        val keyCode = j"""
           |$eventTypeTerm $keyRowTerm = null;
           |boolean $nullTerm = true;
           |for (java.util.Map.Entry entry : $input1Term.entrySet()) {
           |  java.util.List value = (java.util.List) entry.getValue();
           |  if (value != null && value.size() > 0) {
           |    $keyRowTerm = ($eventTypeTerm) value.get(0);
           |    $nullTerm = false;
           |    break;
           |  }
           |}
           """.stripMargin

        val exp = GeneratedExpression(keyRowTerm, nullTerm, keyCode, input)
        reusableInputUnboxingExprs((keyRowTerm, 0)) = exp
        exp
    }
    exp.copy(code = NO_CODE)
  }

  /**
    * Extracts partition keys from any element of the match
    *
    * @param partitionKey partition key to be extracted
    * @return generated code for the given key
    */
  private def generatePartitionKeyAccess(
      partitionKey: RexInputRef)
    : GeneratedExpression = {

    val keyRow = generateKeyRow()
    generateFieldAccess(keyRow, partitionKey.getIndex)
  }

  def generateOneRowPerMatchExpression(
      partitionKeys: util.List[RexNode],
      measures: util.Map[String, RexNode],
      returnType: RowSchema)
    : GeneratedExpression = {
    // For "ONE ROW PER MATCH", the output columns include:
    // 1) the partition columns;
    // 2) the columns defined in the measures clause.
    val resultExprs =
      partitionKeys.asScala.map { case inputRef: RexInputRef =>
        generatePartitionKeyAccess(inputRef)
      } ++ returnType.fieldNames.filter(measures.containsKey(_)).map { fieldName =>
        generateExpression(measures.get(fieldName))
      }

    val exp = generateResultExpression(
      resultExprs,
      returnType.typeInfo,
      returnType.fieldNames)
    aggregatesPerVariable.values.foreach(_.generateAggFunction())
    if (hasCodeSplits) {
      makeReusableInSplits(reusableAggregationExpr.values)
    }

    val resultCode =
      j"""
         |${exp.code}
         |$collectorTerm.collect(${exp.resultTerm});
         |""".stripMargin
    exp.copy(code = resultCode)
  }

  def generateAllRowsPerMatchExpression(
      partitionKeys: java.util.List[RexNode],
      orderKeys: util.List[RelFieldCollation],
      measures: java.util.Map[String, RexNode],
      returnSchema: RowSchema): GeneratedExpression = {

    val patternNameTerm = newName("patternName")
    val eventNameTerm = newName("event")
    val eventNameListTerm = newName("eventList")
    val listTypeTerm = classOf[java.util.List[_]].getCanonicalName
    val eventTypeTerm = boxedTypeTermForTypeInfo(input)

    setAllRows(allRows = true)
    reusableMemberStatements.add(s"$eventTypeTerm $eventNameTerm;")

    // For "ALL ROWS PER MATCH", the output columns include:
    // 1) the partition columns;
    // 2) the ordering columns;
    // 3) the columns defined in the measures clause;
    // 4) any remaining columns defined of the input.
    val fieldsAccessed = mutable.Set[Int]()
    val resultExprs =
      partitionKeys.asScala.map { case inputRef: RexInputRef =>
        fieldsAccessed += inputRef.getIndex
        generateFieldAccess(input, eventNameTerm, inputRef.getIndex)
      } ++ orderKeys.asScala.map { fieldCollation =>
        fieldsAccessed += fieldCollation.getFieldIndex
        generateFieldAccess(input, eventNameTerm, fieldCollation.getFieldIndex)
      } ++ (0 until input.getArity).filterNot(fieldsAccessed.contains).map { idx =>
        generateFieldAccess(input, eventNameTerm, idx)
      } ++ returnSchema.fieldNames.filter(measures.containsKey(_)).map { fieldName =>
        generateExpression(measures.get(fieldName))
      }

    val expr = generateResultExpression(
      resultExprs,
      returnSchema.typeInfo,
      returnSchema.fieldNames)

    aggregatesPerVariable.values.foreach(_.generateAggFunction())
    if (hasCodeSplits) {
      makeReusableInSplits(reusableAggregationExpr.filterNot(_._1._1).values)
      makeReusableInSplitsForRunningInputs(reusableAggregationExpr.filter(_._1._1).values)
      makeReusableInSplitsForRunningInputs(reusableRunningInputUnboxingExprs.values)
    }

    val resultCode = {
      addReusablePatternNames()

      def fillRunningPatternLists(): String = {
        // For RUNNING semantics, the events mapped to a pattern reference variable are
        // different depending on the current processed row of the matching rows
        reusablePatternLists.filter(_._2.running).map { patternList =>
          val patternName = patternList._1._2
          val listName = patternList._2.resultTerm
          if (patternName == ALL_PATTERN_VARIABLE) {
            j"""
               |$listName.add($eventNameTerm);
               |""".stripMargin
          } else {
            val escapedPatternName = EncodingUtils.escapeJava(patternName)

            j"""
               |if ($patternNameTerm.equals("$escapedPatternName")) {
               |  $listName.add($eventNameTerm);
               |}
               |""".stripMargin
          }
        }.mkString("\n")
      }

      j"""
         |for (String $patternNameTerm : $patternNamesTerm) {
         |  $listTypeTerm $eventNameListTerm = ($listTypeTerm) $input1Term.get($patternNameTerm);
         |  if ($eventNameListTerm != null) {
         |    for ($eventTypeTerm event : $eventNameListTerm) {
         |      $eventNameTerm = event;
         |      ${fillRunningPatternLists()}
         |      ${reuseRunningInputUnboxingCode()}
         |      ${reuseRunningPerRecordCode()}
         |      ${expr.code}
         |      $collectorTerm.collect(${expr.resultTerm});
         |    }
         |  }
         |}
         |""".stripMargin
    }

    expr.copy(code = resultCode)
  }

  def generateCondition(call: RexNode): GeneratedExpression = {
    val exp = call.accept(this)
    aggregatesPerVariable.values.foreach(_.generateAggFunction())
    if (hasCodeSplits) {
      makeReusableInSplits(reusableAggregationExpr.values)
    }

    exp
  }

  override def visitCall(call: RexCall): GeneratedExpression = {
    call.getOperator match {
      case PREV | NEXT =>
        val countLiteral = call.operands.get(1).asInstanceOf[RexLiteral]
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
        val countLiteral = call.operands.get(1).asInstanceOf[RexLiteral]
        val offset = checkedDownCast(countLiteral.getValueAs(classOf[JLong]))
        updateOffsets(call.getOperator == FIRST, offset)
        val patternExp = call.operands.get(0).accept(this)
        resetOffsets()
        patternExp

      case RUNNING =>
        if (allRows) {
          setRunning(true)
          val expr = call.operands.get(0).accept(this)
          resetRunning()
          expr
        } else {
          // in ONE ROW PER MATCH, RUNNING has the same semantics as FINAL
          call.getOperands.get(0).accept(this)
        }

      case FINAL =>
        call.getOperands.get(0).accept(this)

      case _ : SqlAggFunction =>

        val variable = call.accept(new AggregationPatternVariableFinder)
          .getOrElse(throw new TableException("No pattern variable specified in aggregate"))

        val matchAgg = aggregatesPerVariable.get((running, variable)) match {
          case Some(agg) => agg
          case None =>
            val agg = new AggBuilder(variable)
            aggregatesPerVariable((running, variable)) = agg
            agg
        }

        matchAgg.generateDeduplicatedAggAccess(call)

      case _ => super.visitCall(call)
    }
  }

  override private[flink] def generateProctimeTimestamp() = {
    val resultTerm = newName("result")

    //TODO use timerService once it is available in PatternFlatSelectFunction
    val resultCode =
      j"""
         |long $resultTerm = System.currentTimeMillis();
         |""".stripMargin
    GeneratedExpression(resultTerm, NEVER_NULL, resultCode, SqlTimeTypeInfo.TIMESTAMP)
  }

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression = {
    if (isWithinAggExprState) {
      generateFieldAccess(input, inputAggRowTerm, fieldRef.getIndex)
    } else {
      if (fieldRef.getAlpha.equals(ALL_PATTERN_VARIABLE) &&
          currentPattern.isDefined && offset == 0 && !first) {
        generateInputAccess(input, input1Term, fieldRef.getIndex)
      } else {
        generatePatternFieldRef(fieldRef)
      }
    }
  }

  private def generateDefinePatternVariableExp(
      patternName: String,
      currentPattern: String)
    : GeneratedPatternList = {
    val listName = newName("patternEvents")
    val eventTypeTerm = boxedTypeTermForTypeInfo(input)
    val eventNameTerm = newName("event")

    val addCurrent = if (currentPattern == patternName || patternName == ALL_PATTERN_VARIABLE) {
      j"""
         |$listName.add($input1Term);
         """.stripMargin
    } else {
      ""
    }
    val listCode = if (patternName == ALL_PATTERN_VARIABLE) {
      addReusablePatternNames()
      val patternTerm = newName("pattern")
      j"""
         |java.util.List $listName = new java.util.ArrayList();
         |for (String $patternTerm : $patternNamesTerm) {
         |  for ($eventTypeTerm $eventNameTerm :
         |  $contextTerm.getEventsForPattern($patternTerm)) {
         |    $listName.add($eventNameTerm);
         |  }
         |}
         """.stripMargin
    } else {
      val escapedPatternName = EncodingUtils.escapeJava(patternName)
      j"""
         |java.util.List $listName = new java.util.ArrayList();
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
       """.stripMargin

    GeneratedPatternList(listName, code)
  }

  private def generateMeasurePatternVariableExp(patternName: String): GeneratedPatternList = {
    val listName = newName("patternEvents")

    val code = if (running) {
      // For RUNNING semantics, the pattern lists will be calculated
      // for each of the matching events.
      j"""
         |java.util.List $listName = new java.util.ArrayList();
         |""".stripMargin
    } else if (patternName == ALL_PATTERN_VARIABLE) {
      addReusablePatternNames()

      val patternTerm = newName("pattern")

      j"""
         |java.util.List $listName = new java.util.ArrayList();
         |for (String $patternTerm : $patternNamesTerm) {
         |  java.util.List rows = (java.util.List) $input1Term.get($patternTerm);
         |  if (rows != null) {
         |    $listName.addAll(rows);
         |  }
         |}
         """.stripMargin
    } else {
      val escapedPatternName = EncodingUtils.escapeJava(patternName)
      j"""
         |java.util.List $listName = (java.util.List) $input1Term.get("$escapedPatternName");
         |if ($listName == null) {
         |  $listName = java.util.Collections.emptyList();
         |}
         |""".stripMargin
    }

    GeneratedPatternList(listName, code, running)
  }

  private def findEventByLogicalPosition(
      patternFieldAlpha: String)
    : GeneratedExpression = {
    val rowNameTerm = newName("row")
    val eventTypeTerm = boxedTypeTermForTypeInfo(input)
    val isRowNull = newName("isRowNull")

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

    GeneratedExpression(rowNameTerm, isRowNull, funcCode, input)
  }

  private def findEventsByPatternName(
      patternFieldAlpha: String)
    : GeneratedPatternList = {
    reusablePatternLists.get((running, patternFieldAlpha)) match {
      case Some(expr) =>
        expr

      case None =>
        val exp = currentPattern match {
          case Some(p) => generateDefinePatternVariableExp(patternFieldAlpha, p)
          case None => generateMeasurePatternVariableExp(patternFieldAlpha)
        }
        reusablePatternLists((running, patternFieldAlpha)) = exp
        exp
    }
  }

  private def generatePatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression = {
    val escapedAlpha = EncodingUtils.escapeJava(fieldRef.getAlpha)
    val patternVariableRef = getReusableInputUnboxingExprs(s"$escapedAlpha#$first", offset) match {
      case Some(expr) =>
        expr

      case None =>
        val exp = findEventByLogicalPosition(fieldRef.getAlpha)
        addReusableInputUnboxingExprs(s"$escapedAlpha#$first", offset, exp)
        exp
    }

    generateFieldAccess(patternVariableRef.copy(code = NO_CODE), fieldRef.getIndex)
  }

  class AggBuilder(variable: String) {

    private val aggregates = new mutable.ListBuffer[RexCall]()

    private val variableUID = newName("variable")

    private val rowTypeTerm = "org.apache.flink.types.Row"

    private val calculateAggFuncName = s"calculateAgg_$variableUID"

    def generateDeduplicatedAggAccess(aggCall: RexCall): GeneratedExpression = {
      reusableAggregationExpr.get((running, aggCall.toString)) match  {
        case Some(expr) =>
          expr

        case None =>
          val exp: GeneratedExpression = generateAggAccess(aggCall)
          aggregates += aggCall
          reusableAggregationExpr((running, aggCall.toString)) = exp
          addReusablePerRecordStatement(exp.code)
          exp.copy(code = NO_CODE)
      }
    }

    private def generateAggAccess(aggCall: RexCall): GeneratedExpression = {
      val singleAggResultTerm = newName("result")
      val singleAggNullTerm = newName("nullTerm")
      val singleAggResultType = FlinkTypeFactory.toTypeInfo(aggCall.`type`)
      val primitiveSingleAggResultTypeTerm = primitiveTypeTermForTypeInfo(singleAggResultType)
      val boxedSingleAggResultTypeTerm = boxedTypeTermForTypeInfo(singleAggResultType)

      val allAggRowTerm = s"aggRow_$variableUID"

      val rowsForVariableCode = findEventsByPatternName(variable)
      val codeForAgg =
        j"""
           |$rowTypeTerm $allAggRowTerm = $calculateAggFuncName(${rowsForVariableCode.resultTerm});
           |""".stripMargin

      addReusablePerRecordStatement(codeForAgg)

      val defaultValue = primitiveDefaultValue(singleAggResultType)
      val codeForSingleAgg = if (nullCheck) {
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

      addReusablePerRecordStatement(codeForSingleAgg)

      GeneratedExpression(singleAggResultTerm, singleAggNullTerm, NO_CODE, singleAggResultType)
    }

    def generateAggFunction(): Unit = {
      val matchAgg = extractAggregatesAndExpressions

      val aggGenerator = new AggregationCodeGenerator(config, false, input, None)

      val aggFunc = aggGenerator.generateAggregations(
        s"AggFunction_$variableUID",
        matchAgg.inputExprs.map(r => FlinkTypeFactory.toTypeInfo(r.getType)),
        matchAgg.aggregations.map(_.aggFunction).toArray,
        matchAgg.aggregations.map(_.inputIndices).toArray,
        matchAgg.aggregations.indices.toArray,
        Array.fill(matchAgg.aggregations.size)(false),
        isStateBackedDataViews = false,
        partialResults = false,
        Array.emptyIntArray,
        None,
        matchAgg.aggregations.size,
        needRetract = false,
        needMerge = false,
        needReset = false,
        None
      )

      reusableMemberStatements.add(aggFunc.code)

      val transformFuncName = s"transformRowForAgg_$variableUID"
      val inputTransform: String = generateAggInputExprEvaluation(
        matchAgg.inputExprs,
        transformFuncName)

      generateAggCalculation(aggFunc, transformFuncName, inputTransform)
    }

    private def extractAggregatesAndExpressions: MatchAgg = {
      val inputRows = new mutable.LinkedHashMap[String, (RexNode, Int)]

      val logicalAggregates = aggregates.map(aggCall => {
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

        val agg = aggCall.getOperator.asInstanceOf[SqlAggFunction]
        LogicalSingleAggCall(agg,
          callsWithIndices.map(_._1.getType),
          callsWithIndices.map(_._2).toArray)
      })

      val aggs = logicalAggregates.zipWithIndex.map {
        case (agg, index) =>
          val result = AggregateUtil.extractAggregateCallMetadata(
            agg.function,
            isDistinct = false, // TODO properly set once supported in Calcite
            agg.inputTypes,
            needRetraction = false,
            config,
            isStateBackedDataViews = false,
            index)

          SingleAggCall(result.aggregateFunction, agg.exprIndices.toArray, result.accumulatorSpecs)
      }

      MatchAgg(aggs, inputRows.values.map(_._1).toSeq)
    }

    private def generateAggCalculation(
        aggFunc: GeneratedAggregationsFunction,
        transformFuncName: String,
        inputTransformFunc: String)
      : Unit = {
      val aggregatorTerm = s"aggregator_$variableUID"
      val code =
        j"""
           |private final ${aggFunc.name} $aggregatorTerm;
           |
           |$inputTransformFunc
           |
           |private $rowTypeTerm $calculateAggFuncName(java.util.List input)
           |    throws Exception {
           |  $rowTypeTerm accumulator = $aggregatorTerm.createAccumulators();
           |  for ($rowTypeTerm row : input) {
           |    $aggregatorTerm.accumulate(accumulator, $transformFuncName(row));
           |  }
           |  $rowTypeTerm result = $aggregatorTerm.createOutputRow();
           |  $aggregatorTerm.setAggregationResults(accumulator, result);
           |  return result;
           |}
         """.stripMargin

      reusableInitStatements.add(s"$aggregatorTerm = new ${aggFunc.name}();")
      reusableOpenStatements.add(s"$aggregatorTerm.open(getRuntimeContext());")
      reusableCloseStatements.add(s"$aggregatorTerm.close();")
      reusableMemberStatements.add(code)
    }

    private def generateAggInputExprEvaluation(
        inputExprs: Seq[RexNode],
        funcName: String)
      : String = {
      isWithinAggExprState = true
      val resultTerm = newName("result")
      val exprs = inputExprs.zipWithIndex.map {
        case (inputExpr, outputIndex) => {
          val expr = generateExpression(inputExpr)
          s"""
             |${expr.code}
             |if (${expr.nullTerm}) {
             |  $resultTerm.setField($outputIndex, null);
             |} else {
             |  $resultTerm.setField($outputIndex, ${expr.resultTerm});
             |}
         """.stripMargin
        }
      }.mkString("\n")
      isWithinAggExprState = false

      j"""
         |private $rowTypeTerm $funcName($rowTypeTerm $inputAggRowTerm) {
         |  $rowTypeTerm $resultTerm = new $rowTypeTerm(${inputExprs.size});
         |  $exprs
         |  return $resultTerm;
         |}
       """.stripMargin
    }

    private case class LogicalSingleAggCall(
      function: SqlAggFunction,
      inputTypes: Seq[RelDataType],
      exprIndices: Seq[Int]
    )

    private case class SingleAggCall(
      aggFunction: TableAggregateFunction[_, _],
      inputIndices: Array[Int],
      dataViews: Seq[DataViewSpec[_]]
    )

    private case class MatchAgg(
      aggregations: Seq[SingleAggCall],
      inputExprs: Seq[RexNode]
    )

  }

}
