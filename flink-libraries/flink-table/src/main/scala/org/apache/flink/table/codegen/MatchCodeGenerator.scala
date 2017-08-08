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

import java.math.{BigDecimal => JBigDecimal}
import java.util

import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{CLASSIFIER, FINAL, FIRST, LAST, MATCH_NUMBER, NEXT, PREV, RUNNING}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction}
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForTypeInfo, newName, primitiveDefaultValue}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A code generator for generating CEP related functions.
  *
  * @param config configuration that determines runtime behavior
  * @param nullableInput input(s) can be null.
  * @param input type information about the first input of the Function
  * @param patternNames the names of patterns
  * @param generateCondition whether the code generator is generating [[IterativeCondition]]
  * @param patternName the name of current pattern
  */
class MatchCodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input: TypeInformation[_ <: Any],
    patternNames: Seq[String],
    generateCondition: Boolean,
    patternName: Option[String] = None)
  extends CodeGenerator(config, nullableInput, input){

  /**
    * @return term of pattern names
    */
  private val patternNameListTerm = newName("patternNameList")

  /**
    * @return term of current pattern which is processing
    */
  private val currPatternTerm = newName("currPattern")

  /**
    * @return term of current event which is processing
    */
  private val currEventTerm = newName("currEvent")

  private val buildPatternNameList: String = {
    for (patternName <- patternNames) yield
      s"""
        |$patternNameListTerm.add("$patternName");
        |""".stripMargin
  }.mkString("\n")

  def addReusableStatements(): Unit = {
    val eventTypeTerm = boxedTypeTermForTypeInfo(input)
    val memberStatement =
      s"""
        |$eventTypeTerm $currEventTerm = null;
        |String $currPatternTerm = null;
        |java.util.List<String> $patternNameListTerm = new java.util.ArrayList();
        |""".stripMargin
    addReusableMemberStatement(memberStatement)

    addReusableInitStatement(buildPatternNameList)
  }

  /**
    * Generates a [[IterativeCondition]] that can be passed to Java compiler.
    *
    * @param name Class name of the function. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the function
    * @return a GeneratedIterativeCondition
    */
  def generateIterativeCondition(
      name: String,
      bodyCode: String)
    : GeneratedIterativeCondition = {

    val funcName = newName(name)
    val inputTypeTerm = boxedTypeTermForTypeInfo(input)

    val funcCode = j"""
      public class $funcName
          extends ${classOf[IterativeCondition[_]].getCanonicalName} {

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        @Override
        public boolean filter(
          Object _in1, ${classOf[IterativeCondition.Context[_]].getCanonicalName} $contextTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${reusePerRecordCode()}
          ${reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin

    GeneratedIterativeCondition(funcName, funcCode)
  }

  /**
    * Generates a [[PatternSelectFunction]] that can be passed to Java compiler.
    *
    * @param name Class name of the function. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the function
    * @return a GeneratedPatternSelectFunction
    */
  def generatePatternSelectFunction(
      name: String,
      bodyCode: String)
    : GeneratedPatternSelectFunction = {

    val funcName = newName(name)
    val inputTypeTerm =
      classOf[java.util.Map[java.lang.String, java.util.List[Row]]].getCanonicalName

    val funcCode = j"""
      public class $funcName
          implements ${classOf[PatternSelectFunction[_, _]].getCanonicalName} {

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        @Override
        public Object select(java.util.Map<String, java.util.List<Object>> _in1)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${reusePerRecordCode()}
          ${reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin

    GeneratedPatternSelectFunction(funcName, funcCode)
  }

  /**
    * Generates a [[PatternFlatSelectFunction]] that can be passed to Java compiler.
    *
    * @param name Class name of the function. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the function
    * @return a GeneratedPatternFlatSelectFunction
    */
  def generatePatternFlatSelectFunction(
      name: String,
      bodyCode: String)
    : GeneratedPatternFlatSelectFunction = {

    val funcName = newName(name)
    val inputTypeTerm =
      classOf[java.util.Map[java.lang.String, java.util.List[Row]]].getCanonicalName

    val funcCode = j"""
      public class $funcName
          implements ${classOf[PatternFlatSelectFunction[_, _]].getCanonicalName} {

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        @Override
        public void flatSelect(java.util.Map<String, java.util.List<Object>> _in1,
            org.apache.flink.util.Collector $collectorTerm)
          throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${reusePerRecordCode()}
          ${reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin

    GeneratedPatternFlatSelectFunction(funcName, funcCode)
  }

  def generateSelectOutputExpression(
    partitionKeys: util.List[RexNode],
    measures: util.Map[String, RexNode],
    returnType: RowSchema
  ): GeneratedExpression = {

    val eventNameTerm = newName("event")
    val eventTypeTerm = boxedTypeTermForTypeInfo(input)

    // For "ONE ROW PER MATCH", the output columns include:
    // 1) the partition columns;
    // 2) the columns defined in the measures clause.
    val resultExprs =
      partitionKeys.asScala.map { case inputRef: RexInputRef =>
        generateFieldAccess(input, eventNameTerm, inputRef.getIndex)
      } ++ returnType.fieldNames.filter(measures.containsKey(_)).map { fieldName =>
        generateExpression(measures.get(fieldName))
      }

    val resultExpression = generateResultExpression(
      resultExprs,
      returnType.typeInfo,
      returnType.fieldNames)

    val resultCode =
      s"""
        |$eventTypeTerm $eventNameTerm = null;
        |if (${partitionKeys.size()} > 0) {
        |  for (java.util.Map.Entry entry : $input1Term.entrySet()) {
        |    java.util.List<Row> value = (java.util.List<Row>) entry.getValue();
        |    if (value != null && value.size() > 0) {
        |      $eventNameTerm = ($eventTypeTerm) value.get(0);
        |      break;
        |    }
        |  }
        |}
        |
        |${resultExpression.code}
        |""".stripMargin

    resultExpression.copy(code = resultCode)
  }

  def generateFlatSelectOutputExpression(
      partitionKeys: util.List[RexNode],
      orderKeys: RelCollation,
      measures: util.Map[String, RexNode],
      returnType: RowSchema)
    : GeneratedExpression = {

    val patternNameTerm = newName("patternName")
    val eventNameTerm = newName("event")
    val eventNameListTerm = newName("eventList")
    val eventTypeTerm = boxedTypeTermForTypeInfo(input)
    val listTypeTerm = classOf[java.util.List[_]].getCanonicalName

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
      } ++ orderKeys.getFieldCollations.asScala.map { fieldCollation =>
        fieldsAccessed += fieldCollation.getFieldIndex
        generateFieldAccess(input, eventNameTerm, fieldCollation.getFieldIndex)
      } ++ (0 until input.getArity).filterNot(fieldsAccessed.contains).map { idx =>
        generateFieldAccess(input, eventNameTerm, idx)
      } ++ returnType.fieldNames.filter(measures.containsKey(_)).map { fieldName =>
        generateExpression(measures.get(fieldName))
      }

    val resultExpression = generateResultExpression(
      resultExprs,
      returnType.typeInfo,
      returnType.fieldNames)

    val resultCode =
      s"""
        |for (String $patternNameTerm : $patternNameListTerm) {
        |  $currPatternTerm = $patternNameTerm;
        |  $listTypeTerm $eventNameListTerm = ($listTypeTerm) $input1Term.get($patternNameTerm);
        |  if ($eventNameListTerm != null) {
        |    for ($eventTypeTerm $eventNameTerm : $eventNameListTerm) {
        |      $currEventTerm = $eventNameTerm;
        |      ${resultExpression.code}
        |      $collectorTerm.collect(${resultExpression.resultTerm});
        |    }
        |  }
        |}
        |$currPatternTerm = null;
        |$currEventTerm = null;
        |""".stripMargin

    GeneratedExpression("", "false", resultCode, null)
  }

  override def visitCall(call: RexCall): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toTypeInfo(call.getType)
    call.getOperator match {
      case PREV =>
        val countLiteral = call.operands.get(1).asInstanceOf[RexLiteral]
        val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()
        generatePrev(
          call.operands.get(0),
          count,
          resultType)

      case NEXT | CLASSIFIER | MATCH_NUMBER =>
        throw new CodeGenException(s"Unsupported call: $call")

      case FIRST | LAST =>
        val countLiteral = call.operands.get(1).asInstanceOf[RexLiteral]
        val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()
        generateFirstLast(
          call.operands.get(0),
          count,
          resultType,
          running = true,
          call.getOperator == FIRST)

      case RUNNING | FINAL =>
        generateRunningFinal(
          call.operands.get(0),
          resultType,
          call.getOperator == RUNNING)

      case _ => super.visitCall(call)
    }
  }

  private def generatePrev(
      rexNode: RexNode,
      count: Int,
      resultType: TypeInformation[_])
    : GeneratedExpression = {
    rexNode match {
      case patternFieldRef: RexPatternFieldRef =>
        if (count == 0 && patternFieldRef.getAlpha == patternName.get) {
          // return current one
          return visitInputRef(patternFieldRef)
        }

        val listName = newName("patternEvents")
        val resultTerm = newName("result")
        val nullTerm = newName("isNull")
        val indexTerm = newName("eventIndex")
        val visitedEventNumberTerm = newName("visitedEventNumber")
        val eventTerm = newName("event")
        val resultTypeTerm = boxedTypeTermForTypeInfo(resultType)
        val defaultValue = primitiveDefaultValue(resultType)

        val eventTypeTerm = boxedTypeTermForTypeInfo(input)

        val patternNamesToVisit = patternNames
          .take(patternNames.indexOf(patternFieldRef.getAlpha) + 1)
          .reverse
        def findEventByPhysicalPosition: String = {
          val init: String =
            s"""
              |java.util.List $listName = new java.util.ArrayList();
              |""".stripMargin

          val getResult: String = {
            for (tmpPatternName <- patternNamesToVisit) yield
              s"""
                |for ($eventTypeTerm $eventTerm : $contextTerm
                |    .getEventsForPattern("$tmpPatternName")) {
                |  $listName.add($eventTerm);
                |}
                |
                |$indexTerm = $listName.size() - ($count - $visitedEventNumberTerm);
                |if ($indexTerm >= 0) {
                |  $resultTerm = ($resultTypeTerm) (($eventTypeTerm) $listName.get($indexTerm))
                |    .getField(${patternFieldRef.getIndex});
                |  $nullTerm = false;
                |  break;
                |}
                |
                |$visitedEventNumberTerm += $listName.size();
                |$listName.clear();
                |""".stripMargin
          }.mkString("\n")

          s"""
            |$init
            |$getResult
            |""".stripMargin
        }

        val resultCode =
          s"""
            |int $visitedEventNumberTerm = 0;
            |int $indexTerm;
            |$resultTypeTerm $resultTerm = $defaultValue;
            |boolean $nullTerm = true;
            |do {
            |  $findEventByPhysicalPosition
            |} while (false);
            |""".stripMargin

        GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)

      case rexCall: RexCall =>
        val operands = rexCall.operands.asScala.map {
          operand => generatePrev(
            operand,
            count,
            FlinkTypeFactory.toTypeInfo(operand.getType))
        }

        generateCallExpression(rexCall.getOperator, operands, resultType)

      case _ =>
        generateExpression(rexNode)
    }
  }

  private def generateFirstLast(
      rexNode: RexNode,
      count: Int,
      resultType: TypeInformation[_],
      running: Boolean,
      first: Boolean)
    : GeneratedExpression = {
    rexNode match {
      case patternFieldRef: RexPatternFieldRef =>

        val eventNameTerm = newName("event")
        val resultTerm = newName("result")
        val listName = newName("patternEvents")
        val nullTerm = newName("isNull")
        val patternNameTerm = newName("patternName")
        val eventNameListTerm = newName("eventNameList")
        val resultTypeTerm = boxedTypeTermForTypeInfo(resultType)
        val defaultValue = primitiveDefaultValue(resultType)

        val eventTypeTerm = boxedTypeTermForTypeInfo(input)
        val listTypeTerm = classOf[java.util.List[_]].getCanonicalName

        def findEventByLogicalPosition: String = {
          val init =
            s"""
              |java.util.List $listName = new java.util.ArrayList();
              |""".stripMargin

          val findEventsByPatterName = if (generateCondition) {
            s"""
              |for ($eventTypeTerm $eventNameTerm : $contextTerm
              |    .getEventsForPattern("${patternFieldRef.getAlpha}")) {
              |  $listName.add($eventNameTerm);
              |}
              |""".stripMargin
          } else {
            s"""
              |for (String $patternNameTerm : $patternNameListTerm) {
              |  if ($patternNameTerm.equals("${patternFieldRef.getAlpha}") ||
              |      ${patternFieldRef.getAlpha.equals("*")}) {
              |    boolean skipLoop = false;
              |    $listTypeTerm $eventNameListTerm =
              |      ($listTypeTerm) $input1Term.get($patternNameTerm);
              |    if ($eventNameListTerm != null) {
              |      for ($eventTypeTerm $eventNameTerm : $eventNameListTerm) {
              |        $listName.add($eventNameTerm);
              |        if ($running && $eventNameTerm == $currEventTerm) {
              |          skipLoop = true;
              |          break;
              |        }
              |      }
              |    }
              |
              |    if (skipLoop) {
              |      break;
              |    }
              |  }
              |
              |  if ($running && $patternNameTerm.equals($currPatternTerm)) {
              |    break;
              |  }
              |}
              |""".stripMargin
          }

          val getResult =
            s"""
              |if ($listName.size() > $count) {
              |  if ($first) {
              |    $resultTerm = ($resultTypeTerm) (($eventTypeTerm)
              |      $listName.get($count))
              |        .getField(${patternFieldRef.getIndex});
              |  } else {
              |    $resultTerm = ($resultTypeTerm) (($eventTypeTerm)
              |      $listName.get($listName.size() - $count - 1))
              |        .getField(${patternFieldRef.getIndex});
              |  }
              |  $nullTerm = false;
              |}
              |""".stripMargin

          s"""
            |$init
            |$findEventsByPatterName
            |$getResult
            |""".stripMargin
        }

        val resultCode =
          s"""
            |$resultTypeTerm $resultTerm = $defaultValue;
            |boolean $nullTerm = true;
            |$findEventByLogicalPosition
            |""".stripMargin

        GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)

      case rexCall: RexCall =>
        val operands = rexCall.operands.asScala.map {
          operand => generateFirstLast(
            operand,
            count,
            FlinkTypeFactory.toTypeInfo(operand.getType),
            running,
            first)
        }

        generateCallExpression(rexCall.getOperator, operands, resultType)

      case _ =>
        generateExpression(rexNode)
    }
  }

  private def generateRunningFinal(
      rexNode: RexNode,
      resultType: TypeInformation[_],
      running: Boolean)
    : GeneratedExpression = {
    rexNode match {
      case _: RexPatternFieldRef =>
        generateFirstLast(rexNode, 0, resultType, running, first = false)

      case rexCall: RexCall if rexCall.getOperator == FIRST || rexCall.getOperator == LAST =>
        val countLiteral = rexCall.operands.get(1).asInstanceOf[RexLiteral]
        val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()
        generateFirstLast(
          rexCall.operands.get(0),
          count,
          resultType,
          running,
          rexCall.getOperator == FIRST)

      case rexCall: RexCall =>
        val operands = rexCall.operands.asScala.map {
          operand => generateRunningFinal(
            operand,
            FlinkTypeFactory.toTypeInfo(operand.getType),
            running)
        }

        generateCallExpression(rexCall.getOperator, operands, resultType)

      case _ =>
        generateExpression(rexNode)
    }
  }
}
