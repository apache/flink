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

import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForTypeInfo, newName}
import org.apache.flink.table.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.util.Collector
import org.apache.flink.util.MathUtils.checkedDownCast

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A code generator for generating CEP related functions.
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

  private case class GeneratedPatternList(resultTerm: String, code: String)

  /**
    * Used to assign unique names for list of events per pattern variable name. Those lists
    * are treated as inputs and are needed by input access code.
    */
  private val reusablePatternLists: mutable.HashMap[String, GeneratedPatternList] = mutable
    .HashMap[String, GeneratedPatternList]()

  /**
    * Context information used by Pattern reference variable to index rows mapped to it.
    * Indexes element at offset either from beginning or the end based on the value of first.
    */
  private var offset: Int = 0
  private var first : Boolean = false

  /** Term for row for key extraction */
  private val keyRowTerm = newName("keyRow")

  /** Term for list of all pattern names */
  private val patternNamesTerm = newName("patternNames")

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

  private def addReusablePatternNames() : Unit = {
    reusableMemberStatements
      .add(s"private String[] $patternNamesTerm = new String[] { ${
        patternNames.map(p => s""""${EncodingUtils.escapeJava(p)}"""").mkString(", ")
      } };")
  }

  /**
    * Generates a [[org.apache.flink.api.common.functions.Function]] that can be passed to Java
    * compiler.
    *
    * This is a separate method from [[FunctionCodeGenerator.generateFunction()]] because as of
    * now functions in CEP library do not support rich interfaces
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
    val (functionClass, signature, inputStatements, isInterface) =
      if (clazz == classOf[IterativeCondition[_]]) {
        val baseClass = classOf[IterativeCondition[_]]
        val inputTypeTerm = boxedTypeTermForTypeInfo(input)
        val contextType = classOf[IterativeCondition.Context[_]].getCanonicalName

        (baseClass,
          s"boolean filter(Object _in1, $contextType $contextTerm)",
          List(s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;"),
          false)
      } else if (clazz == classOf[PatternSelectFunction[_, _]]) {
        val baseClass = classOf[PatternSelectFunction[_, _]]
        val inputTypeTerm =
          s"java.util.Map<String, java.util.List<${boxedTypeTermForTypeInfo(input)}>>"

        (baseClass,
          s"Object select($inputTypeTerm $input1Term)",
          List(),
          true)
      } else if (clazz == classOf[PatternFlatSelectFunction[_, _]]) {
        val baseClass = classOf[PatternFlatSelectFunction[_, _]]
        val inputTypeTerm =
          s"java.util.Map<String, java.util.List<${boxedTypeTermForTypeInfo(input)}>>"

        (baseClass,
          s"void flatSelect($inputTypeTerm $input1Term, $collectorTypeTerm $collectorTerm)",
          List(),
          true)
      } else {
        throw new CodeGenException("Unsupported Function.")
      }

    if (!reuseOpenCode().trim.isEmpty || !reuseCloseCode().trim.isEmpty) {
      throw new TableException(
        "Match recognize does not support UDFs, nor other functions that require " +
          "open/close methods yet.")
    }

    val extendsKeyword = if (isInterface) "implements" else "extends"
    val funcCode = j"""
      |public class $funcName $extendsKeyword ${functionClass.getCanonicalName} {
      |
      |  ${reuseMemberCode()}
      |
      |  public $funcName() throws Exception {
      |    ${reuseInitCode()}
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
      |}
    """.stripMargin

    GeneratedFunction(funcName, returnType, funcCode)
  }

  private def generateKeyRow() : GeneratedExpression = {
    val exp = reusableInputUnboxingExprs
      .get((keyRowTerm, 0)) match {
      // input access and unboxing has already been generated
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

    generateResultExpression(
      resultExprs,
      returnType.typeInfo,
      returnType.fieldNames)
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

      case FINAL =>
        call.getOperands.get(0).accept(this)

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
    if (fieldRef.getAlpha.equals("*") && currentPattern.isDefined && offset == 0 && !first) {
      generateInputAccess(input, input1Term, fieldRef.getIndex)
    } else {
      generatePatternFieldRef(fieldRef)
    }
  }

  private def generateDefinePatternVariableExp(
      patternName: String,
      currentPattern: String)
    : GeneratedPatternList = {
    val listName = newName("patternEvents")
    val eventTypeTerm = boxedTypeTermForTypeInfo(input)
    val eventNameTerm = newName("event")

    val addCurrent = if (currentPattern == patternName || patternName == "*") {
      j"""
         |$listName.add($input1Term);
         """.stripMargin
    } else {
      ""
    }
    val listCode = if (patternName == "*") {
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

    val code = if (patternName == "*") {
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

    GeneratedPatternList(listName, code)
  }

  private def findEventByLogicalPosition(
      patternFieldAlpha: String)
    : GeneratedExpression = {
    val rowNameTerm = newName("row")
    val eventTypeTerm = boxedTypeTermForTypeInfo(input)
    val isRowNull = newName("isRowNull")

    val findEventsByPatternName = reusablePatternLists.get(patternFieldAlpha) match {
      // input access and unboxing has already been generated
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

    val listName = findEventsByPatternName.resultTerm
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

  private def generatePatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression = {
    val escapedAlpha = EncodingUtils.escapeJava(fieldRef.getAlpha)
    val patternVariableRef = reusableInputUnboxingExprs
      .get((s"$escapedAlpha#$first", offset)) match {
      // input access and unboxing has already been generated
      case Some(expr) =>
        expr

      case None =>
        val exp = findEventByLogicalPosition(fieldRef.getAlpha)
        reusableInputUnboxingExprs((s"$escapedAlpha#$first", offset)) = exp
        exp
    }

    generateFieldAccess(patternVariableRef.copy(code = NO_CODE), fieldRef.getIndex)
  }
}
