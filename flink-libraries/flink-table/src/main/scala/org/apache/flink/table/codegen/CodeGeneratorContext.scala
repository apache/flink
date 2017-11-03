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

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.flink.api.common.functions.Function
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForTypeInfo, newName}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.{FunctionContext, UserDefinedFunction}

import scala.collection.mutable

/**
 * The context for code generator, maintaining various reusable statements that could be insert
 * into different code sections in the final generated class.
 */
class CodeGeneratorContext {

  // set of member statements that will be added only once
  private val reusableMemberStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of constructor statements that will be added only once
  private val reusableInitStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of statements that will be added only once per record
  private val reusablePerRecordStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of open statements for RichFunction that will be added only once
  private val reusableOpenStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of close statements for RichFunction that will be added only once
  private val reusableCloseStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // map of initial input unboxing expressions that will be added only once
  // (inputTerm, index) -> expr
  private val reusableInputUnboxingExprs: mutable.Map[(String, Int), GeneratedExpression] =
    mutable.Map[(String, Int), GeneratedExpression]()

  // set of constructor statements that will be added only once
  private val reusableConstructorStatements: mutable.LinkedHashSet[(String, String)] =
    mutable.LinkedHashSet[(String, String)]()

  /**
   * @return code block of statements that need to be placed in the member area of the class
   *         (e.g. member variables and their initialization)
   */
  def reuseMemberCode(): String = {
    reusableMemberStatements.mkString("")
  }

  /**
   * @return code block of statements that need to be placed in the constructor
   */
  def reuseInitCode(): String = {
    reusableInitStatements.mkString("")
  }

  /**
   * @return code block of statements that need to be placed in the per recode process block
   *         (e.g. Function or StreamOperator's processElement)
   */
  def reusePerRecordCode(): String = {
    reusablePerRecordStatements.mkString("")
  }

  /**
   * @return code block of statements that need to be placed in the open() method
   *         (e.g. RichFunction or StreamOperator)
   */
  def reuseOpenCode(): String = {
    reusableOpenStatements.mkString("")
  }

  /**
   * @return code block of statements that need to be placed in the close() method
   *         (e.g. RichFunction or StreamOperator)
   */
  def reuseCloseCode(): String = {
    reusableCloseStatements.mkString("")
  }

  /**
   * @return code block of statements that unbox input variables to a primitive variable
   *         and a corresponding null flag variable
   */
  def reuseInputUnboxingCode(): String = {
    reusableInputUnboxingExprs.values.map(_.code).mkString("")
  }

  /**
   * @return code block of constructor statements
   */
  def reuseConstructorCode(className: String): String = {
    reusableConstructorStatements.map { case (params, body) =>
      s"""
         |public $className($params) throws Exception {
         |  this();
         |  $body
         |}
         |""".stripMargin
    }.mkString("", "\n", "\n")
  }

  def addReusableMember(declareCode: String, initCode: String = ""): Unit = {
    reusableMemberStatements.add(declareCode)
    if (!initCode.isEmpty) reusableInitStatements.add(initCode)
  }

  def addPerRecordStatement(s: String): Unit = reusablePerRecordStatements.add(s)

  def addReusableOpenStatement(s: String): Unit = reusableOpenStatements.add(s)

  def addReusableCloseStatement(s: String): Unit = reusableCloseStatements.add(s)

  def getReusableInputUnboxingExprs(inputTerm: String, index: Int): Option[GeneratedExpression] =
    reusableInputUnboxingExprs.get((inputTerm, index))

  def addReusableInputUnboxingExprs(
      inputTerm: String,
      index: Int,
      expr: GeneratedExpression): Unit =
    reusableInputUnboxingExprs((inputTerm, index)) = expr

  /**
   * Adds a reusable output record to the member area of the generated class.
   * The passed [[TypeInformation]] defines the type class to be instantiated.
   */
  def addReusableOutRecord(ti: TypeInformation[_], outRecordTerm: String): Unit = {
    val statement = ti match {
      case rt: RowTypeInfo =>
        s"""
           |final ${ti.getTypeClass.getCanonicalName} $outRecordTerm =
           |    new ${ti.getTypeClass.getCanonicalName}(${rt.getArity});
           |""".stripMargin
      case _ =>
        s"""
           |final ${ti.getTypeClass.getCanonicalName} $outRecordTerm =
           |    new ${ti.getTypeClass.getCanonicalName}();
           |""".stripMargin
    }
    reusableMemberStatements.add(statement)
  }

  /**
   * Adds a reusable [[java.math.BigDecimal]] to the member area of the generated class.
   *
   * @param decimal decimal object to be instantiated during runtime
   * @return member variable term
   */
  def addReusableDecimal(decimal: JBigDecimal): String = decimal match {
    case JBigDecimal.ZERO => "java.math.BigDecimal.ZERO"
    case JBigDecimal.ONE => "java.math.BigDecimal.ONE"
    case JBigDecimal.TEN => "java.math.BigDecimal.TEN"
    case _ =>
      val fieldTerm = newName("decimal")
      val fieldDecimal =
        s"""
           |transient java.math.BigDecimal $fieldTerm =
           |    new java.math.BigDecimal("${decimal.toString}");
           |""".stripMargin
      reusableMemberStatements.add(fieldDecimal)
      fieldTerm
  }

  /**
   * Adds a reusable [[java.lang.reflect.Field]] to the member area of the generated class.
   * The field can be used for accessing POJO fields more efficiently during runtime, however,
   * the field does not have to be public.
   */
  def addReusablePrivateFieldAccess(clazz: Class[_], fieldName: String): String = {
    val fieldTerm = s"field_${clazz.getCanonicalName.replace('.', '$')}_$fieldName"
    val fieldExtraction =
      s"""
         |final java.lang.reflect.Field $fieldTerm =
         |  org.apache.flink.api.java.typeutils.TypeExtractor.getDeclaredField(
         |    ${clazz.getCanonicalName}.class, "$fieldName");
         |""".stripMargin
    val fieldAccessibility =
      s"""
         |$fieldTerm.setAccessible(true);
         |""".stripMargin

    addReusableMember(fieldExtraction, fieldAccessibility)
    fieldTerm
  }

  /**
   * Adds a reusable [[java.util.HashSet]] to the member area of the generated class.
   */
  def addReusableSet(elements: Seq[GeneratedExpression]): String = {
    val fieldTerm = newName("set")
    val field =
      s"""
         |final java.util.Set $fieldTerm;
         |""".stripMargin
    val init =
      s"""
         |$fieldTerm = new java.util.HashSet();
         |""".stripMargin

    addReusableMember(field, init)

    elements.foreach { element =>
      val content =
        s"""
           |${element.code}
           |if (${element.nullTerm}) {
           |  $fieldTerm.add(null);
           |} else {
           |  $fieldTerm.add(${element.resultTerm});
           |}
           |""".stripMargin
      reusableInitStatements.add(content)
    }

    fieldTerm
  }

  /**
   * Adds a reusable array to the member area of the generated class.
   */
  def addReusableArray(clazz: Class[_], size: Int): String = {
    val fieldTerm = newName("array")
    val classQualifier = clazz.getCanonicalName // works also for int[] etc.
    val initArray = classQualifier.replaceFirst("\\[", s"[$size")
    val fieldArray =
      s"""
         |final $classQualifier $fieldTerm = new $initArray;
         |""".stripMargin
    reusableMemberStatements.add(fieldArray)
    fieldTerm
  }

  /**
   * Adds a reusable timestamp to the beginning of the SAM of the generated class.
   */
  def addReusableTimestamp(): String = {
    val fieldTerm = s"timestamp"
    val field =
      s"""
         |final long $fieldTerm = java.lang.System.currentTimeMillis();
         |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
   * Adds a reusable local timestamp to the beginning of the SAM of the generated class.
   */
  def addReusableLocalTimestamp(): String = {
    val fieldTerm = s"localtimestamp"
    val timestamp = addReusableTimestamp()
    val field =
      s"""
         |final long $fieldTerm = $timestamp + java.util.TimeZone.getDefault().getOffset(timestamp);
         |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
   * Adds a reusable time to the beginning of the SAM of the generated class.
   */
  def addReusableTime(): String = {
    val fieldTerm = s"time"
    val timestamp = addReusableTimestamp()
    // adopted from org.apache.calcite.runtime.SqlFunctions.currentTime()
    val field =
      s"""
         |final int $fieldTerm = (int) ($timestamp % ${DateTimeUtils.MILLIS_PER_DAY});
         |if (time < 0) {
         |  time += ${DateTimeUtils.MILLIS_PER_DAY};
         |}
         |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
   * Adds a reusable local time to the beginning of the SAM of the generated class.
   */
  def addReusableLocalTime(): String = {
    val fieldTerm = s"localtime"
    val localtimestamp = addReusableLocalTimestamp()
    // adopted from org.apache.calcite.runtime.SqlFunctions.localTime()
    val field =
      s"""
         |final int $fieldTerm = (int) ($localtimestamp % ${DateTimeUtils.MILLIS_PER_DAY});
         |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
   * Adds a reusable date to the beginning of the SAM of the generated class.
   */
  def addReusableDate(): String = {
    val fieldTerm = s"date"
    val timestamp = addReusableTimestamp()
    val time = addReusableTime()

    // adopted from org.apache.calcite.runtime.SqlFunctions.currentDate()
    val field =
      s"""
         |final int $fieldTerm = (int) ($timestamp / ${DateTimeUtils.MILLIS_PER_DAY});
         |if ($time < 0) {
         |  $fieldTerm -= 1;
         |}
         |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
   * Adds a reusable DateFormatter to the member area of the generated [[Function]].
   */
  def addReusableDateFormatter(format: GeneratedExpression): String = {
    val fieldTerm = newName("dateFormatter")

    val field =
      s"""
         |final org.joda.time.format.DateTimeFormatter $fieldTerm;
         |""".stripMargin

    val fieldInit =
      s"""
         |${format.code}
         |$fieldTerm = org.apache.flink.table.runtime.functions.
         |DateTimeFunctions$$.MODULE$$.createDateTimeFormatter(${format.resultTerm});
         |""".stripMargin

    addReusableMember(field, fieldInit)
    fieldTerm
  }

  /**
   * Adds a reusable [[java.util.Random]] to the member area of the generated class.
   *
   * The seed parameter must be a literal/constant expression.
   *
   * @return member variable term
   */
  def addReusableRandom(seedExpr: Option[GeneratedExpression], nullCheck: Boolean): String = {
    val fieldTerm = newName("random")

    val field =
      s"""
         |final java.util.Random $fieldTerm;
         |""".stripMargin

    val fieldInit = seedExpr match {
      case Some(s) if nullCheck =>
        s"""
           |${s.code}
           |if (!${s.nullTerm}) {
           |  $fieldTerm = new java.util.Random(${s.resultTerm});
           |}
           |else {
           |  $fieldTerm = new java.util.Random();
           |}
           |""".stripMargin
      case Some(s) =>
        s"""
           |${s.code}
           |$fieldTerm = new java.util.Random(${s.resultTerm});
           |""".stripMargin
      case _ =>
        s"""
           |$fieldTerm = new java.util.Random();
           |""".stripMargin
    }

    addReusableMember(field, fieldInit)
    fieldTerm
  }

  /**
   * Adds a reusable [[UserDefinedFunction]] to the member area of the generated class.
   */
  def addReusableFunction(function: UserDefinedFunction, contextTerm: String = null): String = {
    val classQualifier = function.getClass.getCanonicalName
    val functionSerializedData = UserDefinedFunctionUtils.serialize(function)
    val fieldTerm = s"function_${function.functionIdentifier}"

    val fieldFunction =
      s"""
         |final $classQualifier $fieldTerm;
         |""".stripMargin
    reusableMemberStatements.add(fieldFunction)

    val functionDeserialization =
      s"""
         |$fieldTerm = ($classQualifier)
         |${UserDefinedFunctionUtils.getClass.getName.stripSuffix("$")}
         |.deserialize("$functionSerializedData");
       """.stripMargin

    reusableInitStatements.add(functionDeserialization)

    val openFunction = if (contextTerm != null) {
      s"""
         |$fieldTerm.open(new ${classOf[FunctionContext].getCanonicalName}($contextTerm));
       """.stripMargin
    } else {
      s"""
         |$fieldTerm.open(new ${classOf[FunctionContext].getCanonicalName}(getRuntimeContext()));
       """.stripMargin
    }
    reusableOpenStatements.add(openFunction)

    val closeFunction =
      s"""
         |$fieldTerm.close();
       """.stripMargin
    reusableCloseStatements.add(closeFunction)

    fieldTerm
  }

  /**
   * Adds a reusable constructor statement with the given parameter types.
   */
  def addReusableConstructor(parameterTypes: Class[_]*): Array[String] = {
    val parameters = mutable.ListBuffer[String]()
    val fieldTerms = mutable.ListBuffer[String]()
    val body = mutable.ListBuffer[String]()

    parameterTypes.zipWithIndex.foreach { case (t, index) =>
      val classQualifier = t.getCanonicalName
      val fieldTerm = newName(s"instance_${classQualifier.replace('.', '$')}")
      val field = s"final $classQualifier $fieldTerm;"
      reusableMemberStatements.add(field)
      fieldTerms += fieldTerm
      parameters += s"$classQualifier arg$index"
      body += s"$fieldTerm = arg$index;"
    }

    reusableConstructorStatements.add((parameters.mkString(","), body.mkString("", "\n", "\n")))

    fieldTerms.toArray
  }

  /**
   * Adds a reusable constant to the member area of the generated class.
   */
  def addReusableBoxedConstant(constant: GeneratedExpression, nullCheck: Boolean): String = {
    require(constant.literal, "Literal expected")

    val fieldTerm = newName("constant")
    val boxed = CodeGenUtils.generateOutputFieldBoxing(constant, nullCheck)
    val boxedType = boxedTypeTermForTypeInfo(boxed.resultType)

    val field =
      s"""
         |final $boxedType $fieldTerm;
         |""".stripMargin

    val init =
      s"""
         |${boxed.code}
         |$fieldTerm = ${boxed.resultTerm};
         |""".stripMargin

    addReusableMember(field, init)
    fieldTerm
  }
}

object CodeGeneratorContext {

  def apply(): CodeGeneratorContext = new CodeGeneratorContext()

  val DEFAULT_INPUT1_TERM = "in1"

  val DEFAULT_INPUT2_TERM = "in2"

  val DEFAULT_COLLECTOR_TERM = "c"

  val DEFAULT_OUT_RECORD_TERM = "out"

  val DEFAULT_CONTEXT_TERM = "ctx"
}
