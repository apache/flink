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

import org.apache.flink.api.common.functions.{Function, RuntimeContext}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.functions.{FunctionContext, UserDefinedFunction}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GenerateUtils.generateRecordStatement
import org.apache.flink.table.runtime.operators.TableStreamOperator
import org.apache.flink.table.runtime.types.InternalSerializers
import org.apache.flink.table.runtime.util.collections._
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical._
import org.apache.flink.util.InstantiationUtil
import org.apache.calcite.avatica.util.DateTimeUtils
import java.util.TimeZone

import org.apache.flink.table.data.conversion.DataStructureConverter

import scala.collection.mutable

/**
  * The context for code generator, maintaining various reusable statements that could be insert
  * into different code sections in the final generated class.
  */
class CodeGeneratorContext(val tableConfig: TableConfig) {

  // holding a list of objects that could be used passed into generated class
  val references: mutable.ArrayBuffer[AnyRef] = new mutable.ArrayBuffer[AnyRef]()

  // set of member statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableMemberStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of constructor statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableInitStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of open statements for RichFunction that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableOpenStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of close statements for RichFunction that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableCloseStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of statements for cleanup dataview that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableCleanupStatements = mutable.LinkedHashSet[String]()

  // set of statements that will be added only once per record;
  // code should only update member variables because local variables are not accessible if
  // the code needs to be split;
  // we use a LinkedHashSet to keep the insertion order
  private val reusablePerRecordStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // map of initial input unboxing expressions that will be added only once
  // (inputTerm, index) -> expr
  val reusableInputUnboxingExprs: mutable.Map[(String, Int), GeneratedExpression] =
    mutable.Map[(String, Int), GeneratedExpression]()

  // set of constructor statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableConstructorStatements: mutable.LinkedHashSet[(String, String)] =
    mutable.LinkedHashSet[(String, String)]()

  // set of inner class definition statements that will be added only once
  private val reusableInnerClassDefinitionStatements: mutable.Map[String, String] =
    mutable.Map[String, String]()

  // map of string constants that will be added only once
  // string_constant -> reused_term
  private val reusableStringConstants: mutable.Map[String, String] = mutable.Map[String,  String]()

  // map of type serializer that will be added only once
  // LogicalType -> reused_term
  private val reusableTypeSerializers: mutable.Map[LogicalType, String] =
    mutable.Map[LogicalType,  String]()

  /**
    * The current method name for [[reusableLocalVariableStatements]]. You can start a new
    * local variable statements for another method using [[startNewLocalVariableStatement()]]
    */
  private var currentMethodNameForLocalVariables = "DEFAULT"

  /**
   * Flag map that indicates whether the generated code for method is split into several methods.
   */
  private val isCodeSplitMap = mutable.Map[String, Boolean]()

  // map of local variable statements. It will be placed in method if method code not excess
  // max code length, otherwise will be placed in member area of the class. The statements
  // are maintained for multiple methods, so that it's a map from method_name to variables.
  //
  // method_name -> local_variable_statements
  private val reusableLocalVariableStatements = mutable.Map[String, mutable.LinkedHashSet[String]](
    (currentMethodNameForLocalVariables, mutable.LinkedHashSet[String]()))

  /**
    * the class is used as the  generated operator code's base class.
    */
  private var operatorBaseClass: Class[_] = classOf[TableStreamOperator[_]]

  // ---------------------------------------------------------------------------------
  // Getter
  // ---------------------------------------------------------------------------------

  def getReusableInputUnboxingExprs(inputTerm: String, index: Int): Option[GeneratedExpression] =
    reusableInputUnboxingExprs.get((inputTerm, index))

  def nullCheck: Boolean = tableConfig.getNullCheck

  // ---------------------------------------------------------------------------------
  // Local Variables for Code Split
  // ---------------------------------------------------------------------------------

  /**
    * Starts a new local variable statements for a generated class with the given method name.
    * @param methodName the method name which the fields will be placed into if code is not split.
    */
  def startNewLocalVariableStatement(methodName: String): Unit = {
    currentMethodNameForLocalVariables = methodName
    reusableLocalVariableStatements(methodName) = mutable.LinkedHashSet[String]()
  }

  /**
   * Set the flag [[isCodeSplitMap]] to be true for methodName, which indicates
   * the generated code is split into several methods.
   * @param methodName the method which will be split.
   */
  def setCodeSplit(methodName: String = currentMethodNameForLocalVariables): Unit = {
    isCodeSplitMap(methodName) = true
  }

  /**
    * Adds a reusable local variable statement with the given type term and field name.
    * The local variable statements will be placed in methods or class member area depends
    * on whether the method length excess max code length.
    *
    * @param fieldName  the field name prefix
    * @param fieldTypeTerm  the field type term
    * @return a new generated unique field name
    */
  def addReusableLocalVariable(fieldTypeTerm: String, fieldName: String): String = {
    val fieldTerm = newName(fieldName)
    reusableLocalVariableStatements
    .getOrElse(currentMethodNameForLocalVariables, mutable.LinkedHashSet[String]())
    .add(s"$fieldTypeTerm $fieldTerm;")
    fieldTerm
  }

  /**
    * Adds multiple pairs of local variables.
    * The local variable statements will be placed in methods or class
    * member area depends on whether the method length excess max code length.
    *
    * @param fieldTypeAndNames pairs of local variables with
    *                          left is field type term and right is field name
    * @return the new generated unique field names for each variable pairs
    */
  def addReusableLocalVariables(fieldTypeAndNames: (String, String)*): Seq[String] = {
    val fieldTerms = newNames(fieldTypeAndNames.map(_._2): _*)
    fieldTypeAndNames.map(_._1).zip(fieldTerms).foreach { case (fieldTypeTerm, fieldTerm) =>
      reusableLocalVariableStatements
      .getOrElse(currentMethodNameForLocalVariables, mutable.LinkedHashSet[String]())
      .add(s"$fieldTypeTerm $fieldTerm;")
    }
    fieldTerms
  }

  // ---------------------------------------------------------------------------------
  // generate reuse code methods
  // ---------------------------------------------------------------------------------

  /**
    * @return code block of statements that need to be placed in the member area of the class
    *         (e.g. inner class definition)
    */
  def reuseInnerClassDefinitionCode(): String = {
    reusableInnerClassDefinitionStatements.values.mkString("\n")
  }

  /**
    * @return code block of statements that need to be placed in the member area of the class
    *         (e.g. member variables and their initialization)
    */
  def reuseMemberCode(): String = {
    val result = reusableMemberStatements.mkString("\n")
    if (isCodeSplitMap.nonEmpty) {
      val localVariableAsMember = reusableLocalVariableStatements.map(
        statements => if (isCodeSplitMap.getOrElse(statements._1, false)) {
          statements._2.map("private " + _).mkString("\n")
        } else {
          ""
        }
      ).filter(_.length > 0).mkString("\n")
      result + "\n" + localVariableAsMember
    } else {
      result
    }
  }

  /**
    * @return code block of statements that will be placed in the member area of the class
    *         if generated code is split or in local variables of method
    */
  def reuseLocalVariableCode(methodName: String = currentMethodNameForLocalVariables): String = {
    if (isCodeSplitMap.getOrElse(methodName, false)) {
      GeneratedExpression.NO_CODE
    } else if (methodName == null) {
      reusableLocalVariableStatements(currentMethodNameForLocalVariables).mkString("\n")
    } else {
      reusableLocalVariableStatements(methodName).mkString("\n")
    }
  }

  /**
    * @return code block of statements that need to be placed in the constructor
    */
  def reuseInitCode(): String = {
    reusableInitStatements.mkString("\n")
  }

  /**
    * @return code block of statements that need to be placed in the per recode process block
    *         (e.g. Function or StreamOperator's processElement)
    */
  def reusePerRecordCode(): String = {
    reusablePerRecordStatements.mkString("\n")
  }

  /**
    * @return code block of statements that need to be placed in the open() method
    *         (e.g. RichFunction or StreamOperator)
    */
  def reuseOpenCode(): String = {
    reusableOpenStatements.mkString("\n")
  }

  /**
    * @return code block of statements that need to be placed in the close() method
    *         (e.g. RichFunction or StreamOperator)
    */
  def reuseCloseCode(): String = {
    reusableCloseStatements.mkString("\n")
  }

  /**
    * @return code block of statements that need to be placed in the cleanup() method of
    *         [AggregationsFunction]
    */
  def reuseCleanupCode(): String = {
    reusableCleanupStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that unbox input variables to a primitive variable
    *         and a corresponding null flag variable
    */
  def reuseInputUnboxingCode(): String = {
    reusableInputUnboxingExprs.values.map(_.code).mkString("\n")
  }

  /**
    * Returns code block of unboxing input variables which belongs to the given inputTerm.
    */
  def reuseInputUnboxingCode(inputTerm: String): String = {
    val exprs = reusableInputUnboxingExprs.filter { case ((term, _), _) =>
      inputTerm.equals(term)
    }
    val codes = for (((_, _), expr) <- exprs) yield expr.code
    codes.mkString("\n").trim
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
    }.mkString("\n")
  }

  def setOperatorBaseClass(operatorBaseClass: Class[_]): CodeGeneratorContext = {
    this.operatorBaseClass = operatorBaseClass
    this
  }

  def getOperatorBaseClass: Class[_] = this.operatorBaseClass

  // ---------------------------------------------------------------------------------
  // add reusable code blocks
  // ---------------------------------------------------------------------------------

  /**
    * Adds a reusable inner class statement with the given class name and class code
    */
  def addReusableInnerClass(className: String, statements: String): Unit = {
    reusableInnerClassDefinitionStatements(className) = statements
  }

  /**
    * Adds a reusable member field statement to the member area.
    *
    * @param memberStatement the member field declare statement
    */
  def addReusableMember(memberStatement: String): Unit = {
    reusableMemberStatements.add(memberStatement)
  }

  /**
    * Adds a reusable init statement which will be placed in constructor.
    */
  def addReusableInitStatement(s: String): Unit = reusableInitStatements.add(s)

  /**
    * Adds a reusable per record statement
    */
  def addReusablePerRecordStatement(s: String): Unit = reusablePerRecordStatements.add(s)

  /**
    * Adds a reusable open statement
    */
  def addReusableOpenStatement(s: String): Unit = reusableOpenStatements.add(s)

  /**
    * Adds a reusable close statement
    */
  def addReusableCloseStatement(s: String): Unit = reusableCloseStatements.add(s)

  /**
    * Adds a reusable cleanup statement
    */
  def addReusableCleanupStatement(s: String): Unit = reusableCleanupStatements.add(s)


  /**
    * Adds a reusable input unboxing expression
    */
  def addReusableInputUnboxingExprs(
    inputTerm: String,
    index: Int,
    expr: GeneratedExpression): Unit = reusableInputUnboxingExprs((inputTerm, index)) = expr

  /**
    * Adds a reusable output record statement to member area.
    */
  def addReusableOutputRecord(
      t: LogicalType,
      clazz: Class[_],
      outRecordTerm: String,
      outRecordWriterTerm: Option[String] = None): Unit = {
    generateRecordStatement(t, clazz, outRecordTerm, outRecordWriterTerm, this)
  }

  /**
    * Adds a reusable null [[org.apache.flink.table.dataformat.GenericRowData]] to the member area.
    */
  def addReusableNullRow(rowTerm: String, arity: Int): Unit = {
    addReusableOutputRecord(
      RowType.of((0 until arity).map(_ => new IntType()): _*),
      classOf[GenericRowData],
      rowTerm)
  }

  /**
    * Adds a reusable internal hash set to the member area of the generated class.
    */
  def addReusableHashSet(elements: Seq[GeneratedExpression], elementType: LogicalType): String = {
    val fieldTerm = newName("set")

    val setTypeTerm = elementType.getTypeRoot match {
      case TINYINT => className[ByteHashSet]
      case SMALLINT => className[ShortHashSet]
      case INTEGER => className[IntHashSet]
      case BIGINT => className[LongHashSet]
      case FLOAT => className[FloatHashSet]
      case DOUBLE => className[DoubleHashSet]
      case _ => className[ObjectHashSet[_]]
    }

    addReusableMember(
      s"final $setTypeTerm $fieldTerm = new $setTypeTerm(${elements.size});")

    elements.foreach { element =>
      val content =
        s"""
           |${element.code}
           |if (${element.nullTerm}) {
           |  $fieldTerm.addNull();
           |} else {
           |  $fieldTerm.add(${element.resultTerm});
           |}
           |""".stripMargin
      reusableInitStatements.add(content)
    }
    reusableInitStatements.add(s"$fieldTerm.optimize();")

    fieldTerm
  }

  /**
    * Adds a reusable timestamp to the beginning of the SAM of the generated class.
    */
  def addReusableTimestamp(): String = {
    val fieldTerm = s"timestamp"

    reusableMemberStatements.add(s"private $TIMESTAMP_DATA $fieldTerm;")

    val field =
      s"""
         |$fieldTerm =
         |  $TIMESTAMP_DATA.fromEpochMillis(java.lang.System.currentTimeMillis());
         |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
    * Adds a reusable time to the beginning of the SAM of the generated [[Function]].
    */
  def addReusableTime(): String = {
    val fieldTerm = s"time"

    val timestamp = addReusableTimestamp()

    // declaration
    reusableMemberStatements.add(s"private int $fieldTerm;")

    // assignment
    // adopted from org.apache.calcite.runtime.SqlFunctions.currentTime()
    val field =
      s"""
         |$fieldTerm = (int) ($timestamp.getMillisecond() % ${DateTimeUtils.MILLIS_PER_DAY});
         |if (time < 0) {
         |  time += ${DateTimeUtils.MILLIS_PER_DAY};
         |}
         |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
    * Adds a reusable local date time to the beginning of the SAM of the generated class.
    */
  def addReusableLocalDateTime(): String = {
    val fieldTerm = s"localtimestamp"

    val timestamp = addReusableTimestamp()

    // declaration
    reusableMemberStatements.add(s"private $TIMESTAMP_DATA $fieldTerm;")

    // assignment
    val field =
      s"""
         |$fieldTerm = $TIMESTAMP_DATA.fromEpochMillis(
         |  $timestamp.getMillisecond() +
         |  java.util.TimeZone.getDefault().getOffset($timestamp.getMillisecond()));
         |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
    * Adds a reusable local time to the beginning of the SAM of the generated class.
    */
  def addReusableLocalTime(): String = {
    val fieldTerm = s"localtime"

    val localtimestamp = addReusableLocalDateTime()

    // declaration
    reusableMemberStatements.add(s"private int $fieldTerm;")

    // assignment
    // adopted from org.apache.calcite.runtime.SqlFunctions.localTime()
    val field =
    s"""
       |$fieldTerm = (int) ($localtimestamp.getMillisecond() % ${DateTimeUtils.MILLIS_PER_DAY});
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

    // declaration
    reusableMemberStatements.add(s"private int $fieldTerm;")

    // assignment
    // adopted from org.apache.calcite.runtime.SqlFunctions.currentDate()
    val field =
      s"""
         |$fieldTerm = (int) ($timestamp.getMillisecond() / ${DateTimeUtils.MILLIS_PER_DAY});
         |if ($time < 0) {
         |  $fieldTerm -= 1;
         |}
         |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
    * Adds a reusable TimeZone to the member area of the generated class.
    */
  def addReusableSessionTimeZone(): String = {
    val zoneID = TimeZone.getTimeZone(tableConfig.getLocalTimeZone).getID
    val stmt =
      s"""private static final java.util.TimeZone $DEFAULT_TIMEZONE_TERM =
         |                 java.util.TimeZone.getTimeZone("$zoneID");""".stripMargin
    addReusableMember(stmt)
    DEFAULT_TIMEZONE_TERM
  }

  /**
    * Adds a reusable [[java.util.Random]] to the member area of the generated class.
    *
    * The seed parameter must be a literal/constant expression.
    *
    * @return member variable term
    */
  def addReusableRandom(seedExpr: Option[GeneratedExpression]): String = {
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

    reusableMemberStatements.add(field)
    reusableInitStatements.add(fieldInit)
    fieldTerm
  }

  /**
    * Adds a reusable Object to the member area of the generated class
    * @param obj  the object to be added to the generated class
    * @param fieldNamePrefix  prefix field name of the generated member field term
    * @param fieldTypeTerm  field type class name
    * @return the generated unique field term
    */
  def addReusableObject(
      obj: AnyRef,
      fieldNamePrefix: String,
      fieldTypeTerm: String = null): String = {
    addReusableObjectWithName(obj, newName(fieldNamePrefix), fieldTypeTerm)
  }

  def addReusableObjectWithName(
      obj: AnyRef,
      fieldTerm: String,
      fieldTypeTerm: String = null): String = {
    val clsName = Option(fieldTypeTerm).getOrElse(obj.getClass.getCanonicalName)
    addReusableObjectInternal(obj, fieldTerm, clsName)
    fieldTerm
  }

  private def addReusableObjectInternal(
      obj: AnyRef,
      fieldTerm: String,
      fieldTypeTerm: String): Unit = {
    val idx = references.length
    // make a deep copy of the object
    val byteArray = InstantiationUtil.serializeObject(obj)
    val objCopy: AnyRef = InstantiationUtil.deserializeObject(
      byteArray,
      Thread.currentThread().getContextClassLoader)
    references += objCopy

    reusableMemberStatements.add(s"private transient $fieldTypeTerm $fieldTerm;")
    reusableInitStatements.add(s"$fieldTerm = ((($fieldTypeTerm) references[$idx]));")
  }

  /**
    * Adds a reusable [[UserDefinedFunction]] to the member area of the generated [[Function]].
    *
    * @param function [[UserDefinedFunction]] object to be instantiated during runtime
    * @param functionContextClass class of [[FunctionContext]]
    * @param contextTerm [[RuntimeContext]] term to access the [[RuntimeContext]]
    * @return member variable term
    */
  def addReusableFunction(
      function: UserDefinedFunction,
      functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext],
      contextTerm: String = null): String = {
    val classQualifier = function.getClass.getName
    val fieldTerm = CodeGenUtils.udfFieldName(function)

    addReusableObjectInternal(function, fieldTerm, classQualifier)

    val openFunction = if (contextTerm != null) {
      s"""
         |$fieldTerm.open(new ${functionContextClass.getCanonicalName}($contextTerm));
       """.stripMargin
    } else {
      s"""
         |$fieldTerm.open(new ${functionContextClass.getCanonicalName}(getRuntimeContext()));
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
   * Adds a reusable [[DataStructureConverter]] to the member area of the generated class.
   *
   * @param converter converter to be added
   * @param classLoaderTerm term to access the [[ClassLoader]] for user-defined classes
   */
  def addReusableConverter(
      converter: DataStructureConverter[_, _],
      classLoaderTerm: String = null)
    : String = {

    val converterTerm = addReusableObject(converter, "converter")

    val openConverter = if (classLoaderTerm != null) {
      s"""
         |$converterTerm.open($classLoaderTerm);
       """.stripMargin
    } else {
      s"""
         |$converterTerm.open(getRuntimeContext().getUserCodeClassLoader());
       """.stripMargin
    }
    reusableOpenStatements.add(openConverter)

    converterTerm
  }

  /**
    * Adds a reusable [[TypeSerializer]] to the member area of the generated class.
    *
    * @param t the internal type which used to generate internal type serializer
    * @return member variable term
    */
  def addReusableTypeSerializer(t: LogicalType): String = {
    // if type serializer has been used before, we can reuse the code that
    // has already been generated
    reusableTypeSerializers.get(t) match {
      case Some(term) => term

      case None =>
        val term = newName("typeSerializer")
        val ser = InternalSerializers.create(t)
        addReusableObjectInternal(ser, term, ser.getClass.getCanonicalName)
        reusableTypeSerializers(t) = term
        term
    }
  }

  /**
    * Adds a reusable static SLF4J Logger to the member area of the generated class.
    */
  def addReusableLogger(logTerm: String, clazzTerm: String): Unit = {
    val stmt =
      s"""
         |private static final org.slf4j.Logger $logTerm =
         |  org.slf4j.LoggerFactory.getLogger("$clazzTerm");
         |""".stripMargin
    reusableMemberStatements.add(stmt)
  }

  /**
    * Adds a reusable constant to the member area of the generated class.
    *
    * @param constant constant expression
    * @return generated expression with the fieldTerm and nullTerm
    */
  def addReusableConstant(
      constant: GeneratedExpression,
      nullCheck: Boolean): GeneratedExpression = {
    require(constant.literal, "Literal expected")

    val fieldTerm = newName("constant")
    val nullTerm = fieldTerm + "isNull"

    val fieldType = primitiveTypeTermForType(constant.resultType)

    val field =
      s"""
         |private final $fieldType $fieldTerm;
         |private final boolean $nullTerm;
         |""".stripMargin
    reusableMemberStatements.add(field)

    val init =
      s"""
         |${constant.code}
         |$fieldTerm = ${constant.resultTerm};
         |$nullTerm = ${constant.nullTerm};
         |""".stripMargin
    reusableInitStatements.add(init)

    GeneratedExpression(fieldTerm, nullTerm, "", constant.resultType)
  }


  /**
    * Adds a reusable string constant to the member area of the generated class.
    */
  def addReusableStringConstants(value: String): String = {
    reusableStringConstants.get(value) match {
      case Some(field) => field
      case None =>
        val field = newName("str")
        val stmt =
          s"""
             |private final $BINARY_STRING $field = $BINARY_STRING.fromString("$value");
           """.stripMargin
        reusableMemberStatements.add(stmt)
        reusableStringConstants(value) = field
        field
    }
  }

  /**
    * Adds a reusable MessageDigest to the member area of the generated [[Function]].
    *
    * @return member variable term
    */
  def addReusableMessageDigest(algorithm: String): String = {
    val fieldTerm = newName("messageDigest")

    val field = s"final java.security.MessageDigest $fieldTerm;"
    reusableMemberStatements.add(field)

    val fieldInit =
      s"""
         |try {
         |  $fieldTerm = java.security.MessageDigest.getInstance("$algorithm");
         |} catch (java.security.NoSuchAlgorithmException e) {
         |  throw new RuntimeException("Algorithm for '$algorithm' is not available.", e);
         |}
         |""".stripMargin
    reusableInitStatements.add(fieldInit)

    fieldTerm
  }

  /**
    * Adds a constant SHA2 reusable MessageDigest to the member area of the generated [[Function]].
    *
    * @return member variable term
    */
  def addReusableSha2MessageDigest(constant: GeneratedExpression): String = {
    require(constant.literal, "Literal expected")
    val fieldTerm = newName("messageDigest")

    val field =
      s"final java.security.MessageDigest $fieldTerm;"
    reusableMemberStatements.add(field)

    val bitLen = constant.resultTerm
    val init =
      s"""
         |if ($bitLen == 224 || $bitLen == 256 || $bitLen == 384 || $bitLen == 512) {
         |  try {
         |    $fieldTerm = java.security.MessageDigest.getInstance("SHA-" + $bitLen);
         |  } catch (java.security.NoSuchAlgorithmException e) {
         |    throw new RuntimeException(
         |      "Algorithm for 'SHA-" + $bitLen + "' is not available.", e);
         |  }
         |} else {
         |  throw new RuntimeException("Unsupported algorithm.");
         |}
         |""".stripMargin
    val nullableInit = if (nullCheck) {
      s"""
         |${constant.code}
         |if (${constant.nullTerm}) {
         |  $fieldTerm = null;
         |} else {
         |  $init
         |}
         |""".stripMargin
    } else {
      s"""
         |${constant.code}
         |$init
         |""".stripMargin
    }
    reusableInitStatements.add(nullableInit)

    fieldTerm
  }
}

object CodeGeneratorContext {
  def apply(config: TableConfig): CodeGeneratorContext = {
    new CodeGeneratorContext(config)
  }
}
