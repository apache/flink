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

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.commons.codec.binary.Base64
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.memory.MemoryManager
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.functions.{FunctionContext, UserDefinedFunction}
import org.apache.flink.table.api.types.{DataTypes, InternalType, RowType}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.CodeGeneratorContext._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.dataformat._
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics
import org.apache.flink.table.runtime.functions.FunctionContextImpl
import org.apache.flink.table.runtime.util.{ResettableExternalBuffer, RowIterator}
import org.apache.flink.table.typeutils.{AbstractRowSerializer, BaseRowSerializer, BinaryRowSerializer}
import org.apache.flink.table.util.collections._
import org.apache.flink.types.Row
import org.apache.flink.util.{InstantiationUtil, Preconditions}

import scala.collection.mutable

/**
 * The context for code generator, maintaining various reusable statements that could be insert
 * into different code sections in the final generated class.
 */
class CodeGeneratorContext(val tableConfig: TableConfig, val supportReference: Boolean) {

  // set of inner class definition statements that will be added only once
  private val reusableInnerClassDefinitionStatements: mutable.Map[String, String] =
    mutable.Map[String, String]()

  // set of member statements that will be added only once
  private val reusableMemberStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // Referenced objects of code generator classes to avoid serialization and base64.
  val references: mutable.ArrayBuffer[AnyRef] = new mutable.ArrayBuffer[AnyRef]()

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

  // set of endInput statements that will be added only once
  private val reusableEndInputStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of statements for cleanup dataview that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableCleanupStatements = mutable.LinkedHashSet[String]()

  // map of initial input unboxing expressions that will be added only once
  // (inputTerm, index) -> expr
  val reusableInputUnboxingExprs: mutable.Map[(String, Int), GeneratedExpression] =
    mutable.Map[(String, Int), GeneratedExpression]()

  // set of constructor statements that will be added only once
  private val reusableConstructorStatements: mutable.LinkedHashSet[(String, String)] =
    mutable.LinkedHashSet[(String, String)]()

  // map of type serializer expressions
  private val reusableTypeSerializerExprs: mutable.Map[InternalType, String] =
    mutable.Map[InternalType,  String]()

  // map of generic type expressions
  private val reusableTypeExprs: mutable.Map[InternalType, String] =
    mutable.Map[InternalType,  String]()

  // map of string constants
  private val reusableStringConstants: mutable.Map[String, String] = mutable.Map[String,  String]()

  /**
    * statements will be placed in the member area of the class if generated code is not split
    * or in local variables of method. This statements is mapping from method name to field
    * statements.
    */
  private val reusableMethodFieldStatements = mutable.Map[String, mutable.LinkedHashSet[String]]()

  /**
    * statements will be placed in the member area of the class if generated code is not split
    * or in local variables of method. This is the default reusable statements, if
    * [[startNewFieldStatements()]] is called, the default statements will be dropped and replaced
    * with a new statements.
    */
  private var reusableFieldStatements = mutable.LinkedHashSet[String]()

  /**
    * start a new field statements for a generated class with many methods.
    * @param methodName the method name which the fields will be placed into if code is not split.
    */
  def startNewFieldStatements(methodName: String): Unit = {
    reusableFieldStatements = mutable.LinkedHashSet[String]()
    reusableMethodFieldStatements(methodName) = reusableFieldStatements
  }

  /**
    * the class is used as the  generated operator code's base class.
    */
  private var operatorBaseClass: Class[_] = classOf[AbstractStreamOperatorWithMetrics[_]]

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
    reusableMemberStatements.mkString("\n")
  }

  /**
    * @return code block of statements that will be placed in the member area of the class
    *         if generated code is split or in local variables of method
    */
  def reuseFieldCode(methodName: String = null): String = {
    if (methodName == null) {
      reusableFieldStatements.mkString("\n")
    } else {
      reusableMethodFieldStatements(methodName).mkString("\n")
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
    * @return code block of statements that need to be placed in the endInput() method
    *         (StreamOperator)
    */
  def reuseEndInputCode(): String = {
    reusableEndInputStatements.mkString("\n")
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

  def reuseInputUnboxingCode(terms: Set[String]): String = {
    val exprs = reusableInputUnboxingExprs.filter { entry =>
      val ((term, _), _) = entry
      terms contains term
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

  def addReusableInnerClass(className: String, statements: String): Unit = {
    reusableInnerClassDefinitionStatements(className) = statements
  }

  def addReusableMember(declareCode: String, initCode: String = ""): Unit = {
    reusableMemberStatements.add(declareCode)
    if (!initCode.isEmpty) reusableInitStatements.add(initCode)
  }

  def getTableConfig: TableConfig = this.tableConfig

  /**
    * @param fields statements of local field like "boolean isNull;"
    * this fields may be put in class or in method
    */
  def addAllReusableFields(fields: Set[String]): Unit = {
    this.reusableFieldStatements ++= fields
  }

  def addPerRecordStatement(s: String): Unit = reusablePerRecordStatements.add(s)

  def addReusableOpenStatement(s: String): Unit = reusableOpenStatements.add(s)

  def addReusableCloseStatement(s: String): Unit = reusableCloseStatements.add(s)

  def addReusableEndInputStatement(s: String): Unit = reusableEndInputStatements.add(s)

  def addReusableCleanupStatement(s: String): Unit = reusableCleanupStatements.add(s)

  def getReusableInputUnboxingExprs(inputTerm: String, index: Int): Option[GeneratedExpression] =
    reusableInputUnboxingExprs.get((inputTerm, index))

  def addReusableInputUnboxingExprs(
      inputTerm: String,
      index: Int,
      expr: GeneratedExpression): Unit =
    reusableInputUnboxingExprs((inputTerm, index)) = expr

  /**
   * generator a output record..
   * The passed [[InternalType]] defines the type class to be instantiated.
   * @param reused If objects or variables can be reused, they will be added a reusable output
   * record to the member area of the generated class. If not they will be return directly.
   */
  def addOutputRecord(
      t: InternalType,
      clazz: Class[_],
      outRecordTerm: String,
      outRecordWriterTerm: Option[String] = None,
      reused: Boolean = true): String = {
    val statement = t match {
      case rt: RowType if clazz == classOf[BinaryRow] =>
        val writerTerm = outRecordWriterTerm.getOrElse(
          throw new CodeGenException("No writer is specified when writing BinaryRow record.")
        )
        val binaryRowWriter = classOf[BinaryRowWriter].getName
        val typeTerm = clazz.getCanonicalName
        s"""
           |final $typeTerm $outRecordTerm = new $typeTerm(${rt.getArity});
           |final $binaryRowWriter $writerTerm = new $binaryRowWriter($outRecordTerm);
           |""".stripMargin.trim
      case rt: RowType if classOf[ObjectArrayRow].isAssignableFrom(clazz) =>
        val typeTerm = clazz.getCanonicalName
        s"final $typeTerm $outRecordTerm = new $typeTerm(${rt.getArity});"
      case rt: RowType if clazz == classOf[JoinedRow] =>
        val typeTerm = clazz.getCanonicalName
        s"final $typeTerm $outRecordTerm = new $typeTerm();"
      case _ =>
        val typeTerm = boxedTypeTermForType(t)
        s"final $typeTerm $outRecordTerm = new $typeTerm();"
    }
    if (reused) {
      reusableMemberStatements.add(statement)
      ""
    } else {
      statement
    }
  }

  /**
    * Adds a reusable null [[org.apache.flink.table.dataformat.GenericRow]] to the member area.
    */
  def addReusableNullRow(rowTerm: String, arity: Int): String = {
    addOutputRecord(
      new RowType(
        (0 until arity).map(_ => DataTypes.INT): _*),
      classOf[GenericRow],
      rowTerm)
  }

  def addReusableTypeSerializer(t: InternalType): String = {
    // if type serializer has been used before, we can reuse the code that
    // has already been generated
    reusableTypeSerializerExprs.get(t) match {
      case Some(expr) => expr

      case None =>
        val term = newName("typeSerializer")
        val declareCode =
          s"org.apache.flink.api.common.typeutils.TypeSerializer $term;"
        val ser = DataTypes.createInternalSerializer(t)
        val initCode = if (supportReference) {
          s"$term = ${addReferenceObj(ser)};"
        } else {
          val byteArray = InstantiationUtil.serializeObject(ser)
          val serializedData = Base64.encodeBase64URLSafeString(byteArray)
          s"""
             |$term = (org.apache.flink.api.common.typeutils.TypeSerializer)
             | org.apache.flink.util.InstantiationUtil.deserializeObject(
             |  org.apache.commons.codec.binary.Base64.decodeBase64("$serializedData"),
             |    Thread.currentThread().getContextClassLoader());
           """.stripMargin
        }
        addReusableMember(declareCode, initCode)

        reusableTypeSerializerExprs(t) = term
        term
    }
  }

  def addReusableGenericType(t: InternalType): String = {
    // if type information has been used before, we can reuse the code that
    // has already been generated
    reusableTypeExprs.get(t) match {
      case Some(expr) => expr

      case None =>
        val term = newName("typeInfo")
        val typeTerm = classOf[InternalType].getName
        val declareCode = s"transient $typeTerm $term;"
        val initCode = if (supportReference) {
          s"$term = ${addReferenceObj(t)};"
        } else {
          val byteArray = InstantiationUtil.serializeObject(t)
          val serializedData = Base64.encodeBase64URLSafeString(byteArray)
          s"""
             |$term = ($typeTerm) org.apache.flink.util.InstantiationUtil.deserializeObject(
             |  org.apache.commons.codec.binary.Base64.decodeBase64("$serializedData"),
             |    Thread.currentThread().getContextClassLoader());
           """.stripMargin
        }
        addReusableMember(declareCode, initCode)

        reusableTypeExprs(t) = term
        term
    }
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
   * Adds a reusable set to the member area of the generated class.
   */
  def addReusableSet(elements: Seq[GeneratedExpression], t: InternalType): String = {
    val fieldTerm = newName("set")

    val setTerm = (t match {
      case DataTypes.BYTE => classOf[ByteSet]
      case DataTypes.SHORT => classOf[ShortSet]
      case DataTypes.INT => classOf[IntSet]
      case DataTypes.LONG => classOf[LongSet]
      case DataTypes.FLOAT => classOf[FloatSet]
      case DataTypes.DOUBLE => classOf[DoubleSet]
      case _ => classOf[ObjectHashSet[_]]
    }).getCanonicalName

    addReusableMember(
      s"final $setTerm $fieldTerm;",
      s"$fieldTerm = new $setTerm(${elements.size});")

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
    addReusableTimestamp()
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
    val timeZone = addReusableTimeZone()
    val localtimestamp = addReusableLocalTimestamp()
    // adopted from org.apache.calcite.runtime.SqlFunctions.localTime()
    val field =
      s"""
         |final int $fieldTerm = (int) ( ($localtimestamp + $timeZone.getOffset($localtimestamp))
         |                              % ${DateTimeUtils.MILLIS_PER_DAY});
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
    val timeZone = addReusableTimeZone()

    // adopted from org.apache.calcite.runtime.SqlFunctions.currentDate()
    val field =
      s"""
         |final int $fieldTerm = (int) (($timestamp + $timeZone.getOffset($timestamp))
         |                              / ${DateTimeUtils.MILLIS_PER_DAY});
         |if ($time < 0) {
         |  $fieldTerm -= 1;
         |}
         |""".stripMargin
    reusablePerRecordStatements.add(field)
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

  def addReferenceObj(obj: AnyRef, className: String = null): String = {
    val idx = references.length

    // make a deep copy of the object
    val byteArray = InstantiationUtil.serializeObject(obj)
    val objCopy: AnyRef = InstantiationUtil.deserializeObject(
      byteArray,
      Thread.currentThread().getContextClassLoader)
    references += objCopy

    val clsName = Option(className).getOrElse(obj.getClass.getName)
    s"(($clsName) references[$idx])"
  }

  /**
    * Adds a reusable Object to the member area of the generated class
    */
  def addReusableObject(obj: AnyRef, termPrefix: String, className: String = null): String = {
    val fieldTerm = newName(termPrefix)
    val typeTerm = obj.getClass.getCanonicalName

    val initCode = if (supportReference) {
      s"$fieldTerm = ${addReferenceObj(obj, className)};"
    } else {
      val byteArray = InstantiationUtil.serializeObject(obj)
      val serializedData = Base64.encodeBase64URLSafeString(byteArray)
      s"""
         | $fieldTerm = ($typeTerm)
         |      ${classOf[InstantiationUtil].getCanonicalName}.deserializeObject(
         |         ${classOf[Base64].getCanonicalName}.decodeBase64("$serializedData"),
         |         Thread.currentThread().getContextClassLoader());
           """.stripMargin
    }

    addReusableMember(s"private $typeTerm $fieldTerm;", initCode)
    fieldTerm
  }

  /**
   * Adds a reusable [[UserDefinedFunction]] to the member area of the generated class.
   */
  def addReusableFunction(
      function: UserDefinedFunction,
      functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContextImpl],
      constructorTerm: String = null): String = {
    Preconditions.checkNotNull(functionContextClass)
    val classQualifier = function.getClass.getCanonicalName
    val fieldTerm = udfFieldName(function)

    val fieldFunction =
      s"""
         |final $classQualifier $fieldTerm;
         |""".stripMargin
    reusableMemberStatements.add(fieldFunction)

    val initCode = if (supportReference) {
      s"$fieldTerm = (${addReferenceObj(function)});"
    } else {
      val functionSerializedData = UserDefinedFunctionUtils.serialize(function)
      s"""
         |$fieldTerm = ($classQualifier)
         |${UserDefinedFunctionUtils.getClass.getName.stripSuffix("$")}
         |.deserialize("$functionSerializedData");
       """.stripMargin
    }

    reusableInitStatements.add(initCode)

    val openFunction = if (constructorTerm != null) {
      s"""
         |$fieldTerm.open(new ${functionContextClass.getCanonicalName}($constructorTerm));
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

  def addReusableFunction(funcName: String, funcCode: String, classQualifier: String): String = {
    val fieldTerm = newName(s"function_$funcName")

    val base64FuncCode = Base64.encodeBase64URLSafeString(funcCode.getBytes("UTF-8"))

    val fieldFunction =
      s"""
         |$classQualifier $fieldTerm;
         |
         |""".stripMargin
    reusableMemberStatements.add(fieldFunction)

    val openFunction =
      s"""
         |$fieldTerm = ($classQualifier)
         |  org.apache.flink.table.codegen.CodeGenUtils.compile(
         |    getRuntimeContext().getUserCodeClassLoader(),
         |    "$funcName",
         |    new String(org.apache.commons.codec.binary.Base64.decodeBase64("$base64FuncCode"),
         |      "UTF-8")).newInstance();
         |""".stripMargin
    reusableOpenStatements.add(openFunction)

    val closeFunction =
      s"""
         |$fieldTerm.close();
         |""".stripMargin
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
   * Adds a reusable [[java.util.ArrayList]] to the member area.
   */
  def addReusableList(fieldTerm: String): Unit = {
    val field =
      s"""
         |transient java.util.List $fieldTerm = null;
         |""".stripMargin
    val init =
      s"""
         |$fieldTerm = new java.util.ArrayList();
         |""".stripMargin
    addReusableMember(field, init)
  }

  def addReusableLogger(logTerm: String, clazzTerm: String): Unit = {
    val field =
      s"""
         |private static final org.slf4j.Logger $logTerm =
         |  org.slf4j.LoggerFactory.getLogger("$clazzTerm");
         |""".stripMargin
    addReusableMember(field)
  }

  /**
    * Adds a reusable constant to the member area of the generated [[Function]].
    *
    * @param constant constant expression
    * @return member variable term
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

  def newReusableField(name: String, fieldTypeTerm: String): String = {
    val fieldName = newName(name)
    reusableFieldStatements.add(s"$fieldTypeTerm $fieldName;")
    fieldName
  }

  def newReusableFields(names: Seq[String], fieldTypeTerms: Seq[String]): Seq[String] = {
    require(names.length == fieldTypeTerms.length)
    require(names.toSet.size == names.length, "Duplicated names")
    val fieldNames = newNames(names)
    fieldTypeTerms.zip(fieldNames).foreach(x => reusableFieldStatements.add(s"${x._1} ${x._2};"))
    fieldNames
  }

  def setOperatorBaseClass(operatorBaseClass: Class[_]): CodeGeneratorContext = {
    this.operatorBaseClass = operatorBaseClass
    this
  }

  def getOperatorBaseClass: Class[_] = this.operatorBaseClass

  def addReusableStringConstants(value: String): String =
    reusableStringConstants.get(value) match {
      case Some(field) => field
      case None =>
        val field = newName("str")
        addReusableMember(
          s"$BINARY_STRING $field = $BINARY_STRING.fromString(${"\""}$value${"\""});")
        reusableStringConstants(value) = field
        field
    }

  def addReusableTimeZone(): String = {
    val memberTerm = s"timeZone"
    val zoneID = tableConfig.getTimeZone.getID
    val stmt =
      s"""private static final java.util.TimeZone $DEFAULT_TIMEZONE_TERM =
         |                 java.util.TimeZone.getTimeZone("$zoneID");""".stripMargin

    addReusableMember(stmt)
    memberTerm
  }

  def addReusableResettableExternalBuffer(
    fieldTerm: String, memSize: Long, serializer: String): Unit = {
    val memManager = CodeGenUtils.newName("memManager")
    val ioManager = CodeGenUtils.newName("ioManager")
    addReusableOpenStatement(
      s"$MEMORY_MANAGER $memManager = getContainingTask().getEnvironment().getMemoryManager();")
    addReusableOpenStatement(
      s"$IO_MANAGER $ioManager = getContainingTask().getEnvironment().getIOManager();")

    val field =
      s"""
         |${CodeGeneratorContext.RESETTABLE_EXTERNAL_BUFFER} $fieldTerm = null;
         |""".stripMargin
    val open =
      s"""
         |$fieldTerm = new ${CodeGeneratorContext.RESETTABLE_EXTERNAL_BUFFER}(
         |    $memManager, $ioManager, $memManager
         |    .allocatePages(
         |        getContainingTask(), ((int) $memSize) / $memManager.getPageSize()), $serializer);
         |""".stripMargin
    addReusableMember(field)
    addReusableOpenStatement(open)
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
  def addReusableSha2MessageDigest(constant: GeneratedExpression, nullCheck: Boolean): String = {
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

  def apply(tableConfig: TableConfig, supportReference: Boolean = false): CodeGeneratorContext =
    new CodeGeneratorContext(tableConfig, supportReference)

  val DEFAULT_INPUT1_TERM = "in1"

  val DEFAULT_INPUT2_TERM = "in2"

  val DEFAULT_COLLECTOR_TERM = "c"

  val DEFAULT_OUT_RECORD_TERM = "out"

  val DEFAULT_OPERATOR_COLLECTOR_TERM = "output"

  val DEFAULT_OUT_RECORD_WRITER_TERM = "outWriter"

  val DEFAULT_CONTEXT_TERM = "ctx"

  val ROW: String = classOf[Row].getCanonicalName

  val ROW_ITERATOR: String = classOf[RowIterator[_]].getCanonicalName

  val BINARY_ROW: String = classOf[BinaryRow].getCanonicalName

  val BINARY_STRING: String = classOf[BinaryString].getCanonicalName

  val BASE_ROW: String = classOf[BaseRow].getCanonicalName

  val JOINED_ROW: String = classOf[JoinedRow].getCanonicalName

  val GENERIC_ROW: String = classOf[GenericRow].getCanonicalName

  val MEMORY_MANAGER: String = classOf[MemoryManager].getCanonicalName

  val IO_MANAGER: String = classOf[IOManager].getCanonicalName

  val RESETTABLE_EXTERNAL_BUFFER: String = classOf[ResettableExternalBuffer].getCanonicalName

  val ABSTRACT_ROW_SERIALIZER: String = classOf[AbstractRowSerializer[_]].getCanonicalName

  val BASE_ROW_SERIALIZER: String = classOf[BaseRowSerializer[_]].getCanonicalName

  val BINARY_ROW_SERIALIZER: String = classOf[BinaryRowSerializer].getCanonicalName

  val BASE_ROW_UTIL: String = classOf[BaseRowUtil].getCanonicalName

  val DEFAULT_TIMEZONE_TERM = "timeZone"

  def udfFieldName(udf: UserDefinedFunction): String = s"function_${udf.functionIdentifier}"
}
