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
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable.{ROW, _}
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.GeneratedExpression.{ALWAYS_NULL, NEVER_NULL, NO_CODE}
import org.apache.flink.table.codegen.calls.ScalarOperators._
import org.apache.flink.table.codegen.calls.{CurrentTimePointCallGen, FunctionGenerator}
import org.apache.flink.table.functions.sql.{ProctimeSqlFunction, ScalarSqlFunctions, StreamRecordTimestampSqlFunction}
import org.apache.flink.table.functions.{FunctionContext, UserDefinedFunction}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.utils.EncodingUtils
import org.joda.time.format.DateTimeFormatter

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * [[CodeGenerator]] is the base code generator for generating Flink
  * [[org.apache.flink.api.common.functions.Function]]s.
  * It is responsible for expression generation and tracks the context (member variables etc).
  *
  * @param config configuration that determines runtime behavior
  * @param nullableInput input(s) can be null.
  * @param input1 type information about the first input of the Function
  * @param input2 type information about the second input if the Function is binary
  * @param input1FieldMapping additional mapping information for input1.
  *   POJO types have no deterministic field order and some input fields might not be read.
  *   The input1FieldMapping is also used to inject time indicator attributes.
  * @param input2FieldMapping additional mapping information for input2.
  *   POJO types have no deterministic field order and some input fields might not be read.
  */
abstract class CodeGenerator(
    config: TableConfig,
    nullableInput: Boolean,
    input1: TypeInformation[_ <: Any],
    input2: Option[TypeInformation[_ <: Any]] = None,
    input1FieldMapping: Option[Array[Int]] = None,
    input2FieldMapping: Option[Array[Int]] = None)
  extends RexVisitor[GeneratedExpression] {

  // check if nullCheck is enabled when inputs can be null
  if (nullableInput && !config.getNullCheck) {
    throw new CodeGenException("Null check must be enabled if entire rows can be null.")
  }

  // check for POJO input1 mapping
  input1 match {
    case pt: PojoTypeInfo[_] =>
      input1FieldMapping.getOrElse(
        throw new CodeGenException("No input mapping is specified for input1 of type POJO."))
    case _ => // ok
  }

  // check for POJO input2 mapping
  input2 match {
    case Some(pt: PojoTypeInfo[_]) =>
      input2FieldMapping.getOrElse(
        throw new CodeGenException("No input mapping is specified for input2 of type POJO."))
    case _ => // ok
  }

  protected val input1Mapping: Array[Int] = input1FieldMapping match {
    case Some(mapping) => mapping
    case _ => (0 until input1.getArity).toArray
  }

  protected val input2Mapping: Array[Int] = input2FieldMapping match {
    case Some(mapping) => mapping
    case _ => input2 match {
      case Some(input) => (0 until input.getArity).toArray
      case _ => Array[Int]()
    }
  }

  // set of member statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  val reusableMemberStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of constructor statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  val reusableInitStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of open statements for RichFunction that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  val reusableOpenStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of close statements for RichFunction that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  val reusableCloseStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of statements that will be added only once per record;
  // code should only update member variables because local variables are not accessible if
  // the code needs to be split;
  // we use a LinkedHashSet to keep the insertion order
  val reusablePerRecordStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // map of initial input unboxing expressions that will be added only once
  // (inputTerm, index) -> expr
  val reusableInputUnboxingExprs: mutable.Map[(String, Int), GeneratedExpression] =
    mutable.Map[(String, Int), GeneratedExpression]()

  // set of constructor statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  val reusableConstructorStatements: mutable.LinkedHashSet[(String, String)] =
    mutable.LinkedHashSet[(String, String)]()

  /**
    * Flag that indicates that the generated code needed to be split into several methods.
    */
  protected var hasCodeSplits: Boolean = false

  /**
    * @return code block of statements that need to be placed in the member area of the Function
    *         (e.g. member variables and their initialization)
    */
  def reuseMemberCode(): String = {
    reusableMemberStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that need to be placed in the constructor of the Function
    */
  def reuseInitCode(): String = {
    reusableInitStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that need to be placed in the open() method of RichFunction
    */
  def reuseOpenCode(): String = {
    reusableOpenStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that need to be placed in the close() method of RichFunction
    */
  def reuseCloseCode(): String = {
    reusableCloseStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that need to be placed in the SAM of the Function
    */
  def reusePerRecordCode(): String = {
    reusablePerRecordStatements.mkString("", "\n", "\n")
  }

  /**
    * @return code block of statements that unbox input variables to a primitive variable
    *         and a corresponding null flag variable
    */
  def reuseInputUnboxingCode(): String = {
    reusableInputUnboxingExprs.values.map(_.code).mkString("", "\n", "\n")
  }

  /**
    * @return code block of constructor statements for the Function
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

  /**
    * @return term of the (casted and possibly boxed) first input
    */
  var input1Term = "in1"

  /**
    * @return term of the (casted and possibly boxed) second input
    */
  var input2Term = "in2"

  /**
    * @return term of the (casted) output collector
    */
  var collectorTerm = "c"

  /**
    * @return term of the output record (possibly defined in the member area e.g. Row, Tuple)
    */
  var outRecordTerm = "out"

  /**
    * @return term of the [[ProcessFunction]]'s context
    */
  var contextTerm = "ctx"

  /**
    * @return returns if null checking is enabled
    */
  def nullCheck: Boolean = config.getNullCheck

  /**
    * Generates an expression from a RexNode. If objects or variables can be reused, they will be
    * added to reusable code sections internally.
    *
    * @param rex Calcite row expression
    * @return instance of GeneratedExpression
    */
  def generateExpression(rex: RexNode): GeneratedExpression = {
    rex.accept(this)
  }

  /**
    * Generates an expression that converts the first input (and second input) into the given type.
    * If two inputs are converted, the second input is appended. If objects or variables can
    * be reused, they will be added to reusable code sections internally. The evaluation result
    * may be stored in the global result variable (see [[outRecordTerm]]).
    *
    * @param returnType conversion target type. Inputs and output must have the same arity.
    * @param resultFieldNames result field names necessary for a mapping to POJO fields.
    * @param rowtimeExpression an expression to extract the value of a rowtime field from
    *                          the input data. Required if the field indices include a rowtime
    *                          marker.
    * @return instance of GeneratedExpression
    */
  def generateConverterResultExpression(
      returnType: TypeInformation[_ <: Any],
      resultFieldNames: Seq[String],
      rowtimeExpression: Option[RexNode] = None)
    : GeneratedExpression = {

    val input1AccessExprs = input1Mapping.map {
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER |
           TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER if rowtimeExpression.isDefined =>
          // generate rowtime attribute from expression
          generateExpression(rowtimeExpression.get)
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER |
           TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER =>
          throw new TableException("Rowtime extraction expression missing. Please report a bug.")
      case TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER =>
        // attribute is proctime indicator.
        // we use a null literal and generate a timestamp when we need it.
        generateNullLiteral(TimeIndicatorTypeInfo.PROCTIME_INDICATOR)
      case TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER =>
        // attribute is proctime field in a batch query.
        // it is initialized with the current time.
        generateCurrentTimestamp()
      case idx =>
        generateInputAccess(input1, input1Term, idx)
    }

    val input2AccessExprs = input2 match {
      case Some(ti) =>
        input2Mapping.map(idx => generateInputAccess(ti, input2Term, idx)).toSeq
      case None => Seq() // add nothing
    }

    generateResultExpression(input1AccessExprs ++ input2AccessExprs, returnType, resultFieldNames)
  }

  /**
    * Generates an expression from the left input and the right table function.
    */
  def generateCorrelateAccessExprs: (Seq[GeneratedExpression], Seq[GeneratedExpression]) = {
    val input1AccessExprs = input1Mapping.map { idx =>
      generateInputAccess(input1, input1Term, idx)
    }

    val input2AccessExprs = input2 match {
      case Some(ti) =>
        // use generateFieldAccess instead of generateInputAccess to avoid the generated table
        // function's field access code is put on the top of function body rather than
        // the while loop
        input2Mapping.map { idx =>
          generateFieldAccess(ti, input2Term, idx)
        }.toSeq
      case None => throw new CodeGenException("Type information of input2 must not be null.")
    }
    (input1AccessExprs, input2AccessExprs)
  }

  /**
    * Generates an expression from a sequence of RexNode. If objects or variables can be reused,
    * they will be added to reusable code sections internally. The evaluation result
    * may be stored in the global result variable (see [[outRecordTerm]]).
    *
    * @param returnType conversion target type. Type must have the same arity than rexNodes.
    * @param resultFieldNames result field names necessary for a mapping to POJO fields.
    * @param rexNodes sequence of RexNode to be converted
    * @return instance of GeneratedExpression
    */
  def generateResultExpression(
      returnType: TypeInformation[_ <: Any],
      resultFieldNames: Seq[String],
      rexNodes: Seq[RexNode])
    : GeneratedExpression = {
    val fieldExprs = rexNodes.map(generateExpression)
    generateResultExpression(fieldExprs, returnType, resultFieldNames)
  }

  /**
    * Generates an expression from a sequence of other expressions. If objects or variables can
    * be reused, they will be added to reusable code sections internally. The evaluation result
    * may be stored in the global result variable (see [[outRecordTerm]]).
    *
    * @param fieldExprs field expressions to be converted
    * @param returnType conversion target type. Type must have the same arity than fieldExprs.
    * @param resultFieldNames result field names necessary for a mapping to POJO fields.
    * @return instance of GeneratedExpression
    */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      returnType: TypeInformation[_ <: Any],
      resultFieldNames: Seq[String])
    : GeneratedExpression = {
    // initial type check
    if (returnType.getArity != fieldExprs.length) {
      throw new CodeGenException(
        s"Arity [${returnType.getArity}] of result type [$returnType] does not match " +
        s"number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    if (resultFieldNames.length != fieldExprs.length) {
      throw new CodeGenException(
        s"Arity [${resultFieldNames.length}] of result field names [$resultFieldNames] does not " +
        s"match number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    // type check
    returnType match {
      case pt: PojoTypeInfo[_] =>
        fieldExprs.zipWithIndex foreach {
          case (fieldExpr, i) if fieldExpr.resultType != pt.getTypeAt(resultFieldNames(i)) =>
            throw new CodeGenException(
              s"Incompatible types of expression and result type. Expression [$fieldExpr] type is" +
              s" [${fieldExpr.resultType}], result type is [${pt.getTypeAt(resultFieldNames(i))}]")

          case _ => // ok
        }

      case ct: CompositeType[_] =>
        fieldExprs.zipWithIndex foreach {
          case (fieldExpr, i) if fieldExpr.resultType != ct.getTypeAt(i) =>
            throw new CodeGenException(
              s"Incompatible types of expression and result type. Expression[$fieldExpr] type is " +
              s"[${fieldExpr.resultType}], result type is [${ct.getTypeAt(i)}]")
          case _ => // ok
        }

      case t: TypeInformation[_] if t != fieldExprs.head.resultType =>
        throw new CodeGenException(
          s"Incompatible types of expression and result type. Expression [${fieldExprs.head}] " +
          s"type is [${fieldExprs.head.resultType}], result type is [$t]")

      case _ => // ok
    }

    val returnTypeTerm = boxedTypeTermForTypeInfo(returnType)
    val boxedFieldExprs = fieldExprs.map(generateOutputFieldBoxing)

    // generate result expression
    returnType match {
      case ri: RowTypeInfo =>
        addReusableOutRecord(ri)
        val resultSetters = boxedFieldExprs.zipWithIndex map {
          case (fieldExpr, i) =>
            if (nullCheck) {
              s"""
              |${fieldExpr.code}
              |if (${fieldExpr.nullTerm}) {
              |  $outRecordTerm.setField($i, null);
              |}
              |else {
              |  $outRecordTerm.setField($i, ${fieldExpr.resultTerm});
              |}
              |""".stripMargin
            }
            else {
              s"""
              |${fieldExpr.code}
              |$outRecordTerm.setField($i, ${fieldExpr.resultTerm});
              |""".stripMargin
            }
        }

        val code = generateCodeSplits(resultSetters)

        GeneratedExpression(outRecordTerm, NEVER_NULL, code, returnType)

      case pt: PojoTypeInfo[_] =>
        addReusableOutRecord(pt)
        val resultSetters = boxedFieldExprs.zip(resultFieldNames) map {
          case (fieldExpr, fieldName) =>
            val accessor = getFieldAccessor(pt.getTypeClass, fieldName)

            accessor match {
              // Reflective access of primitives/Objects
              case ObjectPrivateFieldAccessor(field) =>
                val fieldTerm = addReusablePrivateFieldAccess(pt.getTypeClass, fieldName)

                val defaultIfNull = if (isFieldPrimitive(field)) {
                  primitiveDefaultValue(fieldExpr.resultType)
                } else {
                  "null"
                }

                if (nullCheck) {
                  s"""
                    |${fieldExpr.code}
                    |if (${fieldExpr.nullTerm}) {
                    |  ${reflectiveFieldWriteAccess(
                          fieldTerm,
                          field,
                          outRecordTerm,
                          defaultIfNull)};
                    |}
                    |else {
                    |  ${reflectiveFieldWriteAccess(
                          fieldTerm,
                          field,
                          outRecordTerm,
                          fieldExpr.resultTerm)};
                    |}
                    |""".stripMargin
                }
                else {
                  s"""
                    |${fieldExpr.code}
                    |${reflectiveFieldWriteAccess(
                          fieldTerm,
                          field,
                          outRecordTerm,
                          fieldExpr.resultTerm)};
                    |""".stripMargin
                }

              // primitive or Object field access (implicit boxing)
              case _ =>
                if (nullCheck) {
                  s"""
                    |${fieldExpr.code}
                    |if (${fieldExpr.nullTerm}) {
                    |  $outRecordTerm.$fieldName = null;
                    |}
                    |else {
                    |  $outRecordTerm.$fieldName = ${fieldExpr.resultTerm};
                    |}
                    |""".stripMargin
                }
                else {
                  s"""
                    |${fieldExpr.code}
                    |$outRecordTerm.$fieldName = ${fieldExpr.resultTerm};
                    |""".stripMargin
                }
              }
          }

        val code = generateCodeSplits(resultSetters)

        GeneratedExpression(outRecordTerm, NEVER_NULL, code, returnType)

      case tup: TupleTypeInfo[_] =>
        addReusableOutRecord(tup)
        val resultSetters = boxedFieldExprs.zipWithIndex map {
          case (fieldExpr, i) =>
            val fieldName = "f" + i
            if (nullCheck) {
              s"""
                |${fieldExpr.code}
                |if (${fieldExpr.nullTerm}) {
                |  throw new NullPointerException("Null result cannot be stored in a Tuple.");
                |}
                |else {
                |  $outRecordTerm.$fieldName = ${fieldExpr.resultTerm};
                |}
                |""".stripMargin
            }
            else {
              s"""
                |${fieldExpr.code}
                |$outRecordTerm.$fieldName = ${fieldExpr.resultTerm};
                |""".stripMargin
            }
        }

        val code = generateCodeSplits(resultSetters)

        GeneratedExpression(outRecordTerm, NEVER_NULL, code, returnType)

      case _: CaseClassTypeInfo[_] =>
        val fieldCodes: String = boxedFieldExprs.map(_.code).mkString("\n")
        val constructorParams: String = boxedFieldExprs.map(_.resultTerm).mkString(", ")
        val resultTerm = newName(outRecordTerm)

        val nullCheckCode = if (nullCheck) {
        boxedFieldExprs map { (fieldExpr) =>
          s"""
              |if (${fieldExpr.nullTerm}) {
              |  throw new NullPointerException("Null result cannot be stored in a Case Class.");
              |}
              |""".stripMargin
          } mkString "\n"
        } else {
          ""
        }

        val resultCode =
          s"""
            |$fieldCodes
            |$nullCheckCode
            |$returnTypeTerm $resultTerm = new $returnTypeTerm($constructorParams);
            |""".stripMargin

        // case classes are not splittable
        GeneratedExpression(resultTerm, NEVER_NULL, resultCode, returnType)

      case _: TypeInformation[_] =>
        val fieldExpr = boxedFieldExprs.head
        val nullCheckCode = if (nullCheck) {
          s"""
          |if (${fieldExpr.nullTerm}) {
          |  throw new NullPointerException("Null result cannot be used for atomic types.");
          |}
          |""".stripMargin
        } else {
          ""
        }
        val resultCode =
          s"""
            |${fieldExpr.code}
            |$nullCheckCode
            |""".stripMargin

        // other types are not splittable
        GeneratedExpression(fieldExpr.resultTerm, fieldExpr.nullTerm, resultCode, returnType)

      case _ =>
        throw new CodeGenException(s"Unsupported result type: $returnType")
    }
  }

  // ----------------------------------------------------------------------------------------------
  // RexVisitor methods
  // ----------------------------------------------------------------------------------------------

  override def visitInputRef(inputRef: RexInputRef): GeneratedExpression = {
    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    val input = if (inputRef.getIndex < input1.getArity) {
      (input1, input1Term)
    } else {
      (input2.getOrElse(throw new CodeGenException("Invalid input access.")), input2Term)
    }

    val index = if (input._2 == input1Term) {
      inputRef.getIndex
    } else {
      inputRef.getIndex - input1.getArity
    }

    generateInputAccess(input._1, input._2, index)
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): GeneratedExpression =
    visitInputRef(rexTableInputRef)

  override def visitFieldAccess(rexFieldAccess: RexFieldAccess): GeneratedExpression = {
    val refExpr = rexFieldAccess.getReferenceExpr.accept(this)
    val index = rexFieldAccess.getField.getIndex
    generateFieldAccess(refExpr, index)
  }

  override def visitLiteral(literal: RexLiteral): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toTypeInfo(literal.getType)
    val value = literal.getValue3
    // null value with type
    if (value == null) {
      return generateNullLiteral(resultType)
    }
    // non-null values
    literal.getType.getSqlTypeName match {

      case BOOLEAN =>
        generateNonNullLiteral(resultType, literal.getValue3.toString)

      case TINYINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidByte) {
          generateNonNullLiteral(resultType, decimal.byteValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to byte.")
        }

      case SMALLINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidShort) {
          generateNonNullLiteral(resultType, decimal.shortValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to short.")
        }

      case INTEGER =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          generateNonNullLiteral(resultType, decimal.intValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to integer.")
        }

      case BIGINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(resultType, decimal.longValue().toString + "L")
        }
        else {
          throw new CodeGenException("Decimal can not be converted to long.")
        }

      case FLOAT =>
        val floatValue = value.asInstanceOf[JBigDecimal].floatValue()
        floatValue match {
          case Float.NaN => generateNonNullLiteral(resultType, "java.lang.Float.NaN")
          case Float.NegativeInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Float.NEGATIVE_INFINITY")
          case Float.PositiveInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Float.POSITIVE_INFINITY")
          case _ => generateNonNullLiteral(resultType, floatValue.toString + "f")
        }

      case DOUBLE =>
        val doubleValue = value.asInstanceOf[JBigDecimal].doubleValue()
        doubleValue match {
          case Double.NaN => generateNonNullLiteral(resultType, "java.lang.Double.NaN")
          case Double.NegativeInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Double.NEGATIVE_INFINITY")
          case Double.PositiveInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Double.POSITIVE_INFINITY")
          case _ => generateNonNullLiteral(resultType, doubleValue.toString + "d")
        }
      case DECIMAL =>
        val decimalField = addReusableDecimal(value.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(resultType, decimalField)

      case VARCHAR | CHAR =>
        val escapedValue = EncodingUtils.escapeJava(value.toString)
        generateNonNullLiteral(resultType, "\"" + escapedValue + "\"")

      case SYMBOL =>
        generateSymbol(value.asInstanceOf[Enum[_]])

      case DATE =>
        generateNonNullLiteral(resultType, value.toString)

      case TIME =>
        generateNonNullLiteral(resultType, value.toString)

      case TIMESTAMP =>
        generateNonNullLiteral(resultType, value.toString + "L")

      case typeName if YEAR_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          generateNonNullLiteral(resultType, decimal.intValue().toString)
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of months.")
        }

      case typeName if DAY_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(resultType, decimal.longValue().toString + "L")
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of milliseconds.")
        }

      case t@_ =>
        throw new CodeGenException(s"Type not supported: $t")
    }
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): GeneratedExpression = {
    GeneratedExpression(input1Term, NEVER_NULL, NO_CODE, input1)
  }

  override def visitLocalRef(localRef: RexLocalRef): GeneratedExpression =
    throw new CodeGenException("Local variables are not supported yet.")

  override def visitRangeRef(rangeRef: RexRangeRef): GeneratedExpression =
    throw new CodeGenException("Range references are not supported yet.")

  override def visitDynamicParam(dynamicParam: RexDynamicParam): GeneratedExpression =
    throw new CodeGenException("Dynamic parameter references are not supported yet.")

  override def visitCall(call: RexCall): GeneratedExpression = {

    // special case: time materialization
    if (call.getOperator == ProctimeSqlFunction) {
      return generateProctimeTimestamp()
    }

    val resultType = FlinkTypeFactory.toTypeInfo(call.getType)

    // convert operands and help giving untyped NULL literals a type
    val operands = call.getOperands.zipWithIndex.map {

      // this helps e.g. for AS(null)
      // we might need to extend this logic in case some rules do not create typed NULLs
      case (operandLiteral: RexLiteral, 0) if
          operandLiteral.getType.getSqlTypeName == SqlTypeName.NULL &&
          call.getOperator.getReturnTypeInference == ReturnTypes.ARG0 =>
        generateNullLiteral(resultType)

      case (o@_, _) =>
        o.accept(this)
    }

    call.getOperator match {
      // arithmetic
      case PLUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("+", nullCheck, resultType, left, right, config)

      case PLUS | DATETIME_PLUS if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(plus = true, nullCheck, resultType, left, right, config)

      case MINUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("-", nullCheck, resultType, left, right, config)

      case MINUS | MINUS_DATE if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(plus = false, nullCheck, resultType, left, right, config)

      case MULTIPLY if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("*", nullCheck, resultType, left, right, config)

      case MULTIPLY if isTimeInterval(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTimeInterval(left)
        requireNumeric(right)
        generateArithmeticOperator("*", nullCheck, resultType, left, right, config)

      case DIVIDE | DIVIDE_INTEGER if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("/", nullCheck, resultType, left, right, config)

      case MOD if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("%", nullCheck, resultType, left, right, config)

      case UNARY_MINUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator("-", nullCheck, resultType, operand)

      case UNARY_MINUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(plus = false, nullCheck, operand)

      case UNARY_PLUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator("+", nullCheck, resultType, operand)

      case UNARY_PLUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(plus = true, nullCheck, operand)

      // comparison
      case EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateEquals(nullCheck, left, right)

      case IS_NOT_DISTINCT_FROM =>
        val left = operands.head
        val right = operands(1)
        generateIsNotDistinctFrom(nullCheck, left, right);

      case NOT_EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateNotEquals(nullCheck, left, right)

      case GREATER_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(">", nullCheck, left, right)

      case GREATER_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(">=", nullCheck, left, right)

      case LESS_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison("<", nullCheck, left, right)

      case LESS_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison("<=", nullCheck, left, right)

      case IS_NULL =>
        val operand = operands.head
        generateIsNull(nullCheck, operand)

      case IS_NOT_NULL =>
        val operand = operands.head
        generateIsNotNull(nullCheck, operand)

      // logic
      case AND =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left)
          requireBoolean(right)
          generateAnd(nullCheck, left, right)
        }

      case OR =>
        operands.reduceLeft { (left: GeneratedExpression, right: GeneratedExpression) =>
          requireBoolean(left)
          requireBoolean(right)
          generateOr(nullCheck, left, right)
        }

      case NOT =>
        val operand = operands.head
        requireBoolean(operand)
        generateNot(nullCheck, operand)

      case CASE =>
        generateIfElse(nullCheck, operands, resultType)

      case IS_TRUE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsTrue(operand)

      case IS_NOT_TRUE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsNotTrue(operand)

      case IS_FALSE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsFalse(operand)

      case IS_NOT_FALSE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsNotFalse(operand)

      case IN =>
        val left = operands.head
        val right = operands.tail
        generateIn(this, left, right)

      case NOT_IN =>
        val left = operands.head
        val right = operands.tail
        generateNot(nullCheck, generateIn(this, left, right))

      // casting
      case CAST | REINTERPRET =>
        val operand = operands.head
        generateCast(nullCheck, operand, resultType)

      // as / renaming
      case AS =>
        operands.head

      // string arithmetic
      case CONCAT =>
        val left = operands.head
        val right = operands(1)
        requireString(left)
        generateArithmeticOperator("+", nullCheck, resultType, left, right, config)

      // rows
      case ROW =>
        generateRow(this, resultType, operands)

      // arrays
      case ARRAY_VALUE_CONSTRUCTOR =>
        generateArray(this, resultType, operands)

      // maps
      case MAP_VALUE_CONSTRUCTOR =>
        generateMap(this, resultType, operands)

      case ITEM =>
        operands.head.resultType match {
          case t: TypeInformation[_] if isArray(t) =>
            val array = operands.head
            val index = operands(1)
            requireInteger(index)
            generateArrayElementAt(this, array, index)

          case t: TypeInformation[_] if isMap(t) =>
            val key = operands(1)
            generateMapGet(this, operands.head, key)

          case _ => throw new CodeGenException("Expect an array or a map.")
        }

      case CARDINALITY =>
        operands.head.resultType match {
          case t: TypeInformation[_] if isArray(t) =>
            val array = operands.head
            generateArrayCardinality(nullCheck, array)

          case t: TypeInformation[_] if isMap(t) =>
            val map = operands.head
            generateMapCardinality(nullCheck, map)

          case _ => throw new CodeGenException("Expect an array or a map.")
        }

      case ELEMENT =>
        val array = operands.head
        requireArray(array)
        generateArrayElement(this, array)

      case DOT =>
        // Due to https://issues.apache.org/jira/browse/CALCITE-2162, expression such as
        // "array[1].a.b" won't work now.
        if (operands.size > 2) {
          throw new CodeGenException(
            "A DOT operator with more than 2 operands is not supported yet.")
        }
        val fieldName = call.operands.get(1).asInstanceOf[RexLiteral].getValueAs(classOf[String])
        val fieldIdx = operands
          .head
          .resultType
          .asInstanceOf[CompositeType[_]]
          .getFieldIndex(fieldName)
        generateFieldAccess(operands.head, fieldIdx)

      case ScalarSqlFunctions.CONCAT =>
        generateConcat(this.nullCheck, operands)

      case ScalarSqlFunctions.CONCAT_WS =>
        generateConcatWs(operands)

      case StreamRecordTimestampSqlFunction =>
        generateStreamRecordRowtimeAccess()

      // advanced scalar functions
      case sqlOperator: SqlOperator =>
        val callGen = FunctionGenerator.getCallGenerator(
          sqlOperator,
          operands.map(_.resultType),
          resultType)
        callGen
          .getOrElse(throw new CodeGenException(s"Unsupported call: $sqlOperator \n" +
            s"If you think this function should be supported, " +
            s"you can create an issue and start a discussion for it."))
          .generate(this, operands)

      // unknown or invalid
      case call@_ =>
        throw new CodeGenException(s"Unsupported call: $call")
    }
  }

  override def visitOver(over: RexOver): GeneratedExpression =
    throw new CodeGenException("Aggregate functions over windows are not supported yet.")

  override def visitSubQuery(subQuery: RexSubQuery): GeneratedExpression =
    throw new CodeGenException("Subqueries are not supported yet.")

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression =
    throw new CodeGenException("Pattern field references are not supported yet.")

  // ----------------------------------------------------------------------------------------------
  // generator helping methods
  // ----------------------------------------------------------------------------------------------
  protected def makeReusableInSplits(expr: GeneratedExpression): GeneratedExpression = {
    // prepare declaration in class
    val resultTypeTerm = primitiveTypeTermForTypeInfo(expr.resultType)
    if (nullCheck && !expr.nullTerm.equals(NEVER_NULL) && !expr.nullTerm.equals(ALWAYS_NULL)) {
      reusableMemberStatements.add(s"private boolean ${expr.nullTerm};")
    }
    reusableMemberStatements.add(s"private $resultTypeTerm ${expr.resultTerm};")

    // when expr has no code, no need to split it into a method, but still need to assign
    if (expr.code.isEmpty) {
      if (nullCheck && !expr.nullTerm.equals(NEVER_NULL) && !expr.nullTerm.equals(ALWAYS_NULL)) {
        reusablePerRecordStatements.add(s"this.${expr.nullTerm} = ${expr.nullTerm};")
      }
      reusablePerRecordStatements.add(s"this.${expr.resultTerm} = ${expr.resultTerm};")
      expr
    } else {
      // create a method for the unboxing block
      val methodName = newName(s"inputUnboxingSplit")
      val method =
        if (nullCheck && !expr.nullTerm.equals(NEVER_NULL) && !expr.nullTerm.equals(ALWAYS_NULL)) {
          s"""
             |private final void $methodName() throws Exception {
             |  ${expr.code}
             |  this.${expr.nullTerm} = ${expr.nullTerm};
             |  this.${expr.resultTerm} = ${expr.resultTerm};
             |}
           """.stripMargin
        } else {
          s"""
             |private final void $methodName() throws Exception {
             |  ${expr.code}
             |  this.${expr.resultTerm} = ${expr.resultTerm};
             |}
           """.stripMargin
        }

      // add this method to reusable section for later generation
      reusableMemberStatements.add(method)

      // create method call
      GeneratedExpression(
        expr.resultTerm,
        expr.nullTerm,
        s"$methodName();",
        expr.resultType)
    }
  }

  private def generateCodeSplits(splits: Seq[String]): String = {
    val totalLen = splits.map(_.length + 1).sum // 1 for a line break

    // split
    if (totalLen > config.getMaxGeneratedCodeLength) {

      hasCodeSplits = true

      // add input unboxing to member area such that all split functions can access it
      reusableInputUnboxingExprs.keys.foreach(
        key =>
          reusableInputUnboxingExprs(key) = makeReusableInSplits(reusableInputUnboxingExprs(key)))

      // add split methods to the member area and return the code necessary to call those methods
      val methodCalls = splits.map { split =>
        val methodName = newName(s"split")

        val method =
          s"""
            |private final void $methodName() throws Exception {
            |  $split
            |}
            |""".stripMargin
        reusableMemberStatements.add(method)

        // create method call
        s"$methodName();"
      }

      methodCalls.mkString("\n")
    }
    // don't split
    else {
      splits.mkString("\n")
    }
  }

  protected def generateFieldAccess(refExpr: GeneratedExpression, index: Int)
    : GeneratedExpression = {

    val fieldAccessExpr = generateFieldAccess(
      refExpr.resultType,
      refExpr.resultTerm,
      index)

    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(fieldAccessExpr.resultType)
    val defaultValue = primitiveDefaultValue(fieldAccessExpr.resultType)
    val resultCode = if (nullCheck) {
      s"""
        |${refExpr.code}
        |$resultTypeTerm $resultTerm;
        |boolean $nullTerm;
        |if (${refExpr.nullTerm}) {
        |  $resultTerm = $defaultValue;
        |  $nullTerm = true;
        |}
        |else {
        |  ${fieldAccessExpr.code}
        |  $resultTerm = ${fieldAccessExpr.resultTerm};
        |  $nullTerm = ${fieldAccessExpr.nullTerm};
        |}
        |""".stripMargin
    } else {
      s"""
        |${refExpr.code}
        |${fieldAccessExpr.code}
        |$resultTypeTerm $resultTerm = ${fieldAccessExpr.resultTerm};
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, fieldAccessExpr.resultType)
  }

  protected def generateInputAccess(
      inputType: TypeInformation[_ <: Any],
      inputTerm: String,
      index: Int)
    : GeneratedExpression = {
    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = reusableInputUnboxingExprs.get((inputTerm, index)) match {
      // input access and unboxing has already been generated
      case Some(expr) =>
        expr

      // generate input access and unboxing if necessary
      case None =>
        val expr = if (nullableInput) {
          generateNullableInputFieldAccess(inputType, inputTerm, index)
        } else {
          generateFieldAccess(inputType, inputTerm, index)
        }

        reusableInputUnboxingExprs((inputTerm, index)) = expr
        expr
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }

  private def generateNullableInputFieldAccess(
      inputType: TypeInformation[_ <: Any],
      inputTerm: String,
      index: Int)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")

    val fieldType = inputType match {
      case ct: CompositeType[_] => ct.getTypeAt(index)
      case t: TypeInformation[_] => t
    }
    val resultTypeTerm = primitiveTypeTermForTypeInfo(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)
    val fieldAccessExpr = generateFieldAccess(inputType, inputTerm, index)

    val inputCheckCode =
      s"""
        |$resultTypeTerm $resultTerm;
        |boolean $nullTerm;
        |if ($inputTerm == null) {
        |  $resultTerm = $defaultValue;
        |  $nullTerm = true;
        |}
        |else {
        |  ${fieldAccessExpr.code}
        |  $resultTerm = ${fieldAccessExpr.resultTerm};
        |  $nullTerm = ${fieldAccessExpr.nullTerm};
        |}
        |""".stripMargin

    GeneratedExpression(resultTerm, nullTerm, inputCheckCode, fieldType)
  }

  protected def generateFieldAccess(
      inputType: TypeInformation[_],
      inputTerm: String,
      index: Int)
    : GeneratedExpression = {
    inputType match {

      case ct: CompositeType[_] =>
        val accessor = fieldAccessorFor(ct, index)
        val fieldType: TypeInformation[Any] = ct.getTypeAt(index)
        val fieldTypeTerm = boxedTypeTermForTypeInfo(fieldType)

        accessor match {
          case ObjectFieldAccessor(field) =>
            // primitive
            if (isFieldPrimitive(field)) {
              generateTerm(fieldType, s"$inputTerm.${field.getName}")
            }
            // Object
            else {
              generateInputFieldUnboxing(
                fieldType,
                s"($fieldTypeTerm) $inputTerm.${field.getName}")
            }

          case ObjectGenericFieldAccessor(fieldName) =>
            // Object
            val inputCode = s"($fieldTypeTerm) $inputTerm.$fieldName"
            generateInputFieldUnboxing(fieldType, inputCode)

          case ObjectMethodAccessor(methodName) =>
            // Object
            val inputCode = s"($fieldTypeTerm) $inputTerm.$methodName()"
            generateInputFieldUnboxing(fieldType, inputCode)

          case ProductAccessor(i) =>
            // Object
            val inputCode = s"($fieldTypeTerm) $inputTerm.getField($i)"
            generateInputFieldUnboxing(fieldType, inputCode)

          case ObjectPrivateFieldAccessor(field) =>
            val fieldTerm = addReusablePrivateFieldAccess(ct.getTypeClass, field.getName)
            val reflectiveAccessCode = reflectiveFieldReadAccess(fieldTerm, field, inputTerm)
            // primitive
            if (isFieldPrimitive(field)) {
              generateTerm(fieldType, reflectiveAccessCode)
            }
            // Object
            else {
              generateInputFieldUnboxing(fieldType, reflectiveAccessCode)
            }
        }

      case t: TypeInformation[_] =>
        val fieldTypeTerm = boxedTypeTermForTypeInfo(t)
        val inputCode = s"($fieldTypeTerm) $inputTerm"
        generateInputFieldUnboxing(t, inputCode)
    }
  }

  protected def generateNullLiteral(resultType: TypeInformation[_]): GeneratedExpression = {
    val resultTerm = newName("result")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)
    val defaultValue = primitiveDefaultValue(resultType)

    if (nullCheck) {
      val wrappedCode = s"""
        |$resultTypeTerm $resultTerm = $defaultValue;
        |""".stripMargin

      // mark this expression as a constant literal
      GeneratedExpression(resultTerm, ALWAYS_NULL, wrappedCode, resultType, literal = true)
    } else {
      throw new CodeGenException("Null literals are not allowed if nullCheck is disabled.")
    }
  }

  private[flink] def generateNonNullLiteral(
      literalType: TypeInformation[_],
      literalCode: String)
    : GeneratedExpression = {

    // mark this expression as a constant literal
    generateTerm(literalType, literalCode).copy(literal = true)
  }

  private[flink] def generateSymbol(enum: Enum[_]): GeneratedExpression = {
    GeneratedExpression(
      qualifyEnum(enum),
      NEVER_NULL,
      NO_CODE,
      new GenericTypeInfo(enum.getDeclaringClass))
  }

  /**
    * Generates access to a term (e.g. a field) that does not require unboxing logic.
    *
    * @param fieldType type of field
    * @param fieldTerm expression term of field (already unboxed)
    * @return internal unboxed field representation
    */
  private[flink] def generateTerm(
      fieldType: TypeInformation[_],
      fieldTerm: String)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(fieldType)

    val resultCode = s"""
        |$resultTypeTerm $resultTerm = $fieldTerm;
        |""".stripMargin

    GeneratedExpression(resultTerm, NEVER_NULL, resultCode, fieldType)
  }

  /**
    * Converts the external boxed format to an internal mostly primitive field representation.
    * Wrapper types can autoboxed to their corresponding primitive type (Integer -> int). External
    * objects are converted to their internal representation (Timestamp -> internal timestamp
    * in long).
    *
    * @param fieldType type of field
    * @param fieldTerm expression term of field to be unboxed
    * @return internal unboxed field representation
    */
  private[flink] def generateInputFieldUnboxing(
      fieldType: TypeInformation[_],
      fieldTerm: String)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(fieldType)
    val defaultValue = primitiveDefaultValue(fieldType)

    // explicit unboxing
    val unboxedFieldCode = if (isTimePoint(fieldType)) {
      timePointToInternalCode(fieldType, fieldTerm)
    } else {
      fieldTerm
    }

    val wrappedCode = if (nullCheck && !isReference(fieldType)) {
      // assumes that fieldType is a boxed primitive.
      s"""
        |boolean $nullTerm = $fieldTerm == null;
        |$resultTypeTerm $resultTerm;
        |if ($nullTerm) {
        |  $resultTerm = $defaultValue;
        |}
        |else {
        |  $resultTerm = $fieldTerm;
        |}
        |""".stripMargin
    } else if (nullCheck) {
      s"""
        |boolean $nullTerm = $fieldTerm == null;
        |$resultTypeTerm $resultTerm;
        |if ($nullTerm) {
        |  $resultTerm = $defaultValue;
        |}
        |else {
        |  $resultTerm = ($resultTypeTerm) $unboxedFieldCode;
        |}
        |""".stripMargin
    } else {
      s"""
        |$resultTypeTerm $resultTerm = ($resultTypeTerm) $unboxedFieldCode;
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, wrappedCode, fieldType)
  }

  /**
    * Converts the internal mostly primitive field representation to an external boxed format.
    * Primitive types can autoboxed to their corresponding object type (int -> Integer). Internal
    * representations are converted to their external objects (internal timestamp
    * in long -> Timestamp).
    *
    * @param expr expression to be boxed
    * @return external boxed field representation
    */
  private[flink] def generateOutputFieldBoxing(expr: GeneratedExpression): GeneratedExpression = {
    expr.resultType match {
      // convert internal date/time/timestamp to java.sql.* objects
      case SqlTimeTypeInfo.DATE | SqlTimeTypeInfo.TIME | SqlTimeTypeInfo.TIMESTAMP =>
        val resultTerm = newName("result")
        val resultTypeTerm = boxedTypeTermForTypeInfo(expr.resultType)
        val convMethod = internalToTimePointCode(expr.resultType, expr.resultTerm)

        val resultCode = if (nullCheck) {
          s"""
            |${expr.code}
            |$resultTypeTerm $resultTerm;
            |if (${expr.nullTerm}) {
            |  $resultTerm = null;
            |}
            |else {
            |  $resultTerm = $convMethod;
            |}
            |""".stripMargin
        } else {
          s"""
            |${expr.code}
            |$resultTypeTerm $resultTerm = $convMethod;
            |""".stripMargin
        }

        GeneratedExpression(resultTerm, expr.nullTerm, resultCode, expr.resultType)

      // other types are autoboxed or need no boxing
      case _ => expr
    }
  }

  private[flink] def generateNullableOutputBoxing(
      expr: GeneratedExpression,
      typeInfo: TypeInformation[_])
    : GeneratedExpression = {
    val boxedExpr = generateOutputFieldBoxing(generateCast(nullCheck, expr, typeInfo))
    val boxedTypeTerm = boxedTypeTermForTypeInfo(typeInfo)
    val exprOrNull: String = if (nullCheck) {
      s"${boxedExpr.nullTerm} ? null : ($boxedTypeTerm) ${boxedExpr.resultTerm}"
    } else {
      boxedExpr.resultTerm
    }
    boxedExpr.copy(resultTerm = exprOrNull)
  }

  private[flink] def generateStreamRecordRowtimeAccess(): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")

    val accessCode =
      s"""
        |Long $resultTerm = $contextTerm.timestamp();
        |if ($resultTerm == null) {
        |  throw new RuntimeException("Rowtime timestamp is null. Please make sure that a proper " +
        |    "TimestampAssigner is defined and the stream environment uses the EventTime time " +
        |    "characteristic.");
        |}
        |boolean $nullTerm = false;
       """.stripMargin

    GeneratedExpression(resultTerm, nullTerm, accessCode, Types.LONG)
  }

  private[flink] def generateProctimeTimestamp(): GeneratedExpression = {
    val resultTerm = newName("result")

    val resultCode =
      s"""
        |long $resultTerm = $contextTerm.timerService().currentProcessingTime();
        |""".stripMargin
    GeneratedExpression(resultTerm, NEVER_NULL, resultCode, SqlTimeTypeInfo.TIMESTAMP)
  }

  private[flink] def generateCurrentTimestamp(): GeneratedExpression = {
    new CurrentTimePointCallGen(Types.SQL_TIMESTAMP, false).generate(this, Seq())
  }

  // ----------------------------------------------------------------------------------------------
  // Reusable code snippets
  // ----------------------------------------------------------------------------------------------

  /**
    * Adds a reusable output record to the member area of the generated [[Function]].
    * The passed [[TypeInformation]] defines the type class to be instantiated.
    *
    * @param ti type information of type class to be instantiated during runtime
    * @return member variable term
    */
  def addReusableOutRecord(ti: TypeInformation[_]): Unit = {
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
    * Adds a reusable [[java.lang.reflect.Field]] to the member area of the generated [[Function]].
    * The field can be used for accessing POJO fields more efficiently during runtime, however,
    * the field does not have to be public.
    *
    * @param clazz class of containing field
    * @param fieldName name of field to be extracted and instantiated during runtime
    * @return member variable term
    */
  def addReusablePrivateFieldAccess(clazz: Class[_], fieldName: String): String = {
    val fieldTerm = s"field_${clazz.getCanonicalName.replace('.', '$')}_$fieldName"
    val fieldExtraction =
      s"""
        |final java.lang.reflect.Field $fieldTerm =
        |    org.apache.flink.api.java.typeutils.TypeExtractor.getDeclaredField(
        |      ${clazz.getCanonicalName}.class, "$fieldName");
        |""".stripMargin
    reusableMemberStatements.add(fieldExtraction)

    val fieldAccessibility =
      s"""
        |$fieldTerm.setAccessible(true);
        |""".stripMargin
    reusableInitStatements.add(fieldAccessibility)

    fieldTerm
  }

  /**
    * Adds a reusable [[java.math.BigDecimal]] to the member area of the generated [[Function]].
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
          |final java.math.BigDecimal $fieldTerm =
          |    new java.math.BigDecimal("${decimal.toString}");
          |""".stripMargin
      reusableMemberStatements.add(fieldDecimal)
      fieldTerm
  }

  /**
    * Adds a reusable [[java.util.Random]] to the member area of the generated [[Function]].
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
    reusableMemberStatements.add(field)

    val fieldInit = seedExpr match {
      case Some(s) if nullCheck =>
        s"""
         |${s.code}
         |if(!${s.nullTerm}) {
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

    reusableInitStatements.add(fieldInit)
    fieldTerm
  }

  /**
    * Adds a reusable DateFormatter to the member area of the generated [[Function]].
    *
    * @return member variable term
    */
  def addReusableDateFormatter(format: GeneratedExpression): String = {
    val fieldTerm = newName("dateFormatter")

    val field =
      s"""
         |final ${classOf[DateTimeFormatter].getCanonicalName} $fieldTerm;
         |""".stripMargin
    reusableMemberStatements.add(field)

    val fieldInit =
      s"""
         |${format.code}
         |$fieldTerm = org.apache.flink.table.runtime.functions.
         |DateTimeFunctions$$.MODULE$$.createDateTimeFormatter(${format.resultTerm});
         |""".stripMargin

    reusableInitStatements.add(fieldInit)
    fieldTerm
  }

  /**
    * Adds a reusable [[UserDefinedFunction]] to the member area of the generated [[Function]].
    *
    * @param function [[UserDefinedFunction]] object to be instantiated during runtime
    * @param contextTerm [[RuntimeContext]] term to access the [[RuntimeContext]]
    * @return member variable term
    */
  def addReusableFunction(
      function: UserDefinedFunction,
      contextTerm: String = null,
      functionContextClass: Class[_ <: FunctionContext] = classOf[FunctionContext]): String = {
    val classQualifier = function.getClass.getCanonicalName
    val functionSerializedData = EncodingUtils.encodeObjectToString(function)
    val fieldTerm = s"function_${function.functionIdentifier}"

    val fieldFunction =
      s"""
        |final $classQualifier $fieldTerm;
        |""".stripMargin
    reusableMemberStatements.add(fieldFunction)

    val functionDeserialization =
      s"""
         |$fieldTerm = ($classQualifier)
         |${classOf[EncodingUtils].getCanonicalName}.decodeStringToObject(
         |  "$functionSerializedData",
         |  ${classOf[UserDefinedFunction].getCanonicalName}.class);
       """.stripMargin

    reusableInitStatements.add(functionDeserialization)

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
    * Adds a reusable constructor statement with the given parameter types.
    *
    * @param parameterTypes The parameter types to construct the function
    * @return member variable terms
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
    * Adds a reusable [[org.apache.flink.types.Row]]
    * to the member area of the generated [[Function]].
    */
  def addReusableRow(arity: Int): String = {
    val fieldTerm = newName("row")
    val fieldRow =
      s"""
         |final org.apache.flink.types.Row $fieldTerm =
         |    new org.apache.flink.types.Row($arity);
         |""".stripMargin
    reusableMemberStatements.add(fieldRow)
    fieldTerm
  }

  /**
    * Adds a reusable array to the member area of the generated [[Function]].
    */
  def addReusableArray(clazz: Class[_], size: Int): String = {
    val fieldTerm = newName("array")
    val classQualifier = clazz.getCanonicalName // works also for int[] etc.
    val initArray = classQualifier.replaceFirst("\\[", s"[$size")
    val fieldArray =
      s"""
        |final $classQualifier $fieldTerm =
        |    new $initArray;
        |""".stripMargin
    reusableMemberStatements.add(fieldArray)
    fieldTerm
  }

  /**
    * Adds a reusable hash map to the member area of the generated [[Function]].
    */
  def addReusableMap(): String = {
    val fieldTerm = newName("map")
    val classQualifier = "java.util.Map"
    val initMap = "java.util.HashMap()"
    val fieldMap =
      s"""
         |final $classQualifier $fieldTerm =
         |    new $initMap;
         |""".stripMargin
    reusableMemberStatements.add(fieldMap)
    fieldTerm
  }

  /**
    * Adds a reusable timestamp to the beginning of the SAM of the generated [[Function]].
    */
  def addReusableTimestamp(): String = {
    val fieldTerm = s"timestamp"

    // declaration
    reusableMemberStatements.add(s"private long $fieldTerm;")

    // assignment
    val field =
      s"""
        |$fieldTerm = java.lang.System.currentTimeMillis();
        |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

    /**
    * Adds a reusable local timestamp to the beginning of the SAM of the generated [[Function]].
    */
  def addReusableLocalTimestamp(): String = {
    val fieldTerm = s"localtimestamp"

    val timestamp = addReusableTimestamp()

    // declaration
    reusableMemberStatements.add(s"private long $fieldTerm;")

    // assignment
    val field =
      s"""
        |$fieldTerm = $timestamp + java.util.TimeZone.getDefault().getOffset($timestamp);
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
        |$fieldTerm = (int) ($timestamp % ${DateTimeUtils.MILLIS_PER_DAY});
        |if (time < 0) {
        |  time += ${DateTimeUtils.MILLIS_PER_DAY};
        |}
        |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
    * Adds a reusable local time to the beginning of the SAM of the generated [[Function]].
    */
  def addReusableLocalTime(): String = {
    val fieldTerm = s"localtime"

    val localtimestamp = addReusableLocalTimestamp()

    // declaration
    reusableMemberStatements.add(s"private int $fieldTerm;")

    // assignment
    // adopted from org.apache.calcite.runtime.SqlFunctions.localTime()
    val field =
      s"""
        |$fieldTerm = (int) ($localtimestamp % ${DateTimeUtils.MILLIS_PER_DAY});
        |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }


  /**
    * Adds a reusable date to the beginning of the SAM of the generated [[Function]].
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
        |$fieldTerm = (int) ($timestamp / ${DateTimeUtils.MILLIS_PER_DAY});
        |if ($time < 0) {
        |  $fieldTerm -= 1;
        |}
        |""".stripMargin
    reusablePerRecordStatements.add(field)
    fieldTerm
  }

  /**
    * Adds a reusable [[java.util.HashSet]] to the member area of the generated [[Function]].
    *
    * @param elements elements to be added to the set (including null)
    * @return member variable term
    */
  def addReusableSet(elements: Seq[GeneratedExpression]): String = {
    val fieldTerm = newName("set")

    val field =
      s"""
        |final java.util.Set $fieldTerm;
        |""".stripMargin
    reusableMemberStatements.add(field)

    val init =
      s"""
        |$fieldTerm = new java.util.HashSet();
        |""".stripMargin
    reusableInitStatements.add(init)

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
    * Adds a reusable constant to the member area of the generated [[Function]].
    *
    * @param constant constant expression
    * @return member variable term
    */
  def addReusableBoxedConstant(constant: GeneratedExpression): String = {
    require(constant.literal, "Literal expected")

    val fieldTerm = newName("constant")

    val boxed = generateOutputFieldBoxing(constant)
    val boxedType = boxedTypeTermForTypeInfo(boxed.resultType)

    val field =
      s"""
        |final $boxedType $fieldTerm;
        |""".stripMargin
    reusableMemberStatements.add(field)

    val init =
      s"""
        |${boxed.code}
        |$fieldTerm = ${boxed.resultTerm};
        |""".stripMargin
    reusableInitStatements.add(init)

    fieldTerm
  }

  /**
    * Adds a known reusable MessageDigest to the member area of the generated [[Function]].
    *
    * @return member variable term
    */
  def addReusableMessageDigest(algorithm: String): String = {
    val fieldTerm = newName("messageDigest")

    val field =
      s"""
        |final java.security.MessageDigest $fieldTerm;
        |""".stripMargin
    reusableMemberStatements.add(field)

    val init =
      s"""
        |try {
        |  $fieldTerm = java.security.MessageDigest.getInstance("$algorithm");
        |} catch (java.security.NoSuchAlgorithmException e) {
        |  throw new RuntimeException("Algorithm for '$algorithm' is not available.", e);
        |}
        |""".stripMargin

    reusableInitStatements.add(init)
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
        s"""
           |final java.security.MessageDigest $fieldTerm;
           |""".stripMargin
      reusableMemberStatements.add(field)

    val bitLen = constant.resultTerm
    val init = s"""
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
