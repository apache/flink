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

import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlTypeName}
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.functions.sql.ProctimeSqlFunction
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import scala.collection.JavaConversions._

/**
 * This code generator is mainly responsible for generating codes for a given calcite [[RexNode]].
 * It can also generate type conversion codes for the result converter.
 */
class ExprCodeGenerator(ctx: CodeGeneratorContext, nullableInput: Boolean, nullCheck: Boolean)
  extends RexVisitor[GeneratedExpression] {

  // check if nullCheck is enabled when inputs can be null
  if (nullableInput && !nullCheck) {
    throw new CodeGenException("Null check must be enabled if entire rows can be null.")
  }

  /**
   * term of the [[ProcessFunction]]'s context, can be changed when needed
   */
  var contextTerm = "ctx"

  /**
   * information of the first input
   */
  var input1Type: TypeInformation[_ <: Any] = _
  var input1Term: String = _
  var input1FieldMapping: Option[Array[Int]] = None

  /**
   * information of the optional second input
   */
  var input2Type: Option[TypeInformation[_ <: Any]] = None
  var input2Term: Option[String] = None
  var input2FieldMapping: Option[Array[Int]] = None

  /**
   * Bind the input information, should be called before generating expression.
   */
  def bindInput(
      inputType: TypeInformation[_ <: Any],
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT1_TERM,
      inputFieldMapping: Option[Array[Int]] = None): ExprCodeGenerator = {
    checkPojoInputType(inputType, inputFieldMapping)
    input1Type = inputType
    input1Term = inputTerm
    input1FieldMapping = inputFieldMapping
    this
  }

  /**
   * In some cases, the expression will have two inputs (e.g. join condition and udtf). We should
   * bind second input information before use.
   */
  def bindSecondInput(
      inputType: TypeInformation[_ <: Any],
      inputTerm: String = CodeGeneratorContext.DEFAULT_INPUT2_TERM,
      inputFieldMapping: Option[Array[Int]] = None): ExprCodeGenerator = {
    checkPojoInputType(inputType, inputFieldMapping)
    input2Type = Some(inputType)
    input2Term = Some(inputTerm)
    input2FieldMapping = inputFieldMapping
    this
  }

  protected lazy val input1Mapping: Array[Int] = input1FieldMapping match {
    case Some(mapping) => mapping
    case _ => (0 until input1Type.getArity).toArray
  }

  protected lazy val input2Mapping: Array[Int] = input2FieldMapping match {
    case Some(mapping) => mapping
    case _ => input2Type match {
      case Some(input) => (0 until input.getArity).toArray
      case _ => Array[Int]()
    }
  }

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
   * Generates an expression from the left input and the right table function.
   */
  def generateCorrelateAccessExprs: (Seq[GeneratedExpression], Seq[GeneratedExpression]) = {
    val input1AccessExprs = input1Mapping.map { idx =>
      generateInputAccess(ctx, input1Type, input1Term, idx, nullableInput, nullCheck)
    }

    val input2AccessExprs = (input2Type, input2Term) match {
      case (Some(ti), Some(term)) =>
        // use generateFieldAccess instead of generateInputAccess to avoid the generated table
        // function's field access code is put on the top of function body rather than
        // the while loop
        input2Mapping.map { idx =>
          generateFieldAccess(ctx, ti, term, idx, nullCheck)
        }.toSeq
      case _ => throw new CodeGenException("Type information and term of input2 must not be null.")
    }

    (input1AccessExprs, input2AccessExprs)
  }

  /**
   * Generates an expression that converts the first input (and second input) into the given type.
   * If two inputs are converted, the second input is appended. If objects or variables can
   * be reused, they will be added to reusable code sections internally. The evaluation result
   * will be stored in the variable outRecordTerm.
   *
   * @param returnType conversion target type. Inputs and output must have the same arity.
   * @param resultFieldNames result field names necessary for a mapping to POJO fields.
   * @param rowtimeExpression an expression to extract the value of a rowtime field from
   *                          the input data. Required if the field indicies include a rowtime
   *                          marker.
   * @param outRecordTerm the result term
   * @return instance of GeneratedExpression
   */
  def generateConverterResultExpression(
      returnType: TypeInformation[_ <: Any],
      resultFieldNames: Seq[String],
      rowtimeExpression: Option[RexNode] = None,
      outRecordTerm: String = CodeGeneratorContext.DEFAULT_OUT_RECORD_TERM)
    : GeneratedExpression = {
    val input1AccessExprs = input1Mapping.map {
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER |
           TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER if rowtimeExpression.isDefined =>
        // generate rowtime attribute from expression
        generateExpression(rowtimeExpression.get)
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER |
           TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER =>
        throw TableException("Rowtime extraction expression missing. Please report a bug.")
      case TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER =>
        // attribute is proctime indicator.
        // we use a null literal and generate a timestamp when we need it.
        generateNullLiteral(TimeIndicatorTypeInfo.PROCTIME_INDICATOR, nullCheck)
      case TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER =>
        // attribute is proctime field in a batch query.
        // it is initialized with the current time.
        generateCurrentTimestamp(ctx, nullCheck)
      case idx =>
        generateInputAccess(ctx, input1Type, input1Term, idx, nullableInput, nullCheck)
    }

    val input2AccessExprs = input2Type match {
      case Some(ti) =>
        input2Mapping.map(idx =>
          generateInputAccess(ctx, ti, input2Term.get, idx, nullableInput, nullCheck)).toSeq
      case None => Seq() // add nothing
    }

    generateResultExpression(input1AccessExprs ++ input2AccessExprs, returnType, resultFieldNames)
  }

  /**
   * Generates an expression from a sequence of other expressions. If objects or variables can
   * be reused, they will be added to reusable code sections internally. The evaluation result
   * may be stored in the variable outRecordTerm.
   *
   * @param fieldExprs field expressions to be converted
   * @param returnType conversion target type. Type must have the same arity than fieldExprs.
   * @param resultFieldNames result field names necessary for a mapping to POJO fields.
   * @param outRecordTerm the result term
   * @return instance of GeneratedExpression
   */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      returnType: TypeInformation[_ <: Any],
      resultFieldNames: Seq[String],
      outRecordTerm: String = CodeGeneratorContext.DEFAULT_OUT_RECORD_TERM)
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
                  s" [${fieldExpr.resultType}], result type is " +
                  s"[${pt.getTypeAt(resultFieldNames(i))}]")

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

      case at: AtomicType[_] if at != fieldExprs.head.resultType =>
        throw new CodeGenException(
          s"Incompatible types of expression and result type. Expression [${fieldExprs.head}] " +
              s"type is [${fieldExprs.head.resultType}], result type is [$at]")

      case _ => // ok
    }

    val returnTypeTerm = boxedTypeTermForTypeInfo(returnType)
    val boxedFieldExprs = fieldExprs.map(generateOutputFieldBoxing(_, nullCheck))

    // generate result expression
    returnType match {
      case ri: RowTypeInfo =>
        ctx.addReusableOutRecord(ri, outRecordTerm)
        val resultSetters: String = boxedFieldExprs.zipWithIndex map {
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
        } mkString ""

        GeneratedExpression(outRecordTerm, "false", resultSetters, returnType)

      case pt: PojoTypeInfo[_] =>
        ctx.addReusableOutRecord(pt, outRecordTerm)
        val resultSetters: String = boxedFieldExprs.zip(resultFieldNames) map {
          case (fieldExpr, fieldName) =>
            val accessor = getFieldAccessor(pt.getTypeClass, fieldName)

            accessor match {
              // Reflective access of primitives/Objects
              case ObjectPrivateFieldAccessor(field) =>
                val fieldTerm = ctx.addReusablePrivateFieldAccess(pt.getTypeClass, fieldName)

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
        } mkString ""

        GeneratedExpression(outRecordTerm, "false", resultSetters, returnType)

      case tup: TupleTypeInfo[_] =>
        ctx.addReusableOutRecord(tup, outRecordTerm)
        val resultSetters: String = boxedFieldExprs.zipWithIndex map {
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
        } mkString ""

        GeneratedExpression(outRecordTerm, "false", resultSetters, returnType)

      case cc: CaseClassTypeInfo[_] =>
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
          } mkString ""
        } else {
          ""
        }

        val resultCode =
          s"""
             |$fieldCodes
             |$nullCheckCode
             |$returnTypeTerm $resultTerm = new $returnTypeTerm($constructorParams);
             |""".stripMargin

        GeneratedExpression(resultTerm, "false", resultCode, returnType)

      case a: AtomicType[_] =>
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

        GeneratedExpression(fieldExpr.resultTerm, "false", resultCode, returnType)

      case _ =>
        throw new CodeGenException(s"Unsupported result type: $returnType")
    }
  }

  override def visitInputRef(inputRef: RexInputRef): GeneratedExpression = {
    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    val input = if (inputRef.getIndex < input1Type.getArity) {
      (input1Type, input1Term)
    } else {
      (input2Type.getOrElse(throw new CodeGenException("Invalid input access.")),
          input2Term.getOrElse(throw new CodeGenException("Invalid input access.")))
    }

    val index = if (input._2 == input1Term) {
      inputRef.getIndex
    } else {
      inputRef.getIndex - input1Type.getArity
    }

    generateInputAccess(ctx, input._1, input._2, index, nullableInput, nullCheck)
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): GeneratedExpression =
    visitInputRef(rexTableInputRef)

  override def visitFieldAccess(rexFieldAccess: RexFieldAccess): GeneratedExpression = {
    val refExpr = rexFieldAccess.getReferenceExpr.accept(this)
    val index = rexFieldAccess.getField.getIndex
    val fieldAccessExpr = generateFieldAccess(
      ctx,
      refExpr.resultType,
      refExpr.resultTerm,
      index,
      nullCheck)

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

  override def visitLiteral(literal: RexLiteral): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toTypeInfo(literal.getType)
    val value = literal.getValue3
    // null value with type
    if (value == null) {
      return generateNullLiteral(resultType, nullCheck)
    }
    // non-null values
    literal.getType.getSqlTypeName match {

      case BOOLEAN =>
        generateNonNullLiteral(resultType, literal.getValue3.toString, nullCheck)

      case TINYINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidByte) {
          generateNonNullLiteral(resultType, decimal.byteValue().toString, nullCheck)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to byte.")
        }

      case SMALLINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidShort) {
          generateNonNullLiteral(resultType, decimal.shortValue().toString, nullCheck)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to short.")
        }

      case INTEGER =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          generateNonNullLiteral(resultType, decimal.intValue().toString, nullCheck)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to integer.")
        }

      case BIGINT =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(resultType, decimal.longValue().toString + "L", nullCheck)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to long.")
        }

      case FLOAT =>
        val floatValue = value.asInstanceOf[JBigDecimal].floatValue()
        floatValue match {
          case Float.NaN => generateNonNullLiteral(resultType, "java.lang.Float.NaN", nullCheck)
          case Float.NegativeInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Float.NEGATIVE_INFINITY", nullCheck)
          case Float.PositiveInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Float.POSITIVE_INFINITY", nullCheck)
          case _ => generateNonNullLiteral(resultType, floatValue.toString + "f", nullCheck)
        }

      case DOUBLE =>
        val doubleValue = value.asInstanceOf[JBigDecimal].doubleValue()
        doubleValue match {
          case Double.NaN => generateNonNullLiteral(resultType, "java.lang.Double.NaN", nullCheck)
          case Double.NegativeInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Double.NEGATIVE_INFINITY", nullCheck)
          case Double.PositiveInfinity =>
            generateNonNullLiteral(resultType, "java.lang.Double.POSITIVE_INFINITY", nullCheck)
          case _ => generateNonNullLiteral(resultType, doubleValue.toString + "d", nullCheck)
        }

      case DECIMAL =>
        val decimalField = ctx.addReusableDecimal(value.asInstanceOf[JBigDecimal])
        generateNonNullLiteral(resultType, decimalField, nullCheck)

      case VARCHAR | CHAR =>
        val escapedValue = StringEscapeUtils.ESCAPE_JAVA.translate(value.toString)
        generateNonNullLiteral(resultType, "\"" + escapedValue + "\"", nullCheck)

      case SYMBOL =>
        generateSymbol(value.asInstanceOf[Enum[_]])

      case DATE =>
        generateNonNullLiteral(resultType, value.toString, nullCheck)

      case TIME =>
        generateNonNullLiteral(resultType, value.toString, nullCheck)

      case TIMESTAMP =>
        generateNonNullLiteral(resultType, value.toString + "L", nullCheck)

      case typeName if YEAR_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidInt) {
          generateNonNullLiteral(resultType, decimal.intValue().toString, nullCheck)
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of months.")
        }

      case typeName if DAY_INTERVAL_TYPES.contains(typeName) =>
        val decimal = BigDecimal(value.asInstanceOf[JBigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(resultType, decimal.longValue().toString + "L", nullCheck)
        } else {
          throw new CodeGenException(
            s"Decimal '$decimal' can not be converted to interval of milliseconds.")
        }

      case t@_ =>
        throw new CodeGenException(s"Type not supported: $t")
    }
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): GeneratedExpression = {
    GeneratedExpression(input1Term, NEVER_NULL, NO_CODE, input1Type)
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
      return generateProctimeTimestamp(contextTerm)
    }

    val resultType = FlinkTypeFactory.toTypeInfo(call.getType)

    // convert operands and help giving untyped NULL literals a type
    val operands = call.getOperands.zipWithIndex.map {

      // this helps e.g. for AS(null)
      // we might need to extend this logic in case some rules do not create typed NULLs
      case (operandLiteral: RexLiteral, 0) if
          operandLiteral.getType.getSqlTypeName == SqlTypeName.NULL &&
          call.getOperator.getReturnTypeInference == ReturnTypes.ARG0 =>
        generateNullLiteral(resultType, nullCheck)

      case (o@_, _) =>
        o.accept(this)
    }

    generateCallExpression(ctx, call.getOperator, operands, resultType, nullCheck, contextTerm)
  }

  override def visitOver(over: RexOver): GeneratedExpression =
    throw new CodeGenException("Aggregate functions over windows are not supported yet.")

  override def visitSubQuery(subQuery: RexSubQuery): GeneratedExpression =
    throw new CodeGenException("Subqueries are not supported yet.")

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression =
    throw new CodeGenException("Pattern field references are not supported yet.")


  private def checkPojoInputType(
      inputType: TypeInformation[_ <: Any],
      inputFieldMapping: Option[Array[Int]]): Unit = {
    inputType match {
      case pt: PojoTypeInfo[_] =>
        inputFieldMapping.getOrElse(
          throw new CodeGenException("No input mapping is specified for input of type POJO."))
      case _ => // ok
    }
  }

}
