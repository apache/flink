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

package org.apache.flink.api.table.codegen

import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.api.common.functions.{FlatMapFunction, Function, MapFunction}
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.table.codegen.CodeGenUtils._
import org.apache.flink.api.table.codegen.Indenter.toISC
import org.apache.flink.api.table.codegen.OperatorCodeGen._
import org.apache.flink.api.table.plan.TypeConverter.sqlTypeToTypeInfo
import org.apache.flink.api.table.typeinfo.RowTypeInfo

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * A code generator for generating Flink [[org.apache.flink.api.common.functions.Function]]s.
  *
  * @param config configuration that determines runtime behavior
  * @param input1 type information about the first input of the Function
  * @param input2 type information about the second input if the Function is binary
  */
class CodeGenerator(
    config: TableConfig,
    input1: TypeInformation[Any],
    input2: Option[TypeInformation[Any]] = None)
  extends RexVisitor[GeneratedExpression] {

  // set of member statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableMemberStatements = mutable.LinkedHashSet[String]()

  // set of constructor statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableInitStatements = mutable.LinkedHashSet[String]()

  // map of initial input unboxing expressions that will be added only once
  // (inputTerm, index) -> expr
  private val reusableInputUnboxingExprs = mutable.Map[(String, Int), GeneratedExpression]()

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
    * @return code block of statements that unbox input variables to a primitive variable
    *         and a corresponding null flag variable
    */
  def reuseInputUnboxingCode(): String = {
    reusableInputUnboxingExprs.values.map(_.code).mkString("", "\n", "\n")
  }

  /**
    * @return term of the (casted and possibly boxed) first input
    */
  def input1Term = "in1"

  /**
    * @return term of the (casted and possibly boxed) second input
    */
  def input2Term = "in2"

  /**
    * @return term of the (casted) output collector
    */
  def collectorTerm = "c"

  /**
    * @return term of the output record (possibly defined in the member area e.g. Row, Tuple)
    */
  def outRecordTerm = "out"

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
    * Generates a [[org.apache.flink.api.common.functions.Function]] that can be passed to Java
    * compiler.
    *
    * @param name Class name of the Function. Must not be unique but has to be a valid Java class
    *             identifier.
    * @param clazz Flink Function to be generated.
    * @param bodyCode code contents of the SAM (Single Abstract Method). Inputs, collector, or
    *                 output record can be accessed via the given term methods.
    * @param returnType expected return type
    * @tparam T Flink Function to be generated.
    * @return instance of GeneratedFunction
    */
  def generateFunction[T <: Function](
      name: String,
      clazz: Class[T],
      bodyCode: String,
      returnType: TypeInformation[Any])
    : GeneratedFunction[T] = {
    val funcName = newName(name)

    // Janino does not support generics, that's why we need
    // manual casting here
    val samHeader =
      // FlatMapFunction
      if (clazz == classOf[FlatMapFunction[_,_]]) {
        val inputTypeTerm = boxedTypeTermForTypeInfo(input1)
        (s"void flatMap(Object _in1, org.apache.flink.util.Collector $collectorTerm)",
          s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;")
      }

      // MapFunction
      else if (clazz == classOf[MapFunction[_,_]]) {
        val inputTypeTerm = boxedTypeTermForTypeInfo(input1)
        ("Object map(Object _in1)",
          s"$inputTypeTerm $input1Term = ($inputTypeTerm) _in1;")
      }

      else {
        // TODO more functions
        throw new CodeGenException("Unsupported Function.")
      }

    val funcCode = j"""
      public class $funcName
          implements ${clazz.getCanonicalName} {

        ${reuseMemberCode()}

        public $funcName() {
          ${reuseInitCode()}
        }

        @Override
        public ${samHeader._1} {
          ${samHeader._2}
          ${reuseInputUnboxingCode()}
          $bodyCode
        }
      }
    """.stripMargin

    GeneratedFunction(funcName, returnType, funcCode)
  }

  /**
    * Generates an expression that converts the first input (and second input) into the given type.
    * If two inputs are converted, the second input is appended. If objects or variables can
    * be reused, they will be added to reusable code sections internally. The evaluation result
    * may be stored in the global result variable (see [[outRecordTerm]]).
    *
    * @param returnType conversion target type. Inputs and output must have the same arity.
    * @return instance of GeneratedExpression
    */
  def generateConverterResultExpression(
      returnType: TypeInformation[_ <: Any])
    : GeneratedExpression = {
    val input1AccessExprs = for (i <- 0 until input1.getArity)
      yield generateInputAccess(input1, input1Term, i)

    val input2AccessExprs = input2 match {
      case Some(ti) => for (i <- 0 until ti.getArity)
        yield generateInputAccess(ti, input2Term, i)
      case None => Seq() // add nothing
    }

    generateResultExpression(input1AccessExprs ++ input2AccessExprs, returnType)
  }

  /**
    * Generates an expression from a sequence of RexNode. If objects or variables can be reused,
    * they will be added to reusable code sections internally. The evaluation result
    * may be stored in the global result variable (see [[outRecordTerm]]).
    *
    * @param returnType conversion target type. Type must have the same arity than rexNodes.
    * @param rexNodes sequence of RexNode
    * @return instance of GeneratedExpression
    */
  def generateResultExpression(
      returnType: TypeInformation[_ <: Any],
      rexNodes: Seq[RexNode])
    : GeneratedExpression = {
    val fieldExprs = rexNodes.map(generateExpression)
    generateResultExpression(fieldExprs, returnType)
  }

  /**
    * Generates an expression from a sequence of other expressions. If objects or variables can
    * be reused, they will be added to reusable code sections internally. The evaluation result
    * may be stored in the global result variable (see [[outRecordTerm]]).
    *
    * @param fieldExprs
    * @param returnType conversion target type. Type must have the same arity than fieldExprs.
    * @return instance of GeneratedExpression
    */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      returnType: TypeInformation[_ <: Any])
    : GeneratedExpression = {
    // TODO disable arity check for Rows and derive row arity from fieldExprs
    // initial type check
    if (returnType.getArity != fieldExprs.length) {
      throw new CodeGenException("Arity of result type does not match number of expressions.")
    }
    // type check
    returnType match {
      case ct: CompositeType[_] =>
        fieldExprs.zipWithIndex foreach {
          case (fieldExpr, i) if fieldExpr.resultType != ct.getTypeAt(i) =>
            throw new CodeGenException("Incompatible types of expression and result type.")
          case _ => // ok
        }
      case at: AtomicType[_] if at != fieldExprs.head.resultType =>
        throw new CodeGenException("Incompatible types of expression and result type.")
      case _ => // ok
    }

    val returnTypeTerm = boxedTypeTermForTypeInfo(returnType)

    // generate result expression
    returnType match {
      case ri: RowTypeInfo =>
        addReusableOutRecord(ri)
        val resultSetters: String = fieldExprs.zipWithIndex map {
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
        } mkString "\n"

        GeneratedExpression(outRecordTerm, "false", resultSetters, returnType)

      case pj: PojoTypeInfo[_] =>
        addReusableOutRecord(pj)
        val resultSetters: String = fieldExprs.zip(pj.getFieldNames) map {
        case (fieldExpr, fieldName) =>
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
        } mkString "\n"

        GeneratedExpression(outRecordTerm, "false", resultSetters, returnType)

      case tup: TupleTypeInfo[_] =>
        addReusableOutRecord(tup)
        val resultSetters: String = fieldExprs.zipWithIndex map {
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
        } mkString "\n"

        GeneratedExpression(outRecordTerm, "false", resultSetters, returnType)

      case cc: CaseClassTypeInfo[_] =>
        val fieldCodes: String = fieldExprs.map(_.code).mkString("\n")
        val constructorParams: String = fieldExprs.map(_.resultTerm).mkString(", ")
        val resultTerm = newName(outRecordTerm)

        val nullCheckCode = if (nullCheck) {
        fieldExprs map { (fieldExpr) =>
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

        GeneratedExpression(resultTerm, "false", resultCode, returnType)

      case a: AtomicType[_] =>
        val fieldExpr = fieldExprs.head
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

    val index = if (input._1 == input1) {
      inputRef.getIndex
    } else {
      inputRef.getIndex - input1.getArity
    }

    generateInputAccess(input._1, input._2, index)
  }

  override def visitFieldAccess(rexFieldAccess: RexFieldAccess): GeneratedExpression = ???

  override def visitLiteral(literal: RexLiteral): GeneratedExpression = {
    val resultType = sqlTypeToTypeInfo(literal.getType.getSqlTypeName)
    val value = literal.getValue3
    literal.getType.getSqlTypeName match {
      case BOOLEAN =>
        generateNonNullLiteral(resultType, literal.getValue3.toString)
      case TINYINT =>
        val decimal = BigDecimal(value.asInstanceOf[java.math.BigDecimal])
        if (decimal.isValidByte) {
          generateNonNullLiteral(resultType, decimal.byteValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to byte.")
        }
      case SMALLINT =>
        val decimal = BigDecimal(value.asInstanceOf[java.math.BigDecimal])
        if (decimal.isValidShort) {
          generateNonNullLiteral(resultType, decimal.shortValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to short.")
        }
      case INTEGER =>
        val decimal = BigDecimal(value.asInstanceOf[java.math.BigDecimal])
        if (decimal.isValidShort) {
          generateNonNullLiteral(resultType, decimal.intValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to integer.")
        }
      case BIGINT =>
        val decimal = BigDecimal(value.asInstanceOf[java.math.BigDecimal])
        if (decimal.isValidLong) {
          generateNonNullLiteral(resultType, decimal.longValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to long.")
        }
      case FLOAT =>
        val decimal = BigDecimal(value.asInstanceOf[java.math.BigDecimal])
        if (decimal.isValidFloat) {
          generateNonNullLiteral(resultType, decimal.floatValue().toString + "f")
        }
        else {
          throw new CodeGenException("Decimal can not be converted to float.")
        }
      case DOUBLE =>
        val decimal = BigDecimal(value.asInstanceOf[java.math.BigDecimal])
        if (decimal.isValidDouble) {
          generateNonNullLiteral(resultType, decimal.doubleValue().toString)
        }
        else {
          throw new CodeGenException("Decimal can not be converted to double.")
        }
      case VARCHAR | CHAR =>
        generateNonNullLiteral(resultType, "\"" + value.toString + "\"")
      case NULL =>
        generateNullLiteral(resultType)
      case _ => ??? // TODO more types
    }
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): GeneratedExpression = ???

  override def visitLocalRef(localRef: RexLocalRef): GeneratedExpression = ???

  override def visitRangeRef(rangeRef: RexRangeRef): GeneratedExpression = ???

  override def visitDynamicParam(dynamicParam: RexDynamicParam): GeneratedExpression = ???

  override def visitCall(call: RexCall): GeneratedExpression = {
    val operands = call.getOperands.map(_.accept(this))
    val resultType = sqlTypeToTypeInfo(call.getType.getSqlTypeName)

    call.getOperator match {
      // arithmetic
      case PLUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("+", nullCheck, resultType, left, right)

      case PLUS if isString(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireString(left)
        generateArithmeticOperator("+", nullCheck, resultType, left, right)

      case MINUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("-", nullCheck, resultType, left, right)

      case MULTIPLY if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("*", nullCheck, resultType, left, right)

      case DIVIDE if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("/", nullCheck, resultType, left, right)

      case MOD if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateArithmeticOperator("%", nullCheck, resultType, left, right)

      case UNARY_MINUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator("-", nullCheck, resultType, operand)

      case UNARY_PLUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator("+", nullCheck, resultType, operand)

      // comparison
      case EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateEquals(nullCheck, left, right)

      case NOT_EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateNotEquals(nullCheck, left, right)

      case GREATER_THAN =>
        val left = operands.head
        val right = operands(1)
        generateComparison(">", nullCheck, left, right)

      case GREATER_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        generateComparison(">=", nullCheck, left, right)

      case LESS_THAN =>
        val left = operands.head
        val right = operands(1)
        generateComparison("<", nullCheck, left, right)

      case LESS_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
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

      case call@_ =>
        throw new CodeGenException(s"Unsupported call: $call")
    }
  }

  override def visitOver(over: RexOver): GeneratedExpression = ???

  // ----------------------------------------------------------------------------------------------
  // generator helping methods
  // ----------------------------------------------------------------------------------------------

  private def generateInputAccess(
      inputType: TypeInformation[Any],
      inputTerm: String,
      index: Int)
    : GeneratedExpression = {
    // if input has been used before, we can reuse the code that
    // has already been generated
    val inputExpr = reusableInputUnboxingExprs.get((inputTerm, index)) match {
      // input access and boxing has already been generated
      case Some(expr) =>
        expr

      // generate input access and boxing
      case None =>
        val newExpr = inputType match {
          case ct: CompositeType[_] =>
            val accessor = fieldAccessorFor(ct, index)
            val fieldType: TypeInformation[Any] = ct.getTypeAt(index)
            val fieldTypeTerm = boxedTypeTermForTypeInfo(fieldType)

            val inputCode = accessor match {
              case ObjectFieldAccessor(fieldName) =>
                s"($fieldTypeTerm) $inputTerm.$fieldName"

              case ObjectMethodAccessor(methodName) =>
                s"($fieldTypeTerm) $inputTerm.$methodName()"

              case ProductAccessor(i) =>
                s"($fieldTypeTerm) $inputTerm.productElement($i)"
            }
            generateInputUnboxing(fieldType, inputCode)
          case at: AtomicType[_] =>
            val fieldTypeTerm = boxedTypeTermForTypeInfo(at)
            val inputCode = s"($fieldTypeTerm) $inputTerm"
            generateInputUnboxing(at, inputCode)
          case _ =>
            throw new CodeGenException("Unsupported type for input access.")
        }
        reusableInputUnboxingExprs((inputTerm, index)) = newExpr
        newExpr
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, "", inputExpr.resultType)
  }

  private def generateInputUnboxing(
      inputType: TypeInformation[Any],
      inputCode: String)
    : GeneratedExpression = {
    val tmpTerm = newName("tmp")
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val tmpTypeTerm = boxedTypeTermForTypeInfo(inputType)
    val resultTypeTerm = primitiveTypeTermForTypeInfo(inputType)
    val defaultValue = primitiveDefaultValue(inputType)

    val wrappedCode = if (nullCheck && !isReference(inputType)) {
      s"""
        |$tmpTypeTerm $tmpTerm = $inputCode;
        |boolean $nullTerm = $tmpTerm == null;
        |$resultTypeTerm $resultTerm;
        |if ($nullTerm) {
        |  $resultTerm = $defaultValue;
        |}
        |else {
        |  $resultTerm = $tmpTerm;
        |}
        |""".stripMargin
    } else if (nullCheck) {
      s"""
        |$resultTypeTerm $resultTerm = $inputCode;
        |boolean $nullTerm = $inputCode == null;
        |""".stripMargin
    } else {
      s"""
        |$resultTypeTerm $resultTerm = $inputCode;
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, wrappedCode, inputType)
  }

  private def generateNonNullLiteral(
      literalType: TypeInformation[_],
      literalCode: String)
    : GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(literalType)

    val resultCode = if (nullCheck) {
      s"""
        |$resultTypeTerm $resultTerm = $literalCode;
        |boolean $nullTerm = false;
        |""".stripMargin
    } else {
      s"""
        |$resultTypeTerm $resultTerm = $literalCode;
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, literalType)
  }

  private def generateNullLiteral(resultType: TypeInformation[_]): GeneratedExpression = {
    val resultTerm = newName("result")
    val nullTerm = newName("isNull")
    val resultTypeTerm = primitiveTypeTermForTypeInfo(resultType)
    val defaultValue = primitiveDefaultValue(resultType)

    val wrappedCode = if (nullCheck) {
      s"""
        |$resultTypeTerm $resultTerm = null;
        |boolean $nullTerm = true;
        |""".stripMargin
    } else {
      s"""
        |$resultTypeTerm $resultTerm = $defaultValue;
        |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, wrappedCode, resultType)
  }

  // ----------------------------------------------------------------------------------------------

  def addReusableOutRecord(ti: TypeInformation[_]) = {
    val statement = ti match {
      case rt: RowTypeInfo =>
        s"""
          |${ti.getTypeClass.getCanonicalName} $outRecordTerm =
          |    new ${ti.getTypeClass.getCanonicalName}(${rt.getArity});
          |""".stripMargin
      case _ =>
        s"""
          |${ti.getTypeClass.getCanonicalName} $outRecordTerm =
          |    new ${ti.getTypeClass.getCanonicalName}();
          |""".stripMargin
    }
    reusableMemberStatements.add(statement)
  }

}
