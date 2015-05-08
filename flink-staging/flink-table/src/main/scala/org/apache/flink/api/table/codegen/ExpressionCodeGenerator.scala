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

import java.util.concurrent.atomic.AtomicInteger

import org.codehaus.janino.SimpleCompiler
import org.slf4j.LoggerFactory

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.typeinfo.{RenamingProxyTypeInfo, RowTypeInfo}
import org.apache.flink.api.table.{ExpressionException, expressions}

/** Base class for all code generation classes. This provides the functionality for generating
  * code from an [[Expression]] tree. Derived classes must embed this in a lambda function
  * to form an executable code block.
  *
  * @param inputs List of input variable names with corresponding [[TypeInformation]].
  * @param nullCheck Whether the generated code should include checks for NULL values.
  * @param cl The ClassLoader that is used to create the Scala reflection ToolBox
  * @tparam R The type of the generated code block. In most cases a lambda function such
  *           as "(IN1, IN2) => OUT".
  */
abstract class ExpressionCodeGenerator[R](
    inputs: Seq[(String, CompositeType[_])],
    val nullCheck: Boolean = false,
    cl: ClassLoader) {
  protected val log = LoggerFactory.getLogger(classOf[ExpressionCodeGenerator[_]])

  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{universe => ru}

  if (cl == null) {
    throw new IllegalArgumentException("ClassLoader must not be null.")
  }

  val compiler = new SimpleCompiler()
  compiler.setParentClassLoader(cl)


  // This is to be implemented by subclasses, we have it like this
  // so that we only call it from here with the Scala Reflection Lock.
  protected def generateInternal(): R

  final def generate(): R = {
    generateInternal()
  }

  protected def generateExpression(expr: Expression): GeneratedExpression = {
    generateExpressionInternal(expr)
  }

  protected def generateExpressionInternal(expr: Expression): GeneratedExpression = {
    //  protected def generateExpression(expr: Expression): GeneratedExpression = {
    val nullTerm = freshName("isNull")
    val resultTerm = freshName("result")

    // For binary predicates that must only be evaluated when both operands are non-null.
    // This will write to nullTerm and resultTerm, so don't use those term names
    // after using this function
    def generateIfNonNull(left: Expression, right: Expression, resultType: TypeInformation[_])
                         (expr: (String, String) => String): String = {
      val leftCode = generateExpression(left)
      val rightCode = generateExpression(right)

      val leftTpe = typeTermForTypeInfo(left.typeInfo)
      val rightTpe = typeTermForTypeInfo(right.typeInfo)
      val resultTpe = typeTermForTypeInfo(resultType)

      if (nullCheck) {
        leftCode.code + "\n" + 
          rightCode.code + "\n" +
          s"""
            |boolean $nullTerm = ${leftCode.nullTerm} || ${rightCode.nullTerm};
            |$resultTpe $resultTerm;
            |if ($nullTerm) {
            |  $resultTerm = ${defaultPrimitive(resultType)}
            |} else {
            |  $resultTerm = ${expr(leftCode.resultTerm, rightCode.resultTerm)}
            |}
          """.stripMargin
      } else {
        leftCode.code + "\n" +
          rightCode.code + "\n" +
          s"""
            |$resultTpe $resultTerm = ${expr(leftCode.resultTerm, rightCode.resultTerm)};
          """.stripMargin
      }
    }

    val cleanedExpr = expr match {
      case expressions.Naming(namedExpr, _) => namedExpr
      case _ => expr
    }
    
    val resultTpe = typeTermForTypeInfo(cleanedExpr.typeInfo)

    val code: String = cleanedExpr match {

      case expressions.Literal(null, typeInfo) =>
        if (nullCheck) {
          s"""
            |boolean $nullTerm = true;
            |$resultTpe resultTerm = null;
          """.stripMargin
        } else {
          s"""
            |$resultTpe resultTerm = null;
          """.stripMargin
        }

      case expressions.Literal(intValue: Int, INT_TYPE_INFO) =>
        if (nullCheck) {
          s"""
            |boolean $nullTerm = false;
            |$resultTpe $resultTerm = $intValue;
          """.stripMargin
        } else {
          s"""
            |$resultTpe $resultTerm = $intValue;
          """.stripMargin
        }

      case expressions.Literal(longValue: Long, LONG_TYPE_INFO) =>
        if (nullCheck) {
          s"""
            |boolean $nullTerm = false;
            |$resultTpe $resultTerm = ${longValue}L;
          """.stripMargin
        } else {
          s"""
            |$resultTpe $resultTerm = ${longValue}L;
          """.stripMargin
        }


      case expressions.Literal(doubleValue: Double, DOUBLE_TYPE_INFO) =>
        if (nullCheck) {
          s"""
            |val $nullTerm = false
            |$resultTpe $resultTerm = $doubleValue;
          """.stripMargin
        } else {
          s"""
            |$resultTpe $resultTerm = $doubleValue;
          """.stripMargin
        }

      case expressions.Literal(floatValue: Float, FLOAT_TYPE_INFO) =>
        if (nullCheck) {
          s"""
            |val $nullTerm = false
            |$resultTpe $resultTerm = ${floatValue}f;
          """.stripMargin
        } else {
          s"""
            |$resultTpe $resultTerm = ${floatValue}f;
          """.stripMargin
        }

      case expressions.Literal(strValue: String, STRING_TYPE_INFO) =>
        if (nullCheck) {
          s"""
            |val $nullTerm = false
            |$resultTpe $resultTerm = "$strValue";
          """.stripMargin
        } else {
          s"""
            |$resultTpe $resultTerm = "$strValue";
          """.stripMargin
        }

      case expressions.Literal(boolValue: Boolean, BOOLEAN_TYPE_INFO) =>
        if (nullCheck) {
          s"""
            |val $nullTerm = false
            |$resultTpe $resultTerm = $boolValue;
          """.stripMargin
        } else {
          s"""
            $resultTpe $resultTerm = $boolValue;
          """.stripMargin
        }

      case Substring(str, beginIndex, endIndex) =>
        val strCode = generateExpression(str)
        val beginIndexCode = generateExpression(beginIndex)
        val endIndexCode = generateExpression(endIndex)
        if (nullCheck) {
          strCode.code +
            beginIndexCode.code +
            endIndexCode.code +
            s"""
              boolean $nullTerm =
                ${strCode.nullTerm} || ${beginIndexCode.nullTerm} || ${endIndexCode.nullTerm};
              $resultTpe $resultTerm;
              if ($nullTerm) {
                $resultTerm = ${defaultPrimitive(str.typeInfo)};
              } else {
                if (${endIndexCode.resultTerm} == Int.MaxValue) {
                   $resultTerm = (${strCode.resultTerm}).substring(${beginIndexCode.resultTerm});
                } else {
                  $resultTerm = (${strCode.resultTerm}).substring(
                    ${beginIndexCode.resultTerm},
                    ${endIndexCode.resultTerm});
                }
              }
            """.stripMargin
        } else {
          strCode.code +
            beginIndexCode.code +
            endIndexCode.code +
            s"""
              $resultTpe $resultTerm;

              if (${endIndexCode.resultTerm} == Integer.MAX_VALUE) {
                $resultTerm = (${strCode.resultTerm}).substring(${beginIndexCode.resultTerm});
              } else {
                $resultTerm = (${strCode.resultTerm}).substring(
                  ${beginIndexCode.resultTerm},
                  ${endIndexCode.resultTerm});
              }
            """
        }

      case expressions.Cast(child: Expression, STRING_TYPE_INFO) =>
        val childGen = generateExpression(child)
        val castCode = if (nullCheck) {
          s"""
            |boolean $nullTerm = ${childGen.nullTerm};
            |$resultTpe $resultTerm;
            |if ($nullTerm == null) {
            |  $resultTerm = null;
            |} else {
            |  $resultTerm = "" + ${childGen.resultTerm};
            |}
          """.stripMargin
        } else {
          s"""
            |$resultTpe $resultTerm = "" + ${childGen.resultTerm};
          """.stripMargin
        }
        childGen.code + castCode

      case expressions.Cast(child: Expression, tpe: BasicTypeInfo[_]) =>
        val childGen = generateExpression(child)
        val castCode = if (nullCheck) {
          s"""
            |boolean $nullTerm = ${childGen.nullTerm};
            |$resultTpe $resultTerm =
            |  ${tpe.getTypeClass.getCanonicalName}.valueOf(${childGen.resultTerm});
          """.stripMargin
        } else {
          s"""
            |$resultTpe $resultTerm =
            |  ${tpe.getTypeClass.getCanonicalName}.valueOf(${childGen.resultTerm});
          """.stripMargin
        }
        childGen.code + castCode

      case ResolvedFieldReference(fieldName, fieldTpe: TypeInformation[_]) =>
        inputs find { i => i._2.hasField(fieldName)} match {
          case Some((inputName, inputTpe)) =>
            val fieldCode = getField(newTermName(inputName), inputTpe, fieldName, fieldTpe)
            if (nullCheck) {
              s"""
                |$resultTpe $resultTerm = $fieldCode;
                |boolean $nullTerm = $resultTerm == null;
              """.stripMargin
            } else {
              s"""$resultTpe $resultTerm = $fieldCode;"""
            }

          case None => throw new ExpressionException("Could not get accessor for " + fieldName
            + " in inputs " + inputs.mkString(", ") + ".")
        }

      case GreaterThan(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => s"$leftTerm > $rightTerm"
        }

      case GreaterThanOrEqual(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => s"$leftTerm >= $rightTerm"
        }

      case LessThan(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => s"$leftTerm < $rightTerm"
        }

      case LessThanOrEqual(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => s"$leftTerm <= $rightTerm"
        }

      case EqualTo(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
        }

      case NotEqualTo(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => s"$leftTerm != $rightTerm"
        }

      case And(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => s"$leftTerm && $rightTerm"
        }

      case Or(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => s"$leftTerm || $rightTerm"
        }

      case Plus(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => s"$leftTerm + $rightTerm"
        }

      case Minus(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => s"$leftTerm - $rightTerm"
        }

      case Div(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => s"$leftTerm / $rightTerm"
        }

      case Mul(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => s"$leftTerm * $rightTerm"
        }

      case Mod(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => s"$leftTerm % $rightTerm"
        }

      case UnaryMinus(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code +
            s"""
              |boolean $nullTerm = ${childCode.nullTerm};
              |if ($nullTerm) {
              |  ${defaultPrimitive(child.typeInfo)};
              |} else {
              |  $resultTpe $resultTerm = -(${childCode.resultTerm});
              |}
            """.stripMargin
        } else {
          childCode.code +
            s"""
              |$resultTpe $resultTerm = -(${childCode.resultTerm});
            """.stripMargin
        }

      case BitwiseAnd(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => s"(int) $leftTerm & (int) $rightTerm"
        }

      case BitwiseOr(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => s"(int) $leftTerm | (int) $rightTerm"
        }

      case BitwiseXor(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => s"(int) $leftTerm ^ (int) $rightTerm"
        }

      case BitwiseNot(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code +
            s"""
              |boolean $nullTerm = ${childCode.nullTerm};
              |if ($nullTerm) {
              |  ${defaultPrimitive(child.typeInfo)};
              |} else {
              |  $resultTpe $resultTerm = ~((int) ${childCode.resultTerm});
              |}
            """.stripMargin
        } else {
          childCode.code +
            s"""
              |$resultTpe $resultTerm = ~((int) ${childCode.resultTerm});
            """.stripMargin
        }

      case Not(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code +
            s"""
              |boolean $nullTerm = ${childCode.nullTerm};
              |if ($nullTerm) {
              |  ${defaultPrimitive(child.typeInfo)};
              |} else {
              |  $resultTpe $resultTerm = !(${childCode.resultTerm});
              |}
            """.stripMargin
        } else {
          childCode.code +
            s"""
              |$resultTpe $resultTerm = !(${childCode.resultTerm});
            """.stripMargin
        }

      case IsNull(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code +
            s"""
              |boolean $nullTerm = ${childCode.nullTerm};
              |if ($nullTerm) {
              |  ${defaultPrimitive(child.typeInfo)};
              |} else {
              |  $resultTpe $resultTerm = (${childCode.resultTerm}) == null;
              |}
            """.stripMargin
        } else {
          childCode.code +
            s"""
              |$resultTpe $resultTerm = (${childCode.resultTerm}) == null;
            """.stripMargin
        }

      case IsNotNull(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code +
            s"""
              |boolean $nullTerm = ${childCode.nullTerm};
              |if ($nullTerm) {
              |  ${defaultPrimitive(child.typeInfo)};
              |} else {
              |  $resultTpe $resultTerm = (${childCode.resultTerm}) != null;
              |}
            """.stripMargin
        } else {
          childCode.code +
            s"""
              |$resultTpe $resultTerm = (${childCode.resultTerm}) != null;
            """.stripMargin
        }

      case Abs(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code +
            s"""
              |boolean $nullTerm = ${childCode.nullTerm};
              |if ($nullTerm) {
              |  ${defaultPrimitive(child.typeInfo)};
              |} else {
              |  $resultTpe $resultTerm = Math.abs(${childCode.resultTerm});
              |}
            """.stripMargin
        } else {
          childCode.code +
            s"""
              |$resultTpe $resultTerm = Math.abs(${childCode.resultTerm});
            """.stripMargin
        }

      case _ => throw new ExpressionException("Could not generate code for expression " + expr)
    }

    GeneratedExpression(code, resultTerm, nullTerm)
  }

  case class GeneratedExpression(code: String, resultTerm: String, nullTerm: String)

  def freshName(name: String): String = {
    s"$name$$${freshNameCounter.getAndIncrement}"
  }

  val freshNameCounter = new AtomicInteger

  protected def getField(
    inputTerm: TermName,
    inputType: CompositeType[_],
    fieldName: String,
    fieldType: TypeInformation[_]): String = {
    val accessor = fieldAccessorFor(inputType, fieldName)
    val fieldTpe = typeTermForTypeInfo(fieldType)

    accessor match {
      case ObjectFieldAccessor(fieldName) =>
        val fieldTerm = newTermName(fieldName)
        s"($fieldTpe) $inputTerm.$fieldTerm"

      case ObjectMethodAccessor(methodName) =>
        val methodTerm = newTermName(methodName)
        s"($fieldTpe) $inputTerm.$methodTerm()"

      case ProductAccessor(i) =>
        s"($fieldTpe) $inputTerm.productElement($i)"

    }
  }

  sealed abstract class FieldAccessor

  case class ObjectFieldAccessor(fieldName: String) extends FieldAccessor

  case class ObjectMethodAccessor(methodName: String) extends FieldAccessor

  case class ProductAccessor(i: Int) extends FieldAccessor

  def fieldAccessorFor(elementType: CompositeType[_], fieldName: String): FieldAccessor = {
    elementType match {
      case ri: RowTypeInfo =>
        ProductAccessor(elementType.getFieldIndex(fieldName))

      case cc: CaseClassTypeInfo[_] =>
        ObjectMethodAccessor(fieldName)

      case javaTup: TupleTypeInfo[_] =>
        ObjectFieldAccessor(fieldName)

      case pj: PojoTypeInfo[_] =>
        ObjectFieldAccessor(fieldName)

      case proxy: RenamingProxyTypeInfo[_] =>
        val underlying = proxy.getUnderlyingType
        val fieldIndex = proxy.getFieldIndex(fieldName)
        fieldAccessorFor(underlying, underlying.getFieldNames()(fieldIndex))
    }
  }

  protected def defaultPrimitive(tpe: TypeInformation[_]): String = tpe match {
    case BasicTypeInfo.INT_TYPE_INFO => "-1"
    case BasicTypeInfo.LONG_TYPE_INFO => "-1"
    case BasicTypeInfo.SHORT_TYPE_INFO => "-1"
    case BasicTypeInfo.BYTE_TYPE_INFO => "-1"
    case BasicTypeInfo.FLOAT_TYPE_INFO => "-1.0f"
    case BasicTypeInfo.DOUBLE_TYPE_INFO => "-1.0d"
    case BasicTypeInfo.BOOLEAN_TYPE_INFO => "false"
    case BasicTypeInfo.STRING_TYPE_INFO => "\"<empty>\""
    case BasicTypeInfo.CHAR_TYPE_INFO => "'\\0'"
    case _ => "null"
  }

  protected def typeTermForTypeInfo(tpe: TypeInformation[_]): String = tpe match {

//    case BasicTypeInfo.INT_TYPE_INFO => "int"
//    case BasicTypeInfo.LONG_TYPE_INFO => "long"
//    case BasicTypeInfo.SHORT_TYPE_INFO => "short"
//    case BasicTypeInfo.BYTE_TYPE_INFO => "byte"
//    case BasicTypeInfo.FLOAT_TYPE_INFO => "float"
//    case BasicTypeInfo.DOUBLE_TYPE_INFO => "double"
//    case BasicTypeInfo.BOOLEAN_TYPE_INFO => "boolean"
//    case BasicTypeInfo.CHAR_TYPE_INFO => "char"

    // From PrimitiveArrayTypeInfo we would get class "int[]", scala reflections
    // does not seem to like this, so we manually give the correct type here.
    case PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO => "int[]"
    case PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO => "long[]"
    case PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO => "short[]"
    case PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO => "byte[]"
    case PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO => "float[]"
    case PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO => "double[]"
    case PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO => "boolean[]"
    case PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO => "char[]"

    case _ =>
      tpe.getTypeClass.getCanonicalName

  }
}
