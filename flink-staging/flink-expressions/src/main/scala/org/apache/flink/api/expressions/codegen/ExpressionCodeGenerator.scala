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
package org.apache.flink.api.expressions.codegen

import java.util.concurrent.atomic.AtomicInteger
import org.apache.flink.api.expressions.tree._
import org.apache.flink.api.expressions.typeinfo.{RenamingProxyTypeInfo, RowTypeInfo}
import org.apache.flink.api.expressions.{ExpressionException, tree}
import org.apache.flink.api.common.typeinfo.{PrimitiveArrayTypeInfo, BasicTypeInfo, TypeInformation}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, PojoTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

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

  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  if (cl == null) {
    throw new IllegalArgumentException("ClassLoader must not be null.")
  }

  import scala.tools.reflect.ToolBox
  protected val (mirror, toolBox) = ReflectionLock.synchronized {
    val mirror = runtimeMirror(cl)
    (mirror, mirror.mkToolBox())
  }

  // This is to be implemented by subclasses, we have it like this
  // so that we only call it from here with the Scala Reflection Lock.
  protected def generateInternal(): R

  final def generate(): R = {
    ReflectionLock.synchronized {
      generateInternal()
    }
  }

  val cache = mutable.HashMap[Expression, GeneratedExpression]()

  protected def generateExpression(expr: Expression): GeneratedExpression = {
    // doesn't work yet, because we insert the same code twice and reuse variable names
//    cache.getOrElseUpdate(expr, generateExpressionInternal(expr))
    generateExpressionInternal(expr)
  }

  protected def generateExpressionInternal(expr: Expression): GeneratedExpression = {
//  protected def generateExpression(expr: Expression): GeneratedExpression = {
    val nullTerm = freshTermName("isNull")
    val resultTerm = freshTermName("result")

    // For binary predicates that must only be evaluated when both operands are non-null.
    // This will write to nullTerm and resultTerm, so don't use those term names
    // after using this function
    def generateIfNonNull(left: Expression, right: Expression, resultType: TypeInformation[_])
                         (expr: (TermName, TermName) => Tree): Seq[Tree] = {
      val leftCode = generateExpression(left)
      val rightCode = generateExpression(right)


      if (nullCheck) {
        leftCode.code ++ rightCode.code ++ q"""
        val $nullTerm = ${leftCode.nullTerm} || ${rightCode.nullTerm}
        val $resultTerm = if ($nullTerm) {
          ${defaultPrimitive(resultType)}
        } else {
          ${expr(leftCode.resultTerm, rightCode.resultTerm)}
        }
        """.children
      } else {
        leftCode.code ++ rightCode.code :+ q"""
        val $resultTerm = ${expr(leftCode.resultTerm, rightCode.resultTerm)}
        """
      }
    }

    val cleanedExpr = expr match {
      case tree.Naming(namedExpr, _) => namedExpr
      case _ => expr
    }

    val code: Seq[Tree] = cleanedExpr match {

      case tree.Literal(null, typeInfo) =>
        if (nullCheck) {
          q"""
            val $nullTerm = true
            val resultTerm = null
          """.children
        } else {
          Seq(q"""
            val resultTerm = null
          """)
        }

      case tree.Literal(intValue: Int, INT_TYPE_INFO) =>
        if (nullCheck) {
          q"""
            val $nullTerm = false
            val $resultTerm = $intValue
          """.children
        } else {
          Seq(q"""
            val $resultTerm = $intValue
          """)
        }

      case tree.Literal(longValue: Long, LONG_TYPE_INFO) =>
        if (nullCheck) {
          q"""
            val $nullTerm = false
            val $resultTerm = $longValue
          """.children
        } else {
          Seq(q"""
            val $resultTerm = $longValue
          """)
        }


      case tree.Literal(doubleValue: Double, DOUBLE_TYPE_INFO) =>
        if (nullCheck) {
          q"""
            val $nullTerm = false
            val $resultTerm = $doubleValue
          """.children
        } else {
          Seq(q"""
              val $resultTerm = $doubleValue
          """)
        }

      case tree.Literal(floatValue: Float, FLOAT_TYPE_INFO) =>
        if (nullCheck) {
          q"""
            val $nullTerm = false
            val $resultTerm = $floatValue
          """.children
        } else {
          Seq(q"""
              val $resultTerm = $floatValue
          """)
        }

      case tree.Literal(strValue: String, STRING_TYPE_INFO) =>
        if (nullCheck) {
          q"""
            val $nullTerm = false
            val $resultTerm = $strValue
          """.children
        } else {
          Seq(q"""
              val $resultTerm = $strValue
          """)
        }

      case tree.Literal(boolValue: Boolean, BOOLEAN_TYPE_INFO) =>
        if (nullCheck) {
          q"""
            val $nullTerm = false
            val $resultTerm = $boolValue
          """.children
        } else {
          Seq(q"""
              val $resultTerm = $boolValue
          """)
        }

      case Substring(str, beginIndex, endIndex) =>
        val strCode = generateExpression(str)
        val beginIndexCode = generateExpression(beginIndex)
        val endIndexCode = generateExpression(endIndex)
        if (nullCheck) {
          strCode.code ++ beginIndexCode.code ++ endIndexCode.code ++ q"""
            val $nullTerm =
              ${strCode.nullTerm} || ${beginIndexCode.nullTerm} || ${endIndexCode.nullTerm}
            if ($nullTerm) {
              ${defaultPrimitive(str.typeInfo)}
            } else {
              val $resultTerm = if (${endIndexCode.resultTerm} == Int.MaxValue) {
                 (${strCode.resultTerm}).substring(${beginIndexCode.resultTerm})
              } else {
                (${strCode.resultTerm}).substring(
                  ${beginIndexCode.resultTerm},
                  ${endIndexCode.resultTerm})
              }
            }
          """.children
        } else {
          strCode.code ++ beginIndexCode.code ++ endIndexCode.code :+ q"""
            val $resultTerm = if (${endIndexCode.resultTerm} == Int.MaxValue) {
              (${strCode.resultTerm}).substring(${beginIndexCode.resultTerm})
            } else {
              (${strCode.resultTerm}).substring(
                ${beginIndexCode.resultTerm},
                ${endIndexCode.resultTerm})
            }
          """
        }

      case tree.Cast(child: Expression, STRING_TYPE_INFO) =>
        val childGen = generateExpression(child)
        val castCode = if (nullCheck) {
          q"""
            val $nullTerm = ${childGen.nullTerm}
            val $resultTerm = if ($nullTerm == null) {
              null
            } else {
              ${childGen.resultTerm}.toString
            }
          """.children
        } else {
          Seq(q"""
            val $resultTerm = ${childGen.resultTerm}.toString
          """)
        }
        childGen.code ++ castCode

      case tree.Cast(child: Expression, INT_TYPE_INFO) =>
        val childGen = generateExpression(child)
        val castCode = if (nullCheck) {
          q"""
            val $nullTerm = ${childGen.nullTerm}
            val $resultTerm = ${childGen.resultTerm}.toInt
          """.children
        } else {
          Seq(q"""
            val $resultTerm = ${childGen.resultTerm}.toInt
          """)
        }
        childGen.code ++ castCode

      case tree.Cast(child: Expression, LONG_TYPE_INFO) =>
        val childGen = generateExpression(child)
        val castCode = if (nullCheck) {
          q"""
            val $nullTerm = ${childGen.nullTerm}
            val $resultTerm = ${childGen.resultTerm}.toLong
          """.children
        } else {
          Seq(q"""
            val $resultTerm = ${childGen.resultTerm}.toLong
          """)
        }
        childGen.code ++ castCode

      case tree.Cast(child: Expression, FLOAT_TYPE_INFO) =>
        val childGen = generateExpression(child)
        val castCode = if (nullCheck) {
          q"""
            val $nullTerm = ${childGen.nullTerm}
            val $resultTerm = ${childGen.resultTerm}.toFloat
          """.children
        } else {
          Seq(q"""
            val $resultTerm = ${childGen.resultTerm}.toFloat
          """)
        }
        childGen.code ++ castCode

      case tree.Cast(child: Expression, DOUBLE_TYPE_INFO) =>
        val childGen = generateExpression(child)
        val castCode = if (nullCheck) {
          q"""
            val $nullTerm = ${childGen.nullTerm}
            val $resultTerm = ${childGen.resultTerm}.toDouble
          """.children
        } else {
          Seq(q"""
            val $resultTerm = ${childGen.resultTerm}.toDouble
          """)
        }
        childGen.code ++ castCode

      case ResolvedFieldReference(fieldName, fieldTpe: TypeInformation[_]) =>
        inputs find { i => i._2.hasField(fieldName) } match {
          case Some((inputName, inputTpe)) =>
            val fieldCode = getField(newTermName(inputName), inputTpe, fieldName, fieldTpe)
            if (nullCheck) {
              q"""
                val $resultTerm = $fieldCode
                val $nullTerm = $resultTerm == null
              """.children
            } else {
              Seq(q"""
                val $resultTerm = $fieldCode
              """)
            }

          case None => throw new ExpressionException("Could not get accessor for " + fieldName
            + " in inputs " + inputs.mkString(", ") + ".")
        }

      case GreaterThan(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => q"$leftTerm > $rightTerm"
        }

      case GreaterThanOrEqual(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => q"$leftTerm >= $rightTerm"
        }

      case LessThan(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => q"$leftTerm < $rightTerm"
        }

      case LessThanOrEqual(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => q"$leftTerm <= $rightTerm"
        }

      case EqualTo(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => q"$leftTerm == $rightTerm"
        }

      case NotEqualTo(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => q"$leftTerm != $rightTerm"
        }

      case And(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => q"$leftTerm && $rightTerm"
        }

      case Or(left, right) =>
        generateIfNonNull(left, right, BOOLEAN_TYPE_INFO) {
          (leftTerm, rightTerm) => q"$leftTerm || $rightTerm"
        }

      case Plus(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => q"$leftTerm + $rightTerm"
        }

      case Minus(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => q"$leftTerm - $rightTerm"
        }

      case Div(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => q"$leftTerm / $rightTerm"
        }

      case Mul(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => q"$leftTerm * $rightTerm"
        }

      case Mod(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => q"$leftTerm % $rightTerm"
        }

      case UnaryMinus(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code ++ q"""
            val $nullTerm = ${childCode.nullTerm}
            if ($nullTerm) {
              ${defaultPrimitive(child.typeInfo)}
            } else {
              val $resultTerm = -(${childCode.resultTerm})
            }
          """.children
        } else {
          childCode.code :+ q"""
              val $resultTerm = -(${childCode.resultTerm})
          """
        }

      case BitwiseAnd(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => q"$leftTerm & $rightTerm"
        }

      case BitwiseOr(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => q"$leftTerm | $rightTerm"
        }

      case BitwiseXor(left, right) =>
        generateIfNonNull(left, right, expr.typeInfo) {
          (leftTerm, rightTerm) => q"$leftTerm ^ $rightTerm"
        }

      case BitwiseNot(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code ++ q"""
            val $nullTerm = ${childCode.nullTerm}
            if ($nullTerm) {
              ${defaultPrimitive(child.typeInfo)}
            } else {
              val $resultTerm = ~(${childCode.resultTerm})
            }
          """.children
        } else {
          childCode.code :+ q"""
              val $resultTerm = ~(${childCode.resultTerm})
          """
        }

      case Not(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code ++ q"""
            val $nullTerm = ${childCode.nullTerm}
            if ($nullTerm) {
              ${defaultPrimitive(child.typeInfo)}
            } else {
              val $resultTerm = !(${childCode.resultTerm})
            }
          """.children
        } else {
          childCode.code :+ q"""
              val $resultTerm = !(${childCode.resultTerm})
          """
        }

      case IsNull(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code ++ q"""
            val $nullTerm = ${childCode.nullTerm}
            if ($nullTerm) {
              ${defaultPrimitive(child.typeInfo)}
            } else {
              val $resultTerm = (${childCode.resultTerm}) == null
            }
          """.children
        } else {
          childCode.code :+ q"""
              val $resultTerm = (${childCode.resultTerm}) == null
          """
        }

      case IsNotNull(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code ++ q"""
            val $nullTerm = ${childCode.nullTerm}
            if ($nullTerm) {
              ${defaultPrimitive(child.typeInfo)}
            } else {
              val $resultTerm = (${childCode.resultTerm}) != null
            }
          """.children
        } else {
          childCode.code :+ q"""
              val $resultTerm = (${childCode.resultTerm}) != null
          """
        }

      case Abs(child) =>
        val childCode = generateExpression(child)
        if (nullCheck) {
          childCode.code ++ q"""
            val $nullTerm = ${childCode.nullTerm}
            if ($nullTerm) {
              ${defaultPrimitive(child.typeInfo)}
            } else {
              val $resultTerm = Math.abs(${childCode.resultTerm})
            }
          """.children
        } else {
          childCode.code :+ q"""
              val $resultTerm = Math.abs(${childCode.resultTerm})
          """
        }

      case _ => throw new ExpressionException("Could not generate code for expression " + expr)
    }

    GeneratedExpression(code, resultTerm, nullTerm)
  }

  case class GeneratedExpression(code: Seq[Tree], resultTerm: TermName, nullTerm: TermName)

  // We don't have c.freshName
  // According to http://docs.scala-lang.org/overviews/quasiquotes/hygiene.html
  // it's coming for 2.11. We can't wait that long...
  def freshTermName(name: String): TermName = {
    newTermName(s"$name$$${freshNameCounter.getAndIncrement}")
  }

  val freshNameCounter = new AtomicInteger

  protected def getField(
      inputTerm: TermName,
      inputType: CompositeType[_],
      fieldName: String,
      fieldType: TypeInformation[_]): Tree = {
    val accessor = fieldAccessorFor(inputType, fieldName)
    accessor match {
      case ObjectFieldAccessor(fieldName) =>
        val fieldTerm = newTermName(fieldName)
        q"$inputTerm.$fieldTerm.asInstanceOf[${typeTermForTypeInfo(fieldType)}]"

      case ObjectMethodAccessor(methodName) =>
        val methodTerm = newTermName(methodName)
        q"$inputTerm.$methodTerm().asInstanceOf[${typeTermForTypeInfo(fieldType)}]"

      case ProductAccessor(i) =>
        q"$inputTerm.productElement($i).asInstanceOf[${typeTermForTypeInfo(fieldType)}]"

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
        ObjectFieldAccessor(fieldName)

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

  protected def defaultPrimitive(tpe: TypeInformation[_]) = tpe match {
    case BasicTypeInfo.INT_TYPE_INFO => ru.Literal(Constant(-1))
    case BasicTypeInfo.LONG_TYPE_INFO => ru.Literal(Constant(1L))
    case BasicTypeInfo.SHORT_TYPE_INFO => ru.Literal(Constant(-1.toShort))
    case BasicTypeInfo.BYTE_TYPE_INFO => ru.Literal(Constant(-1.toByte))
    case BasicTypeInfo.FLOAT_TYPE_INFO => ru.Literal(Constant(-1.0.toFloat))
    case BasicTypeInfo.DOUBLE_TYPE_INFO => ru.Literal(Constant(-1.toDouble))
    case BasicTypeInfo.BOOLEAN_TYPE_INFO => ru.Literal(Constant(false))
    case BasicTypeInfo.STRING_TYPE_INFO => ru.Literal(Constant("<empty>"))
    case BasicTypeInfo.CHAR_TYPE_INFO => ru.Literal(Constant('\0'))
    case _ => ru.Literal(Constant(null))
  }

  protected def typeTermForTypeInfo(typeInfo: TypeInformation[_]): Tree = {
    val tpe = typeForTypeInfo(typeInfo)
    tq"$tpe"
  }

  // We need two separate methods here because typeForTypeInfo is recursive when generating
  // the type for a type with generic parameters.
  protected def typeForTypeInfo(tpe: TypeInformation[_]): Type = tpe match {

    // From PrimitiveArrayTypeInfo we would get class "int[]", scala reflections
    // does not seem to like this, so we manually give the correct type here.
    case PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Int]]
    case PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Long]]
    case PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Short]]
    case PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Byte]]
    case PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Float]]
    case PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Double]]
    case PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Boolean]]
    case PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO => typeOf[Array[Char]]

    case _ =>
      val clazz = mirror.staticClass(tpe.getTypeClass.getCanonicalName)

      clazz.selfType.erasure match {
        case ExistentialType(_, underlying) => underlying

        case tpe@TypeRef(prefix, sym, Nil) =>
          // Non-generic type, just return the type
          tpe

        case TypeRef(prefix, sym, emptyParams) =>
          val genericTypeInfos = tpe.getGenericParameters.asScala
          if (emptyParams.length != genericTypeInfos.length) {
            throw new RuntimeException("Number of type parameters does not match.")
          }
          val typeParams = genericTypeInfos.map(typeForTypeInfo)
          TypeRef(prefix, sym, typeParams.toList)
      }

  }

}
