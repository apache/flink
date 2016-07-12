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

package org.apache.flink.api.table.functions

import java.lang.reflect.{Method, Modifier}
import java.sql.{Date, Time, Timestamp}

import com.google.common.primitives.Primitives
import org.apache.calcite.sql.SqlFunction
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.table.functions.wrapper.ScalarSqlFunction
import org.apache.flink.api.table.{FlinkTypeFactory, ValidationException}
import org.apache.flink.api.table.expressions.{Expression, ScalarFunctionCall}

/**
  * Base class for a user-defined scalar function. A user-defined scalar functions maps zero, one,
  * or multiple scalar values to a new scalar value.
  *
  * The behavior of a [[ScalarFunction]] can be defined by implementing a custom evaluation
  * method. An evaluation method must be declared publicly and named "eval". Evaluation methods
  * can also be overloaded by implementing multiple methods named "eval".
  *
  * User-defined functions must have a default constructor and must be instantiable during runtime.
  *
  * By default the result type of an evaluation method is determined by Flink's type extraction
  * facilities. This is sufficient for basic types or simple POJOs but might be wrong for more
  * complex, custom, or composite types. In these cases [[TypeInformation]] of the result type
  * can be manually defined by overriding [[getResultType()]].
  *
  * Internally, the Table/SQL API code generation works with primitive values as much as possible.
  * If user-defined scalar function should not introduce much overhead during runtime, it is
  * recommended to declare parameters and result types as primitive types instead of their boxed
  * classes. DATE/TIME is equal to int, TIMESTAMP is equal to long.
  */
abstract class ScalarFunction extends UserDefinedFunction {

  /**
    * Creates a call to a [[ScalarFunction]] in Scala Table API.
    *
    * @param params actual parameters of function
    * @return [[Expression]] in form of a [[ScalarFunctionCall]]
    */
  def apply(params: Expression*): Expression = {
    ScalarFunctionCall(this, params)
  }

  // ----------------------------------------------------------------------------------------------

  private val evalMethods = checkAndExtractEvalMethods()
  private lazy val signatures = evalMethods.map(_.getParameterTypes)

  /**
    * Extracts evaluation methods and throws a [[ValidationException]] if no implementation
    * can be found.
    */
  private def checkAndExtractEvalMethods(): Array[Method] = {
    val methods = getClass.asSubclass(classOf[ScalarFunction])
      .getDeclaredMethods
      .filter { m =>
        val modifiers = m.getModifiers
        m.getName == "eval" && Modifier.isPublic(modifiers) && !Modifier.isAbstract(modifiers)
      }

    if (methods.isEmpty) {
      throw new ValidationException(s"Scalar function class '$this' does not implement at least " +
        s"one method named 'eval' which is public and not abstract.")
    } else {
      methods
    }
  }

  /**
    * Extracts type classes of [[TypeInformation]] in a null-aware way.
    */
  private def typeInfoToClass(typeInfos: Seq[TypeInformation[_]]): Array[Class[_]] =
    typeInfos.map { typeInfo =>
      if (typeInfo == null) {
        null
      } else {
        typeInfo.getTypeClass
      }
    }.toArray

  /**
    * Compares parameter candidate classes with expected classes. If true, the parameters match.
    * Candidate can be null (acts as a wildcard).
    */
  private def parameterTypeEquals(candidate: Class[_], expected: Class[_]): Boolean =
    candidate == null ||
      candidate == expected ||
      expected.isPrimitive && Primitives.wrap(expected) == candidate ||
      candidate == classOf[Date] && expected == classOf[Int] ||
      candidate == classOf[Time] && expected == classOf[Int] ||
      candidate == classOf[Timestamp] && expected == classOf[Long]

  /**
    * Returns all signature of the possibly overloaded function.
    */
  private[flink] def getSignatures: Array[Array[Class[_]]] = signatures

  /**
    * Returns signature matching given signature of [[TypeInformation]].
    * Elements of the signature can be null (act as a wildcard).
    */
  private[flink] def getSignature(signature: Seq[TypeInformation[_]]): Option[Array[Class[_]]] = {
    // We compare the raw Java classes not the TypeInformation.
    // TypeInformation does not matter during runtime (e.g. within a MapFunction).
    val actualSignature = typeInfoToClass(signature)

    getSignatures
      // go over all signatures and find one matching actual signature
      .find {
        // match parameters of signature to actual parameters
        _.zipWithIndex.forall { case (clazz, i) =>
            i < actualSignature.length && parameterTypeEquals(actualSignature(i), clazz)
        }
      }
  }

  /**
    * Internal method of [[getResultType()]] that does some pre-checking and uses
    * [[TypeExtractor]] as default return type inference.
    */
  private[flink] def getResultTypeInternal(signature: Array[Class[_]]): TypeInformation[_] = {
    // find method for signature
    val evalMethod = evalMethods
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new ValidationException("Given signature is invalid."))

    val userDefinedTypeInfo = getResultType(signature)
    if (userDefinedTypeInfo != null) {
        userDefinedTypeInfo
    } else {
      try {
        TypeExtractor.getForClass(evalMethod.getReturnType)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(s"Return type of scalar function '$this' cannot be " +
            s"automatically determined. Please provide type information manually.")
      }
    }
  }

  /**
    * Returns the return type of the evaluation method matching the given signature.
    */
  private[flink] def getResultTypeClass(signature: Array[Class[_]]): Class[_] = {
    // find method for signature
    val evalMethod = evalMethods
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new IllegalArgumentException("Given signature is invalid."))
    evalMethod.getReturnType
  }

  override private[flink] def createSqlFunction(
      name: String,
      typeFactory: FlinkTypeFactory)
    : SqlFunction = {
    new ScalarSqlFunction(name, this, typeFactory)
  }

  // ----------------------------------------------------------------------------------------------

  private[flink] def signaturesToString: String = {
    getSignatures.map(signatureToString).mkString(", ")
  }

  private[flink] def signatureToString(signature: Array[Class[_]]): String =
    "(" + signature.map { clazz =>
      if (clazz == null) {
        "null"
      } else {
        clazz.getCanonicalName
      }
    }.mkString(", ") + ")"

  private[flink] def signatureToString(signature: Seq[TypeInformation[_]]): String = {
    signatureToString(typeInfoToClass(signature))
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Returns the result type of the evaluation method with a given signature.
    *
    * This method needs to be overriden in case Flink's type extraction facilities are not
    * sufficient to extract the [[TypeInformation]] based on the return type of the evaluation
    * method. Flink's type extraction facilities can handle basic types or
    * simple POJOs but might be wrong for more complex, custom, or composite types.
    *
    * @param signature signature of the method the return type needs to be determined
    * @return [[TypeInformation]] of result type or null if Flink should determine the type
    */
  def getResultType(signature: Array[Class[_]]): TypeInformation[_] = null

  /**
    * Returns [[TypeInformation]] about the operands of the evaluation method with a given
    * signature.
    *
    * In order to perform operand type inference in SQL (especially when NULL is used) it might be
    * necessary to determine the parameter [[TypeInformation]] of an evaluation method.
    * By default Flink's type extraction facilities are used for this but might be wrong for
    * more complex, custom, or composite types.
    *
    * @param signature signature of the method the operand types need to be determined
    * @return [[TypeInformation]] of  operand types
    */
  def getParameterTypes(signature: Array[Class[_]]): Array[TypeInformation[_]] = {
    signature.map { c =>
      try {
        TypeExtractor.getForClass(c)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(s"Parameter types of scalar function '$this' cannot be " +
            s"automatically determined. Please provide type information manually.")
      }
    }
  }
}
