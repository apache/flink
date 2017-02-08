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


package org.apache.flink.table.functions.utils

import java.lang.{Long => JLong, Integer => JInt}
import java.lang.reflect.{Method, Modifier}
import java.sql.{Date, Time, Timestamp}

import com.google.common.primitives.Primitives
import org.apache.calcite.sql.SqlFunction
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.functions.{ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.util.InstantiationUtil

object UserDefinedFunctionUtils {

  /**
    * Instantiates a user-defined function.
    */
  def instantiate[T <: UserDefinedFunction](clazz: Class[T]): T = {
    val constructor = clazz.getDeclaredConstructor()
    constructor.setAccessible(true)
    constructor.newInstance()
  }

  /**
    * Checks if a user-defined function can be easily instantiated.
    */
  def checkForInstantiation(clazz: Class[_]): Unit = {
    if (!InstantiationUtil.isPublic(clazz)) {
      throw ValidationException("Function class is not public.")
    }
    else if (!InstantiationUtil.isProperClass(clazz)) {
      throw ValidationException("Function class is no proper class, it is either abstract," +
        " an interface, or a primitive type.")
    }
    else if (InstantiationUtil.isNonStaticInnerClass(clazz)) {
      throw ValidationException("The class is an inner class, but not statically accessible.")
    }

    // check for default constructor (can be private)
    clazz
      .getDeclaredConstructors
      .find(_.getParameterTypes.isEmpty)
      .getOrElse(throw ValidationException("Function class needs a default constructor."))
  }

  /**
    * Check whether this is a Scala object. It is forbidden to use [[TableFunction]] implemented
    * by a Scala object, since concurrent risks.
    */
  def checkNotSingleton(clazz: Class[_]): Unit = {
    // TODO it is not a good way to check singleton. Maybe improve it further.
    if (clazz.getFields.map(_.getName) contains "MODULE$") {
      throw new ValidationException(
        s"TableFunction implemented by class ${clazz.getCanonicalName} " +
          s"is a Scala object, it is forbidden since concurrent risks.")
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for eval methods
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns signatures matching the given signature of [[TypeInformation]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getSignature(
      function: UserDefinedFunction,
      signature: Seq[TypeInformation[_]])
    : Option[Array[Class[_]]] = {
    // We compare the raw Java classes not the TypeInformation.
    // TypeInformation does not matter during runtime (e.g. within a MapFunction).
    val actualSignature = typeInfoToClass(signature)
    val signatures = getSignatures(function)

    signatures
      // go over all signatures and find one matching actual signature
      .find { curSig =>
      // match parameters of signature to actual parameters
      actualSignature.length == curSig.length &&
        curSig.zipWithIndex.forall { case (clazz, i) =>
          parameterTypeEquals(actualSignature(i), clazz)
        }
    }
  }

  /**
    * Returns eval method matching the given signature of [[TypeInformation]].
    */
  def getEvalMethod(
      function: UserDefinedFunction,
      signature: Seq[TypeInformation[_]])
    : Option[Method] = {
    // We compare the raw Java classes not the TypeInformation.
    // TypeInformation does not matter during runtime (e.g. within a MapFunction).
    val actualSignature = typeInfoToClass(signature)
    val evalMethods = checkAndExtractEvalMethods(function)

    evalMethods
      // go over all eval methods and find one matching
      .find { cur =>
      val signatures = cur.getParameterTypes
      // match parameters of signature to actual parameters
      actualSignature.length == signatures.length &&
        signatures.zipWithIndex.forall { case (clazz, i) =>
          parameterTypeEquals(actualSignature(i), clazz)
        }
    }
  }

  /**
    * Extracts "eval" methods and throws a [[ValidationException]] if no implementation
    * can be found.
    */
  def checkAndExtractEvalMethods(function: UserDefinedFunction): Array[Method] = {
    val methods = function
      .getClass
      .getDeclaredMethods
      .filter { m =>
        val modifiers = m.getModifiers
        m.getName == "eval" &&
          Modifier.isPublic(modifiers) &&
          !Modifier.isAbstract(modifiers) &&
          !(function.isInstanceOf[TableFunction[_]] && Modifier.isStatic(modifiers))
      }

    if (methods.isEmpty) {
      throw new ValidationException(
        s"Function class '${function.getClass.getCanonicalName}' does not implement at least " +
          s"one method named 'eval' which is public, not abstract and " +
          s"(in case of table functions) not static.")
    } else {
      methods
    }
  }

  def getSignatures(function: UserDefinedFunction): Array[Array[Class[_]]] = {
    checkAndExtractEvalMethods(function).map(_.getParameterTypes)
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for SQL functions
  // ----------------------------------------------------------------------------------------------

  /**
    * Create [[SqlFunction]] for a [[ScalarFunction]]
 *
    * @param name function name
    * @param function scalar function
    * @param typeFactory type factory
    * @return the ScalarSqlFunction
    */
  def createScalarSqlFunction(
      name: String,
      function: ScalarFunction,
      typeFactory: FlinkTypeFactory)
    : SqlFunction = {
    new ScalarSqlFunction(name, function, typeFactory)
  }

  /**
    * Create [[SqlFunction]]s for a [[TableFunction]]'s every eval method
 *
    * @param name function name
    * @param tableFunction table function
    * @param resultType the type information of returned table
    * @param typeFactory type factory
    * @return the TableSqlFunction
    */
  def createTableSqlFunctions(
      name: String,
      tableFunction: TableFunction[_],
      resultType: TypeInformation[_],
      typeFactory: FlinkTypeFactory)
    : Seq[SqlFunction] = {
    val (fieldNames, fieldIndexes, _) = UserDefinedFunctionUtils.getFieldInfo(resultType)
    val evalMethods = checkAndExtractEvalMethods(tableFunction)

    evalMethods.map { method =>
      val function = new FlinkTableFunctionImpl(resultType, fieldIndexes, fieldNames, method)
      TableSqlFunction(name, tableFunction, resultType, typeFactory, function)
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for scalar functions
  // ----------------------------------------------------------------------------------------------

  /**
    * Internal method of [[ScalarFunction#getResultType()]] that does some pre-checking and uses
    * [[TypeExtractor]] as default return type inference.
    */
  def getResultType(
      function: ScalarFunction,
      signature: Array[Class[_]])
    : TypeInformation[_] = {
    // find method for signature
    val evalMethod = checkAndExtractEvalMethods(function)
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new ValidationException("Given signature is invalid."))

    val userDefinedTypeInfo = function.getResultType(signature)
    if (userDefinedTypeInfo != null) {
      userDefinedTypeInfo
    } else {
      try {
        TypeExtractor.getForClass(evalMethod.getReturnType)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(
            s"Return type of scalar function '${function.getClass.getCanonicalName}' cannot be " +
              s"automatically determined. Please provide type information manually.")
      }
    }
  }

  /**
    * Returns the return type of the evaluation method matching the given signature.
    */
  def getResultTypeClass(
      function: ScalarFunction,
      signature: Array[Class[_]])
    : Class[_] = {
    // find method for signature
    val evalMethod = checkAndExtractEvalMethods(function)
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new IllegalArgumentException("Given signature is invalid."))
    evalMethod.getReturnType
  }

  // ----------------------------------------------------------------------------------------------
  // Miscellaneous
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * Field names are automatically extracted for
    * [[org.apache.flink.api.common.typeutils.CompositeType]].
    *
    * @param inputType The TypeInformation to extract the field names and positions from.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo(inputType: TypeInformation[_])
    : (Array[String], Array[Int], Array[TypeInformation[_]]) = {

    (TableEnvironment.getFieldNames(inputType),
    TableEnvironment.getFieldIndices(inputType),
    TableEnvironment.getFieldTypes(inputType))
  }

  /**
    * Prints one signature consisting of classes.
    */
  def signatureToString(signature: Array[Class[_]]): String =
  signature.map { clazz =>
    if (clazz == null) {
      "null"
    } else {
      clazz.getCanonicalName
    }
  }.mkString("(", ", ", ")")

  /**
    * Prints one signature consisting of TypeInformation.
    */
  def signatureToString(signature: Seq[TypeInformation[_]]): String = {
    signatureToString(typeInfoToClass(signature))
  }

  /**
    * Prints all eval methods signatures of a class.
    */
  def signaturesToString(function: UserDefinedFunction): String = {
    getSignatures(function).map(signatureToString).mkString(", ")
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
    candidate == classOf[Date] && (expected == classOf[Int] || expected == classOf[JInt])  ||
    candidate == classOf[Time] && (expected == classOf[Int] || expected == classOf[JInt]) ||
    candidate == classOf[Timestamp] && (expected == classOf[Long] || expected == classOf[JLong])

}
