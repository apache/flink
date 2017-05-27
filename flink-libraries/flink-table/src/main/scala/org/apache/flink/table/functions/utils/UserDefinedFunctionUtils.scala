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

import java.lang.{Integer => JInt, Long => JLong}
import java.lang.reflect.{Method, Modifier}
import java.sql.{Date, Time, Timestamp}

import org.apache.commons.codec.binary.Base64
import com.google.common.primitives.Primitives
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlCallBinding, SqlFunction}
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.api.{TableEnvironment, TableException, ValidationException}
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.util.InstantiationUtil

object UserDefinedFunctionUtils {

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
  // Utilities for user-defined methods
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns signatures of eval methods matching the given signature of [[TypeInformation]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getEvalMethodSignature(
    function: UserDefinedFunction,
    signature: Seq[TypeInformation[_]])
    : Option[Array[Class[_]]] = {
    getUserDefinedMethod(function, "eval", typeInfoToClass(signature)).map(_.getParameterTypes)
  }

  /**
    * Returns signatures of accumulate methods matching the given signature of [[TypeInformation]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getAccumulateMethodSignature(
      function: AggregateFunction[_, _],
      signature: Seq[TypeInformation[_]])
  : Option[Array[Class[_]]] = {
    val accType = TypeExtractor.createTypeInfo(
      function, classOf[AggregateFunction[_, _]], function.getClass, 1)
    val input = (Array(accType) ++ signature).toSeq
    getUserDefinedMethod(
      function,
      "accumulate",
      typeInfoToClass(input)).map(_.getParameterTypes)
  }

  def getParameterTypes(
      function: UserDefinedFunction,
      signature: Array[Class[_]]): Array[TypeInformation[_]] = {
    signature.map { c =>
      try {
        TypeExtractor.getForClass(c)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(
            s"Parameter types of function '${function.getClass.getCanonicalName}' cannot be " +
              s"automatically determined. Please provide type information manually.")
      }
    }
  }

  /**
    * Returns user defined method matching the given name and signature.
    *
    * @param function        function instance
    * @param methodName      method name
    * @param methodSignature an array of raw Java classes. We compare the raw Java classes not the
    *                        TypeInformation. TypeInformation does not matter during runtime (e.g.
    *                        within a MapFunction)
    */
  def getUserDefinedMethod(
      function: UserDefinedFunction,
      methodName: String,
      methodSignature: Array[Class[_]])
    : Option[Method] = {

    val methods = checkAndExtractMethods(function, methodName)

    val filtered = methods
      // go over all the methods and filter out matching methods
      .filter {
        case cur if !cur.isVarArgs =>
          val signatures = cur.getParameterTypes
          // match parameters of signature to actual parameters
          methodSignature.length == signatures.length &&
            signatures.zipWithIndex.forall { case (clazz, i) =>
              parameterTypeEquals(methodSignature(i), clazz)
          }
        case cur if cur.isVarArgs =>
          val signatures = cur.getParameterTypes
          methodSignature.zipWithIndex.forall {
            // non-varargs
            case (clazz, i) if i < signatures.length - 1  =>
              parameterTypeEquals(clazz, signatures(i))
            // varargs
            case (clazz, i) if i >= signatures.length - 1 =>
              parameterTypeEquals(clazz, signatures.last.getComponentType)
          } || (methodSignature.isEmpty && signatures.length == 1) // empty varargs
    }

    // if there is a fixed method, compiler will call this method preferentially
    val fixedMethodsCount = filtered.count(!_.isVarArgs)
    val found = filtered.filter { cur =>
      fixedMethodsCount > 0 && !cur.isVarArgs ||
      fixedMethodsCount == 0 && cur.isVarArgs
    }

    // check if there is a Scala varargs annotation
    if (found.isEmpty &&
      methods.exists { method =>
        val signatures = method.getParameterTypes
        signatures.zipWithIndex.forall {
          case (clazz, i) if i < signatures.length - 1 =>
            parameterTypeEquals(methodSignature(i), clazz)
          case (clazz, i) if i == signatures.length - 1 =>
            clazz.getName.equals("scala.collection.Seq")
        }
      }) {
      throw new ValidationException(
        s"Scala-style variable arguments in '${methodName}' methods are not supported. Please " +
          s"add a @scala.annotation.varargs annotation.")
    } else if (found.length > 1) {
      throw new ValidationException(
        s"Found multiple '${methodName}' methods which match the signature.")
    }
    found.headOption
  }

  /**
    * Check if a given method exists in the given function
    */
  def ifMethodExistInFunction(method: String, function: UserDefinedFunction): Boolean = {
    val methods = function
      .getClass
      .getMethods
      .filter {
        m => m.getName == method
      }
    !methods.isEmpty
  }

  /**
    * Extracts methods and throws a [[ValidationException]] if no implementation
    * can be found, or implementation does not match the requirements.
    */
  def checkAndExtractMethods(
      function: UserDefinedFunction,
      methodName: String): Array[Method] = {
    val methods = function
      .getClass
      .getMethods
      .filter { m =>
        val modifiers = m.getModifiers
        m.getName == methodName &&
          Modifier.isPublic(modifiers) &&
          !Modifier.isAbstract(modifiers) &&
          !(function.isInstanceOf[TableFunction[_]] && Modifier.isStatic(modifiers))
      }

    if (methods.isEmpty) {
      throw new ValidationException(
        s"Function class '${function.getClass.getCanonicalName}' does not implement at least " +
          s"one method named '${methodName}' which is public, not abstract and " +
          s"(in case of table functions) not static.")
    }

    methods
  }

  def getMethodSignatures(
      function: UserDefinedFunction,
      methodName: String): Array[Array[Class[_]]] = {
    checkAndExtractMethods(function, methodName).map(_.getParameterTypes)
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
    val evalMethods = checkAndExtractMethods(tableFunction, "eval")

    evalMethods.map { method =>
      val function = new FlinkTableFunctionImpl(resultType, fieldIndexes, fieldNames, method)
      TableSqlFunction(name, tableFunction, resultType, typeFactory, function)
    }
  }

  /**
    * Create [[SqlFunction]] for an [[AggregateFunction]]
    *
    * @param name function name
    * @param aggFunction aggregate function
    * @param typeFactory type factory
    * @return the TableSqlFunction
    */
  def createAggregateSqlFunction(
      name: String,
      aggFunction: AggregateFunction[_, _],
      typeInfo: TypeInformation[_],
      typeFactory: FlinkTypeFactory)
  : SqlFunction = {
    //check if a qualified accumulate method exists before create Sql function
    checkAndExtractMethods(aggFunction, "accumulate")
    val resultType: TypeInformation[_] = getResultTypeOfAggregateFunction(aggFunction, typeInfo)
    AggSqlFunction(name, aggFunction, resultType, typeFactory, aggFunction.requiresOver)
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for user-defined functions
  // ----------------------------------------------------------------------------------------------

  /**
    * Internal method of AggregateFunction#getResultType() that does some pre-checking and uses
    * [[TypeExtractor]] as default return type inference.
    */
  def getResultTypeOfAggregateFunction(
      aggregateFunction: AggregateFunction[_, _],
      extractedType: TypeInformation[_] = null)
    : TypeInformation[_] = {

    val resultType = try {
      val method: Method = aggregateFunction.getClass.getMethod("getResultType")
      method.invoke(aggregateFunction).asInstanceOf[TypeInformation[_]]
    } catch {
      case _: NoSuchMethodException => null
      case ite: Throwable => throw new TableException("Unexpected exception:", ite)
    }
    if (resultType != null) {
      resultType
    } else if(extractedType != null) {
      extractedType
    } else {
      TypeExtractor
        .createTypeInfo(aggregateFunction,
                        classOf[AggregateFunction[_, _]],
                        aggregateFunction.getClass,
                        0)
        .asInstanceOf[TypeInformation[_]]
    }
  }

  /**
    * Internal method of [[ScalarFunction#getResultType()]] that does some pre-checking and uses
    * [[TypeExtractor]] as default return type inference.
    */
  def getResultTypeOfScalarFunction(
      function: ScalarFunction,
      signature: Array[Class[_]])
    : TypeInformation[_] = {

    val userDefinedTypeInfo = function.getResultType(signature)
    if (userDefinedTypeInfo != null) {
      userDefinedTypeInfo
    } else {
      try {
        TypeExtractor.getForClass(getResultTypeClassOfScalarFunction(function, signature))
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
  def getResultTypeClassOfScalarFunction(
      function: ScalarFunction,
      signature: Array[Class[_]])
    : Class[_] = {
    // find method for signature
    val evalMethod = checkAndExtractMethods(function, "eval")
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
    * Prints all signatures of methods with given name in a class.
    */
  def signaturesToString(function: UserDefinedFunction, name: String): String = {
    getMethodSignatures(function, name).map(signatureToString).mkString(", ")
  }

  /**
    * Extracts type classes of [[TypeInformation]] in a null-aware way.
    */
  def typeInfoToClass(typeInfos: Seq[TypeInformation[_]]): Array[Class[_]] =
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
    expected == classOf[Object] ||
    expected.isPrimitive && Primitives.wrap(expected) == candidate ||
    candidate == classOf[Date] && (expected == classOf[Int] || expected == classOf[JInt])  ||
    candidate == classOf[Time] && (expected == classOf[Int] || expected == classOf[JInt]) ||
    candidate == classOf[Timestamp] && (expected == classOf[Long] || expected == classOf[JLong]) ||
    (candidate.isArray &&
      expected.isArray &&
      candidate.getComponentType.isInstanceOf[Object] &&
      expected.getComponentType == classOf[Object])

  @throws[Exception]
  def serialize(function: UserDefinedFunction): String = {
    val byteArray = InstantiationUtil.serializeObject(function)
    Base64.encodeBase64URLSafeString(byteArray)
  }

  @throws[Exception]
  def deserialize(data: String): UserDefinedFunction = {
    val byteData = Base64.decodeBase64(data)
    InstantiationUtil
      .deserializeObject[UserDefinedFunction](byteData, Thread.currentThread.getContextClassLoader)
  }

  /**
    * Creates a [[LogicalTableFunctionCall]] by parsing a String expression.
    *
    * @param tableEnv The table environmenent to lookup the function.
    * @param udtf a String expression of a TableFunctionCall, such as "split(c)"
    * @return A LogicalTableFunctionCall.
    */
  def createLogicalFunctionCall(
      tableEnv: TableEnvironment,
      udtf: String): LogicalTableFunctionCall = {

    var alias: Option[Seq[String]] = None

    // unwrap an Expression until we get a TableFunctionCall
    def unwrap(expr: Expression): TableFunctionCall = expr match {
      case Alias(child, name, extraNames) =>
        alias = Some(Seq(name) ++ extraNames)
        unwrap(child)
      case Call(name, args) =>
        val function = tableEnv.functionCatalog.lookupFunction(name, args)
        unwrap(function)
      case c: TableFunctionCall => c
      case _ =>
        throw new TableException(
          "Table(TableEnv, String) constructor only accept String that " +
            "define table function followed by some Alias.")
    }

    val functionCall: LogicalTableFunctionCall = unwrap(ExpressionParser.parseExpression(udtf))
      .as(alias).toLogicalTableFunctionCall(child = null)
    functionCall
  }

  def getOperandTypeInfo(callBinding: SqlCallBinding): Seq[TypeInformation[_]] = {
    val operandTypes = for (i <- 0 until callBinding.getOperandCount)
      yield callBinding.getOperandType(i)
    operandTypes.map { operandType =>
      if (operandType.getSqlTypeName == SqlTypeName.NULL) {
        null
      } else {
        FlinkTypeFactory.toTypeInfo(operandType)
      }
    }
  }
}
