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


package org.apache.flink.table.planner.functions.utils

import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, Decimal, SqlTimestamp}
import org.apache.flink.table.functions._
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.schema.DeferredTypeFlinkTableFunction
import org.apache.flink.table.runtime.types.ClassDataTypeConverter.fromClassToDataType
import org.apache.flink.table.runtime.types.ClassLogicalTypeConverter.{getDefaultExternalClassForType, getInternalClassForType}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.{fromDataTypeToLogicalType, fromLogicalTypeToDataType}
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isRaw
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot, RowType}
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.FieldInfoUtils
import org.apache.flink.types.Row
import org.apache.flink.util.InstantiationUtil
import com.google.common.primitives.Primitives
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlFunction, SqlOperatorBinding}
import java.lang.reflect.{Method, Modifier}
import java.lang.{Integer => JInt, Long => JLong}
import java.sql.{Date, Time, Timestamp}
import java.time.{Instant, LocalDateTime}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.postfixOps


object UserDefinedFunctionUtils {

  /**
    * Checks if a user-defined function can be easily instantiated.
    */
  def checkForInstantiation(clazz: Class[_]): Unit = {
    if (!InstantiationUtil.isPublic(clazz)) {
      throw new ValidationException(s"Function class ${clazz.getCanonicalName} is not public.")
    }
    else if (!InstantiationUtil.isProperClass(clazz)) {
      throw new ValidationException(
        s"Function class ${clazz.getCanonicalName} is no proper class," +
            " it is either abstract, an interface, or a primitive type.")
    }
    else if (InstantiationUtil.isNonStaticInnerClass(clazz)) {
      throw new ValidationException(
        s"The class ${clazz.getCanonicalName} is an inner class, but" +
            " not statically accessible.")
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

  def throwValidationException(
      name: String,
      func: UserDefinedFunction,
      parameters: Array[LogicalType]): Method = {
    throw new ValidationException(
      s"Given parameters of function '$name' do not match any signature. \n" +
          s"Actual: ${signatureInternalToString(parameters)} \n" +
          s"Expected: ${signaturesToString(func, "eval")}")
  }

  private[table] def getParamClassesConsiderVarArgs(
      isVarArgs: Boolean,
      matchingSignature: Array[Class[_]],
      expectedLength: Int): Array[Class[_]] = {
    var paramClasses = new mutable.ArrayBuffer[Class[_]]
    for (i <- 0 until expectedLength) {
      if (i < matchingSignature.length - 1) {
        paramClasses += matchingSignature(i)
      } else if (isVarArgs) {
        paramClasses += matchingSignature.last.getComponentType
      } else {
        // last argument is not an array type
        paramClasses += matchingSignature.last
      }
    }
    paramClasses.toArray
  }

  def getEvalMethodSignature(
      func: ScalarFunction,
      expectedTypes: Array[LogicalType]): Array[Class[_]] = {
    val method = getEvalUserDefinedMethod(func, expectedTypes).getOrElse(
      throwValidationException(func.getClass.getCanonicalName, func, expectedTypes)
    )
    getParamClassesConsiderVarArgs(method.isVarArgs, method.getParameterTypes, expectedTypes.length)
  }

  def getEvalMethodSignatureOption(
      func: ScalarFunction,
      expectedTypes: Array[LogicalType]): Option[Array[Class[_]]] = {
    getEvalUserDefinedMethod(func, expectedTypes).map( method =>
      getParamClassesConsiderVarArgs(
        method.isVarArgs, method.getParameterTypes, expectedTypes.length))
  }

  def getEvalMethodSignature(
      func: TableFunction[_],
      expectedTypes: Array[LogicalType]): Array[Class[_]] = {
    val method = getEvalUserDefinedMethod(func, expectedTypes).getOrElse(
      throwValidationException(func.getClass.getCanonicalName, func, expectedTypes)
    )
    getParamClassesConsiderVarArgs(method.isVarArgs, method.getParameterTypes, expectedTypes.length)
  }

  def getAggUserDefinedInputTypes(
      func: UserDefinedAggregateFunction[_, _],
      externalAccType: DataType,
      expectedTypes: Array[LogicalType]): Array[DataType] = {
    val accMethod = getAggFunctionUDIMethod(
      func, "accumulate", externalAccType, expectedTypes).getOrElse(
      throwValidationException(func.getClass.getCanonicalName, func, expectedTypes)
    )
//    val udiTypes = func.getUserDefinedInputTypes(
//      getParamClassesConsiderVarArgs(
//        accMethod.isVarArgs,
//        // drop first, first must be Acc.
//        accMethod.getParameterTypes.drop(1),
//        expectedTypes.length))
    val udiTypes = getParamClassesConsiderVarArgs(
        accMethod.isVarArgs,
        // drop first, first must be Acc.
        accMethod.getParameterTypes.drop(1),
        expectedTypes.length).map(fromClassToDataType)

    udiTypes.zipWithIndex.map {
      case (t: DataType, i) =>
        // we don't trust GenericType.
        if (fromDataTypeToLogicalType(t).getTypeRoot == LogicalTypeRoot.RAW) {
          val returnType = fromLogicalTypeToDataType(expectedTypes(i))
          if (expectedTypes(i).supportsOutputConversion(t.getConversionClass)) {
            returnType.bridgedTo(t.getConversionClass)
          } else {
            returnType
          }
        } else {
          t
        }
    }
  }

  /**
    * Returns signatures of accumulate methods matching the given signature of [[LogicalType]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getAccumulateMethodSignature(
      function: UserDefinedAggregateFunction[_, _],
      expectedTypes: Seq[LogicalType])
  : Option[Array[Class[_]]] = {
    getAggFunctionUDIMethod(
      function,
      "accumulate",
      getAccumulatorTypeOfAggregateFunction(function),
      expectedTypes
    ).map(_.getParameterTypes)
  }

  def getParameterTypes(
      function: UserDefinedFunction,
      signature: Array[Class[_]]): Array[LogicalType] = {
    signature.map { c =>
      try {
        fromTypeInfoToLogicalType(TypeExtractor.getForClass(c))
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(
            s"Parameter types of function '${function.getClass.getCanonicalName}' cannot be " +
                s"automatically determined. Please provide type information manually.")
      }
    }
  }

  def getEvalUserDefinedMethod(
      function: ScalarFunction,
      expectedTypes: Seq[LogicalType])
  : Option[Method] = {
    getUserDefinedMethod(
      function,
      "eval",
      internalTypesToClasses(expectedTypes),
      expectedTypes.toArray,
      (paraClasses) => function.getParameterTypes(paraClasses).map(fromLegacyInfoToDataType))
  }

  def getEvalUserDefinedMethod(
      function: TableFunction[_],
      expectedTypes: Seq[LogicalType])
  : Option[Method] = {
    getUserDefinedMethod(
      function,
      "eval",
      internalTypesToClasses(expectedTypes),
      expectedTypes.toArray,
      (paraClasses) => function.getParameterTypes(paraClasses).map(fromLegacyInfoToDataType))
  }

  def getAggFunctionUDIMethod(
      function: UserDefinedAggregateFunction[_, _],
      methodName: String,
      accType: DataType,
      expectedTypes: Seq[LogicalType])
  : Option[Method] = {
    val input = (Array(fromDataTypeToLogicalType(accType)) ++ expectedTypes).toSeq
    getUserDefinedMethod(
      function,
      methodName,
      internalTypesToClasses(input),
      input.map{ t => if (t == null) null else t}.toArray,
      cls => Array(accType) ++ cls.drop(1).map(fromClassToDataType))
  }

  /**
    * Get method without match DateType.
    */
  def getUserDefinedMethod(
      function: UserDefinedFunction,
      methodName: String,
      signature: Seq[DataType])
  : Option[Method] = {
    getUserDefinedMethod(
      function,
      methodName,
      typesToClasses(signature),
      signature.map{ t => if (t == null) null else fromDataTypeToLogicalType(t)}.toArray,
      cls => cls.map { clazz =>
        try {
          fromClassToDataType(clazz)
        } catch {
          case _: Exception => null
        }
      })
  }

  /**
    * Returns user defined method matching the given name and signature.
    *
    * @param function                function instance
    * @param methodName              method name
    * @param methodSignature         an array of raw Java classes. We compare the raw Java classes
    *                                not the DateType. DateType does not matter during runtime (e.g.
    *                                within a MapFunction)
    * @param internalTypes           internal data types of methodSignature
    * @param parameterTypes          user provided parameter data types, usually comes from invoking
    *                                CustomTypeDefinedFunction#getParameterTypes
    * @param parameterClassEquals    function ((expect, reflected) -> Boolean) to decide if the
    *                                provided expect parameter class is equals to reflection method
    *                                signature class. The expect class comes from param
    *                                [methodSignature].
    *
    * @param parameterDataTypeEquals function ((expect, dataType) -> Boolean) to decide if the
    *                                provided expect parameter data type is equals to type in
    *                                [parameterTypes].
    */
  def getUserDefinedMethod(
      function: UserDefinedFunction,
      methodName: String,
      methodSignature: Array[Class[_]],
      internalTypes: Array[LogicalType],
      parameterTypes: Array[Class[_]] => Array[DataType],
      parameterClassEquals: (Class[_], Class[_]) => Boolean = parameterClassEquals,
      parameterDataTypeEquals: (LogicalType, DataType) =>
          Boolean = parameterDataTypeEquals)
  : Option[Method] = {

    val methods = checkAndExtractMethods(function, methodName)

    var applyCnt = 0
    val filtered = methods
        // go over all the methods and filter out matching methods
        .filter {
      case cur if !cur.isVarArgs =>
        val signatures = cur.getParameterTypes
        val dataTypes = parameterTypes(signatures)
        // match parameters of signature to actual parameters
        methodSignature.length == signatures.length &&
            signatures.zipWithIndex.forall { case (clazz, i) => {
              if (methodSignature(i) == classOf[Object]) {
                // The element of the method signature comes from the Table API's apply().
                // We can not decide the type here. It is an Unresolved Expression.
                // Actually, we do not have to decide the type here, any method of the overrides
                // which matches the arguments count will do the job.
                // So here we choose any method is correct.
                applyCnt += 1
              }
              parameterClassEquals(methodSignature(i), clazz) ||
                  parameterDataTypeEquals(internalTypes(i), dataTypes(i))
            }
            }
      case cur if cur.isVarArgs =>
        val signatures = cur.getParameterTypes
        val dataTypes = parameterTypes(signatures)
        methodSignature.zipWithIndex.forall {
          // non-varargs
          case (clazz, i) if i < signatures.length - 1  =>
            parameterClassEquals(clazz, signatures(i)) ||
                parameterDataTypeEquals(internalTypes(i), dataTypes(i))
          // varargs
          case (clazz, i) if i >= signatures.length - 1 =>
            parameterClassEquals(clazz, signatures.last.getComponentType) ||
                parameterDataTypeEquals(internalTypes(i), dataTypes(i))
        } || (methodSignature.isEmpty && signatures.length == 1) // empty varargs
    }

    // if there is a fixed method, compiler will call this method preferentially
    val fixedMethodsCount = filtered.count(!_.isVarArgs)
    val found = filtered.filter { cur =>
      fixedMethodsCount > 0 && !cur.isVarArgs ||
          fixedMethodsCount == 0 && cur.isVarArgs
    }.filter { cur =>
      // filter abstract methods
      !Modifier.isVolatile(cur.getModifiers)
    }

    // check if there is a Scala varargs annotation
    if (found.isEmpty &&
        methods.exists { method =>
          val signatures = method.getParameterTypes
          val dataTypes = parameterTypes(signatures)
          if (!method.isVarArgs && signatures.length != methodSignature.length) {
            false
          } else if (method.isVarArgs && signatures.length > methodSignature.length + 1) {
            false
          } else {
            signatures.zipWithIndex.forall {
              case (clazz, i) if i < signatures.length - 1 =>
                parameterClassEquals(methodSignature(i), clazz) ||
                    parameterDataTypeEquals(internalTypes(i), dataTypes(i))
              case (clazz, i) if i == signatures.length - 1 =>
                clazz.getName.equals("scala.collection.Seq")
            }
          }
        }) {
      throw new ValidationException(
        s"Scala-style variable arguments in '$methodName' methods are not supported. Please " +
            s"add a @scala.annotation.varargs annotation.")
    } else if (found.length > 1) {
      if (applyCnt > 0) {
        // As we can not decide type while apply() exists, so choose any one is correct
        return found.headOption
      }
      // ignore methods with Object parameter
      val nonObjectParameterMethods = found.filter { m =>
        !m.getParameterTypes.contains(classOf[Object])
      }
      if (nonObjectParameterMethods.length == 1) {
        return nonObjectParameterMethods.headOption
      }
      throw new ValidationException(
        s"Found multiple '$methodName' methods which match the signature.")
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
            s"one method named '$methodName' which is public, not abstract and " +
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
    * @param identifier function identifier
    * @param function scalar function
    * @param typeFactory type factory
    * @return the ScalarSqlFunction
    */
  def createScalarSqlFunction(
      identifier: FunctionIdentifier,
      displayName: String,
      function: ScalarFunction,
      typeFactory: FlinkTypeFactory): SqlFunction = {
    new ScalarSqlFunction(identifier, displayName, function, typeFactory)
  }

  /**
    * Create [[SqlFunction]] for a [[TableFunction]].
    *
    * Caution that the implicitResultType is only expect to be passed explicitly by Scala implicit
    * type inference.
    *
    * The entrance in BatchTableEnvironment.scala and StreamTableEnvironment.scala
    * {{{
    *   def registerFunction(name: String, tf: TableFunction[T])
    * }}}
    *
    * The implicitResultType would be inferred from type `T`.
    *
    * For all the other cases, please use
    * createTableSqlFunction (String, String, TableFunction, FlinkTypeFactory) instead.
    *
    * @param identifier function identifier
    * @param tableFunction table function
    * @param implicitResultType the implicit type information of returned table
    * @param typeFactory type factory
    * @return the TableSqlFunction
    */
  def createTableSqlFunction(
      identifier: FunctionIdentifier,
      displayName: String,
      tableFunction: TableFunction[_],
      implicitResultType: DataType,
      typeFactory: FlinkTypeFactory): TableSqlFunction = {
    // we don't know the exact result type yet.
    val function = new DeferredTypeFlinkTableFunction(tableFunction, implicitResultType)
    new TableSqlFunction(identifier, displayName, tableFunction, implicitResultType,
      typeFactory, function)
  }

  /**
    * Create [[SqlFunction]] for an [[AggregateFunction]]
    *
    * @param identifier function identifier
    * @param aggFunction aggregate function
    * @param typeFactory type factory
    * @return the TableSqlFunction
    */
  def createAggregateSqlFunction(
      identifier: FunctionIdentifier,
      displayName: String,
      aggFunction: AggregateFunction[_, _],
      externalResultType: DataType,
      externalAccType: DataType,
      typeFactory: FlinkTypeFactory)
  : SqlFunction = {
    //check if a qualified accumulate method exists before create Sql function
    checkAndExtractMethods(aggFunction, "accumulate")

    AggSqlFunction(
      identifier,
      displayName,
      aggFunction,
      externalResultType,
      externalAccType,
      typeFactory,
      aggFunction.getRequirements.contains(FunctionRequirement.OVER_WINDOW_ONLY))
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for user-defined functions
  // ----------------------------------------------------------------------------------------------

  /**
    * Tries to infer the DataType of a [[UserDefinedAggregateFunction]]'s return type.
    *
    * @param userDefinedAggregateFunction The [[UserDefinedAggregateFunction]] for which the return
    *                                     type is inferred.
    * @param extractedType                The implicitly inferred type of the result type.
    * @return The inferred result type of the [[UserDefinedAggregateFunction]].
    */
  def getResultTypeOfAggregateFunction(
      userDefinedAggregateFunction: UserDefinedAggregateFunction[_, _],
      extractedType: DataType = null): DataType = {

    val resultType = userDefinedAggregateFunction.getResultType
    if (resultType != null) {
      fromLegacyInfoToDataType(resultType)
    } else if (extractedType != null) {
      extractedType
    } else {
      try {
        extractTypeFromAggregateFunction(userDefinedAggregateFunction, 0)
      } catch {
        case ite: InvalidTypesException =>
          throw new TableException(
            "Cannot infer generic type of ${aggregateFunction.getClass}. " +
                "You can override UserDefinedAggregateFunction.getResultType() to " +
                "specify the type.",
            ite
          )
      }
    }
  }

  /**
    * Tries to infer the Type of a [[UserDefinedAggregateFunction]]'s accumulator type.
    *
    * @param userDefinedAggregateFunction The [[UserDefinedAggregateFunction]] for which the
    *                                     accumulator type is inferred.
    * @param extractedType                The implicitly inferred type of the accumulator type.
    * @return The inferred accumulator type of the [[UserDefinedAggregateFunction]].
    */
  def getAccumulatorTypeOfAggregateFunction(
      userDefinedAggregateFunction: UserDefinedAggregateFunction[_, _],
      extractedType: DataType = null): DataType = {

    val accType = userDefinedAggregateFunction.getAccumulatorType
    if (accType != null) {
      fromLegacyInfoToDataType(accType)
    } else if (extractedType != null) {
      extractedType
    } else {
      try {
        extractTypeFromAggregateFunction(userDefinedAggregateFunction, 1)
      } catch {
        case ite: InvalidTypesException =>
          throw new TableException(
            "Cannot infer generic type of ${aggregateFunction.getClass}. " +
                "You can override UserDefinedAggregateFunction.getAccumulatorType() to specify " +
                "the type.",
            ite
          )
      }
    }

  }

  /**
    * Internal method to extract a type from a [[UserDefinedAggregateFunction]]'s type parameters.
    *
    * @param aggregateFunction The [[UserDefinedAggregateFunction]] for which the type is extracted.
    * @param parameterTypePos The position of the type parameter for which the type is extracted.
    *
    * @return The extracted type.
    */
  @throws(classOf[InvalidTypesException])
  private def extractTypeFromAggregateFunction(
      aggregateFunction: UserDefinedAggregateFunction[_, _],
      parameterTypePos: Int): DataType = {

    fromLegacyInfoToDataType(TypeExtractor.createTypeInfo(
      aggregateFunction,
      classOf[UserDefinedAggregateFunction[_, _]],
      aggregateFunction.getClass,
      parameterTypePos))
  }

  def getResultTypeOfScalarFunction(
      function: ScalarFunction,
      arguments: Array[AnyRef],
      argTypes: Array[LogicalType]): DataType = {
    val userDefinedTypeInfo = function.getResultType(getEvalMethodSignature(function, argTypes))
    if (userDefinedTypeInfo != null) {
      fromLegacyInfoToDataType(userDefinedTypeInfo)
    } else {
      extractTypeFromScalarFunc(function, argTypes)
    }
  }

  private[flink] def extractTypeFromScalarFunc(
      function: ScalarFunction,
      argTypes: Array[LogicalType]): DataType = {
    try {
      fromClassToDataType(getResultTypeClassOfScalarFunction(function, argTypes))
    } catch {
      case _: InvalidTypesException =>
        throw new ValidationException(
          s"Return type of scalar function '${function.getClass.getCanonicalName}' cannot be " +
              s"automatically determined. Please provide type information manually.")
    }
  }

  /**
    * Returns the return type of the evaluation method matching the given signature.
    */
  def getResultTypeClassOfScalarFunction(
      function: ScalarFunction,
      argTypes: Array[LogicalType]): Class[_] = {
    // find method for signature
    getEvalUserDefinedMethod(function, argTypes).getOrElse(
      throw new IllegalArgumentException("Given signature is invalid.")).getReturnType
  }

  // ----------------------------------------------------------------------------------------------
  // Miscellaneous
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns field names and field positions for a given [[DataType]].
    *
    * Field names are automatically extracted for [[RowType]].
    *
    * @param inputType The DataType to extract the field names and positions from.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo(inputType: DataType)
    : (Array[String], Array[Int], Array[LogicalType]) = {
    val inputTypeInfo = fromDataTypeToTypeInfo(inputType)
    (
        FieldInfoUtils.getFieldNames(inputTypeInfo),
        FieldInfoUtils.getFieldIndices(inputTypeInfo),
        FieldInfoUtils.getFieldTypes(inputTypeInfo).map(fromTypeInfoToLogicalType))
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

  def signatureInternalToString(signature: Seq[LogicalType]): String = {
    signatureToString(internalTypesToClasses(signature))
  }

  /**
    * Prints one signature consisting of DataType.
    */
  def signatureToString(signature: Seq[DataType]): String = {
    signatureToString(typesToClasses(signature))
  }

  /**
    * Prints all signatures of methods with given name in a class.
    */
  def signaturesToString(function: UserDefinedFunction, name: String): String = {
    getMethodSignatures(function, name).map(signatureToString).mkString(", ")
  }

  /**
    * Extracts type classes of [[DataType]] in a null-aware way.
    */
  def typesToClasses(types: Seq[DataType]): Array[Class[_]] =
    types.map(t => if (t == null) null else t.getConversionClass).toArray

  def internalTypesToClasses(types: Seq[LogicalType]): Array[Class[_]] =
    types.map { t =>
      if (t == null) {
        null
      } else {
        getDefaultExternalClassForType(t)
      }
    }.toArray

  /**
    * Compares parameter candidate classes with expected classes. If true, the parameters match.
    * Candidate can be null (acts as a wildcard).
    */
  private def parameterClassEquals(candidate: Class[_], expected: Class[_]): Boolean =
    candidate == null ||
        candidate == expected ||
        expected == classOf[Object] ||
        candidate == classOf[Object]  ||  // Special case when we don't know the type
        expected.isPrimitive && Primitives.wrap(expected) == candidate ||
        candidate == classOf[Date] && (expected == classOf[Int] || expected == classOf[JInt])  ||
        candidate == classOf[Time] && (expected == classOf[Int] || expected == classOf[JInt]) ||
        candidate == classOf[BinaryString] && expected == classOf[String] ||
        candidate == classOf[String] && expected == classOf[BinaryString] ||
        candidate == classOf[SqlTimestamp] && expected == classOf[LocalDateTime] ||
        candidate == classOf[Timestamp] && expected == classOf[SqlTimestamp] ||
        candidate == classOf[SqlTimestamp] && expected == classOf[Timestamp] ||
        candidate == classOf[LocalDateTime] && expected == classOf[SqlTimestamp] ||
        candidate == classOf[SqlTimestamp] && expected == classOf[Instant] ||
        candidate == classOf[Instant] && expected == classOf[SqlTimestamp] ||
        classOf[BaseRow].isAssignableFrom(candidate) && expected == classOf[Row] ||
        candidate == classOf[Row] && classOf[BaseRow].isAssignableFrom(expected) ||
        classOf[BaseRow].isAssignableFrom(candidate) && expected == classOf[BaseRow] ||
        candidate == classOf[BaseRow] && classOf[BaseRow].isAssignableFrom(expected) ||
        candidate == classOf[Decimal] && expected == classOf[BigDecimal] ||
        candidate == classOf[BigDecimal] && expected == classOf[Decimal] ||
        (candidate.isArray &&
            expected.isArray &&
            candidate.getComponentType.isInstanceOf[Object] &&
            expected.getComponentType == classOf[Object])

  private def parameterDataTypeEquals(
      internal: LogicalType,
      parameterType: DataType): Boolean = {
    val paraInternalType = fromDataTypeToLogicalType(parameterType)
    if (isRaw(internal) && isRaw(paraInternalType)) {
      getDefaultExternalClassForType(internal) == getDefaultExternalClassForType(paraInternalType)
    } else {
      // There is a special equal to GenericType. We need rewrite type extract to BaseRow etc...
      paraInternalType == internal ||
          getInternalClassForType(internal) == getInternalClassForType(paraInternalType)
    }
  }

  def getOperandTypeArray(callBinding: SqlOperatorBinding): Array[LogicalType] = {
    getOperandType(callBinding).toArray
  }

  def getOperandType(callBinding: SqlOperatorBinding): Seq[LogicalType] = {
    val operandTypes = for (i <- 0 until callBinding.getOperandCount)
      yield callBinding.getOperandType(i)
    operandTypes.map { operandType =>
      if (operandType.getSqlTypeName == SqlTypeName.NULL) {
        null
      } else {
        FlinkTypeFactory.toLogicalType(operandType)
      }
    }
  }

  /**
    * Transform the rex nodes to Objects
    * Only literal rex nodes will be transformed, non-literal rex nodes will be
    * translated to nulls.
    *
    * @param rexNodes actual parameters of the function
    * @return A Array of the Objects
    */
  private[table] def transformRexNodes(
      rexNodes: java.util.List[RexNode]): Array[AnyRef] = {
    rexNodes.map {
      case rexNode: RexLiteral =>
        val value = rexNode.getValue2
        rexNode.getType.getSqlTypeName match {
          case SqlTypeName.INTEGER =>
            value.asInstanceOf[Long].toInt.asInstanceOf[AnyRef]
          case SqlTypeName.SMALLINT =>
            value.asInstanceOf[Long].toShort.asInstanceOf[AnyRef]
          case SqlTypeName.TINYINT =>
            value.asInstanceOf[Long].toByte.asInstanceOf[AnyRef]
          case SqlTypeName.FLOAT =>
            value.asInstanceOf[Double].toFloat.asInstanceOf[AnyRef]
          case SqlTypeName.REAL =>
            value.asInstanceOf[Double].toFloat.asInstanceOf[AnyRef]
          case _ =>
            value.asInstanceOf[AnyRef]
        }
      case _ =>
        null
    }.toArray
  }

  def buildRelDataType(
      typeFactory: RelDataTypeFactory,
      resultType: LogicalType,
      fieldNames: Array[String],
      fieldIndexes: Array[Int]): RelDataType = {

    if (fieldIndexes.length != fieldNames.length) {
      throw new TableException(
        "Number of field indexes and field names must be equal.")
    }

    // check uniqueness of field names
    if (fieldNames.length != fieldNames.toSet.size) {
      throw new TableException(
        "Table field names must be unique.")
    }

    val fieldTypes: Array[LogicalType] =
      resultType match {
        case bt: RowType =>
          if (fieldNames.length != bt.getFieldCount) {
            throw new TableException(
              s"Arity of type (" + bt.getFieldNames.toArray.deep + ") " +
                  "not equal to number of field names " + fieldNames.deep + ".")
          }
          fieldIndexes.map(i => bt.getTypeAt(i))
        case _ =>
          if (fieldIndexes.length != 1 || fieldIndexes(0) != 0) {
            throw new TableException(
              "Non-composite input type may have only a single field and its index must be 0.")
          }
          Array(resultType)
      }

    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = flinkTypeFactory.builder
    fieldNames
        .zip(fieldTypes)
        .foreach { f =>
          builder.add(f._1, flinkTypeFactory.createFieldTypeFromLogicalType(f._2))
        }
    builder.build
  }

  /**
    * Extract implicit type from table function through reflection,
    *
    * Broadly, We would consider CustomTypeDefinedFunction#getResultType first, this function
    * should always be considered as a fallback.
    *
    * @return Inferred implicit [[TypeInformation]], if [[InvalidTypesException]] throws, return
    *         GenericTypeInfo(classOf[AnyRef]) as fallback
    */
  def extractResultTypeFromTableFunction[T](tf: TableFunction[T]): TypeInformation[T] = {
    val implicitResultType = try {
      TypeExtractor.createTypeInfo(tf, classOf[TableFunction[_]], tf.getClass, 0)
    } catch {
      case _: InvalidTypesException =>
        new GenericTypeInfo(classOf[AnyRef])
    }
    implicitResultType.asInstanceOf[TypeInformation[T]]
  }
}
