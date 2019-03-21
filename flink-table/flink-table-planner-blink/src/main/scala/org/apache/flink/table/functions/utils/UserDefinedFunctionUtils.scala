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

import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils._
import org.apache.flink.table.`type`.TypeConverters.{createInternalTypeFromTypeInfo, createInternalTypeInfoFromInternalType}
import org.apache.flink.table.`type`.{InternalType, InternalTypeUtils, RowType, TypeConverters}
import org.apache.flink.table.api.{TableEnvironment, TableException, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, Decimal}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.types.Row

import com.google.common.primitives.Primitives
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlFunction, SqlOperatorBinding}

import java.lang.reflect.{Method, Modifier}
import java.lang.{Integer => JInt, Long => JLong}
import java.sql.{Date, Time, Timestamp}

import scala.collection.mutable
import scala.language.postfixOps

object UserDefinedFunctionUtils {

  // ----------------------------------------------------------------------------------------------
  // Utilities for user-defined methods
  // ----------------------------------------------------------------------------------------------

  def throwValidationException(
      name: String,
      func: UserDefinedFunction,
      parameters: Array[InternalType]): Method = {
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

  def getAggUserDefinedInputTypes(
      func: AggregateFunction[_, _],
      externalAccType: TypeInformation[_],
      expectedTypes: Array[InternalType]): Array[TypeInformation[_]] = {
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
        expectedTypes.length).map(TypeExtractor.createTypeInfo(_))

    udiTypes.zipWithIndex.map {
      case (t, i) =>
        // we don't trust GenericType.
        if (t.isInstanceOf[GenericTypeInfo[_]]) {
          TypeConverters.createExternalTypeInfoFromInternalType(expectedTypes(i))
        } else {
          t
        }
    }
  }

  /**
    * Returns signatures of accumulate methods matching the given signature of [[InternalType]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getAccumulateMethodSignature(
      function: AggregateFunction[_, _],
      expectedTypes: Seq[InternalType])
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
      signature: Array[Class[_]]): Array[InternalType] = {
    signature.map { c =>
      try {
        createInternalTypeFromTypeInfo(TypeExtractor.getForClass(c))
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
      expectedTypes: Seq[InternalType])
  : Option[Method] = {
    getUserDefinedMethod(
      function,
      "eval",
      internalTypesToClasses(expectedTypes),
      expectedTypes.toArray,
      (paraClasses) => function.getParameterTypes(paraClasses))
  }

  def getEvalUserDefinedMethod(
      function: TableFunction[_],
      expectedTypes: Seq[InternalType])
  : Option[Method] = {
    getUserDefinedMethod(
      function,
      "eval",
      internalTypesToClasses(expectedTypes),
      expectedTypes.toArray,
      (paraClasses) => function.getParameterTypes(paraClasses))
  }

  def getAggFunctionUDIMethod(
      function: AggregateFunction[_, _],
      methodName: String,
      accType: TypeInformation[_],
      expectedTypes: Seq[InternalType])
  : Option[Method] = {
    val input = (Array(createInternalTypeFromTypeInfo(accType)) ++ expectedTypes).toSeq
    getUserDefinedMethod(
      function,
      methodName,
      internalTypesToClasses(input),
      input.map{ t => if (t == null) null else t}.toArray,
//      (cls) => Array(accType) ++
//          function.getUserDefinedInputTypes(cls.drop(1)))
      (cls) => Array(accType) ++ cls.drop(1).map(TypeExtractor.createTypeInfo(_)))
  }

  /**
    * Get method without match DateType.
    */
  def getUserDefinedMethod(
      function: UserDefinedFunction,
      methodName: String,
      signature: Seq[TypeInformation[_]])
  : Option[Method] = {
    getUserDefinedMethod(
      function,
      methodName,
      typesToClasses(signature),
      signature.map{ t => if (t == null) null else createInternalTypeFromTypeInfo(t)}.toArray,
      (cls) => cls.indices.map((_) => null).toArray)
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
      internalTypes: Array[InternalType],
      parameterTypes: Array[Class[_]] => Array[TypeInformation[_]],
      parameterClassEquals: (Class[_], Class[_]) => Boolean = parameterClassEquals,
      parameterDataTypeEquals: (InternalType, TypeInformation[_]) =>
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
    * Create [[SqlFunction]] for an [[AggregateFunction]]
    *
    * @param name function name
    * @param aggFunction aggregate function
    * @param typeFactory type factory
    * @return the TableSqlFunction
    */
  def createAggregateSqlFunction(
      name: String,
      displayName: String,
      aggFunction: AggregateFunction[_, _],
      externalResultType: TypeInformation[_],
      externalAccType: TypeInformation[_],
      typeFactory: FlinkTypeFactory)
  : SqlFunction = {
    //check if a qualified accumulate method exists before create Sql function
    checkAndExtractMethods(aggFunction, "accumulate")

    AggSqlFunction(
      name,
      displayName,
      aggFunction,
      externalResultType,
      externalAccType,
      typeFactory,
      aggFunction.requiresOver)
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for user-defined functions
  // ----------------------------------------------------------------------------------------------

  /**
    * Tries to infer the DataType of an AggregateFunction's return type.
    *
    * @param aggregateFunction The AggregateFunction for which the return type is inferred.
    * @param extractedType The implicitly inferred type of the result type.
    *
    * @return The inferred result type of the AggregateFunction.
    */
  def getResultTypeOfAggregateFunction(
      aggregateFunction: AggregateFunction[_, _],
      extractedType: TypeInformation[_] = null): TypeInformation[_] = {

    val resultType = aggregateFunction.getResultType
    if (resultType != null) {
      resultType
    } else if (extractedType != null) {
      extractedType
    } else {
      try {
        extractTypeFromAggregateFunction(aggregateFunction, 0)
      } catch {
        case ite: InvalidTypesException =>
          throw new TableException(
            "Cannot infer generic type of ${aggregateFunction.getClass}. " +
                "You can override AggregateFunction.getResultType() to specify the type.",
            ite
          )
      }
    }
  }

  /**
    * Tries to infer the InternalType of an AggregateFunction's accumulator type.
    *
    * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
    * @param extractedType The implicitly inferred type of the accumulator type.
    *
    * @return The inferred accumulator type of the AggregateFunction.
    */
  def getAccumulatorTypeOfAggregateFunction(
      aggregateFunction: AggregateFunction[_, _],
      extractedType: TypeInformation[_] = null): TypeInformation[_] = {

    val accType = aggregateFunction.getAccumulatorType
    if (accType != null) {
      accType
    } else if (extractedType != null) {
      extractedType
    } else {
      try {
        extractTypeFromAggregateFunction(aggregateFunction, 1)
      } catch {
        case ite: InvalidTypesException =>
          throw new TableException(
            "Cannot infer generic type of ${aggregateFunction.getClass}. " +
                "You can override AggregateFunction.getAccumulatorType() to specify the type.",
            ite
          )
      }
    }
  }

  /**
    * Internal method to extract a type from an AggregateFunction's type parameters.
    *
    * @param aggregateFunction The AggregateFunction for which the type is extracted.
    * @param parameterTypePos The position of the type parameter for which the type is extracted.
    *
    * @return The extracted type.
    */
  @throws(classOf[InvalidTypesException])
  private def extractTypeFromAggregateFunction(
      aggregateFunction: AggregateFunction[_, _],
      parameterTypePos: Int): TypeInformation[_] = {

    TypeExtractor.createTypeInfo(
      aggregateFunction,
      classOf[AggregateFunction[_, _]],
      aggregateFunction.getClass,
      parameterTypePos)
  }

  // ----------------------------------------------------------------------------------------------
  // Miscellaneous
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns field names and field positions for a given [[TypeInformation[_]]].
    *
    * Field names are automatically extracted for [[RowType]].
    *
    * @param inputType The DataType to extract the field names and positions from.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo(inputType: TypeInformation[_])
  : (Array[String], Array[Int], Array[InternalType]) = {
    (
        TableEnvironment.getFieldNames(inputType),
        TableEnvironment.getFieldIndices(inputType),
        TableEnvironment.getFieldTypes(inputType).map(createInternalTypeFromTypeInfo))
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

  def signatureInternalToString(signature: Seq[InternalType]): String = {
    signatureToString(internalTypesToClasses(signature))
  }

  /**
    * Prints one signature consisting of DataType.
    */
  def signatureToString(signature: Seq[TypeInformation[_]]): String = {
    signatureToString(typesToClasses(signature))
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
  def typesToClasses(types: Seq[TypeInformation[_]]): Array[Class[_]] =
    types.map { t =>
      if (t == null) {
        null
      } else {
        t.getTypeClass
      }
    }.toArray

  def internalTypesToClasses(types: Seq[InternalType]): Array[Class[_]] =
    types.map { t =>
      if (t == null) {
        null
      } else {
        InternalTypeUtils.getExternalClassForType(t)
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
        candidate == classOf[Timestamp] && (expected == classOf[Long] ||
            expected == classOf[JLong]) ||
        candidate == classOf[BinaryString] && expected == classOf[String] ||
        candidate == classOf[String] && expected == classOf[BinaryString] ||
        classOf[BaseRow].isAssignableFrom(candidate) && expected == classOf[Row] ||
        candidate == classOf[Row] && classOf[BaseRow].isAssignableFrom(expected) ||
        candidate == classOf[Decimal] && expected == classOf[BigDecimal] ||
        candidate == classOf[BigDecimal] && expected == classOf[Decimal] ||
        (candidate.isArray &&
            expected.isArray &&
            candidate.getComponentType.isInstanceOf[Object] &&
            expected.getComponentType == classOf[Object])

  private def parameterDataTypeEquals(
      internal: InternalType,
      parameterType: TypeInformation[_]): Boolean = {
    val paraInternalType = createInternalTypeFromTypeInfo(parameterType)
    // There is a special equal to GenericType. We need rewrite type extract to BaseRow etc...
    paraInternalType == internal ||
        createInternalTypeInfoFromInternalType(internal).getTypeClass ==
            createInternalTypeInfoFromInternalType(paraInternalType).getTypeClass
  }

  def getOperandType(callBinding: SqlOperatorBinding): Seq[InternalType] = {
    val operandTypes = for (i <- 0 until callBinding.getOperandCount)
      yield callBinding.getOperandType(i)
    operandTypes.map { operandType =>
      if (operandType.getSqlTypeName == SqlTypeName.NULL) {
        null
      } else {
        FlinkTypeFactory.toInternalType(operandType)
      }
    }
  }
}
