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

import java.lang.reflect.{Method, Modifier}
import java.lang.{Integer => JInt, Long => JLong}
import java.sql.{Date, Time, Timestamp}
import java.util

import com.google.common.primitives.Primitives
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.{SqlCallBinding, SqlFunction, SqlOperandCountRange, SqlOperator}
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoField, PojoTypeInfo, TypeExtractor}
import org.apache.flink.table.api.dataview._
import org.apache.flink.table.api.{TableEnvironment, TableException, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataview._
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.util.InstantiationUtil

import scala.collection.mutable

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
        s"The class ${clazz.getCanonicalName} is an inner class, but not statically accessible.")
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
    * Returns the signature of the eval method matching the given signature of [[TypeInformation]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getEvalMethodSignature(
      function: UserDefinedFunction,
      signature: Seq[TypeInformation[_]])
    : Option[Array[Class[_]]] = {

    getUserDefinedMethod(function, "eval", typeInfoToClass(signature)).map(_.getParameterTypes)
  }

  /**
    * Returns the signature of the accumulate method matching the given signature
    * of [[TypeInformation]]. Elements of the signature can be null (act as a wildcard).
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
        s"Scala-style variable arguments in '$methodName' methods are not supported. Please " +
          s"add a @scala.annotation.varargs annotation.")
    } else if (found.length > 1) {
      throw new ValidationException(
        s"Found multiple '$methodName' methods which match the signature.")
    }
    found.headOption
  }

  /**
    * Checks if a given method exists in the given function
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
    * Creates [[SqlFunction]] for a [[ScalarFunction]]
    *
    * @param name function name
    * @param function scalar function
    * @param typeFactory type factory
    * @return the ScalarSqlFunction
    */
  def createScalarSqlFunction(
      name: String,
      displayName: String,
      function: ScalarFunction,
      typeFactory: FlinkTypeFactory)
    : SqlFunction = {
    new ScalarSqlFunction(name, displayName, function, typeFactory)
  }

  /**
    * Creates [[SqlFunction]] for a [[TableFunction]]
    *
    * @param name function name
    * @param tableFunction table function
    * @param resultType the type information of returned table
    * @param typeFactory type factory
    * @return the TableSqlFunction
    */
  def createTableSqlFunction(
      name: String,
      displayName: String,
      tableFunction: TableFunction[_],
      resultType: TypeInformation[_],
      typeFactory: FlinkTypeFactory)
    : SqlFunction = {
    val (fieldNames, fieldIndexes, _) = UserDefinedFunctionUtils.getFieldInfo(resultType)
    val function = new FlinkTableFunctionImpl(resultType, fieldIndexes, fieldNames)
    new TableSqlFunction(name, displayName, tableFunction, resultType, typeFactory, function)
  }

  /**
    * Creates [[SqlFunction]] for an [[AggregateFunction]]
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
      resultType: TypeInformation[_],
      accTypeInfo: TypeInformation[_],
      typeFactory: FlinkTypeFactory)
  : SqlFunction = {
    //check if a qualified accumulate method exists before create Sql function
    checkAndExtractMethods(aggFunction, "accumulate")

    AggSqlFunction(
      name,
      displayName,
      aggFunction,
      resultType,
      accTypeInfo,
      typeFactory,
      aggFunction.requiresOver)
  }

  /**
    * Creates a [[SqlOperandTypeChecker]] for SQL validation of
    * eval functions (scalar and table functions).
    */
  def createEvalOperandTypeChecker(
      name: String,
      function: UserDefinedFunction)
    : SqlOperandTypeChecker = {

    val methods = checkAndExtractMethods(function, "eval")

    new SqlOperandTypeChecker {
      override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
        s"$opName[${signaturesToString(function, "eval")}]"
      }

      override def getOperandCountRange: SqlOperandCountRange = {
        var min = 254
        var max = -1
        var isVarargs = false
        methods.foreach( m => {
          var len = m.getParameterTypes.length
          if (len > 0 && m.isVarArgs && m.getParameterTypes()(len - 1).isArray) {
            isVarargs = true
            len = len - 1
          }
          max = Math.max(len, max)
          min = Math.min(len, min)
        })
        if (isVarargs) {
          // if eval method is varargs, set max to -1 to skip length check in Calcite
          max = -1
        }

        SqlOperandCountRanges.between(min, max)
      }

      override def checkOperandTypes(
          callBinding: SqlCallBinding,
          throwOnFailure: Boolean)
        : Boolean = {
        val operandTypeInfo = getOperandTypeInfo(callBinding)

        val foundSignature = getEvalMethodSignature(function, operandTypeInfo)

        if (foundSignature.isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypeInfo)} \n" +
                s"Expected: ${signaturesToString(function, "eval")}")
          } else {
            false
          }
        } else {
          true
        }
      }

      override def isOptional(i: Int): Boolean = false

      override def getConsistency: Consistency = Consistency.NONE

    }
  }

  /**
    * Creates a [[SqlOperandTypeInference]] for the SQL validation of eval functions
    * (scalar and table functions).
    */
  def createEvalOperandTypeInference(
    name: String,
    function: UserDefinedFunction,
    typeFactory: FlinkTypeFactory)
  : SqlOperandTypeInference = {

    new SqlOperandTypeInference {
      override def inferOperandTypes(
          callBinding: SqlCallBinding,
          returnType: RelDataType,
          operandTypes: Array[RelDataType]): Unit = {

        val operandTypeInfo = getOperandTypeInfo(callBinding)

        val foundSignature = getEvalMethodSignature(function, operandTypeInfo)
          .getOrElse(throw new ValidationException(
            s"Given parameters of function '$name' do not match any signature. \n" +
              s"Actual: ${signatureToString(operandTypeInfo)} \n" +
              s"Expected: ${signaturesToString(function, "eval")}"))

        val inferredTypes = function match {
          case sf: ScalarFunction =>
            sf.getParameterTypes(foundSignature)
              .map(typeFactory.createTypeFromTypeInfo(_, isNullable = true))
          case tf: TableFunction[_] =>
            tf.getParameterTypes(foundSignature)
              .map(typeFactory.createTypeFromTypeInfo(_, isNullable = true))
          case _ => throw new TableException("Unsupported function.")
        }

        for (i <- operandTypes.indices) {
          if (i < inferredTypes.length - 1) {
            operandTypes(i) = inferredTypes(i)
          } else if (null != inferredTypes.last.getComponentType) {
            // last argument is a collection, the array type
            operandTypes(i) = inferredTypes.last.getComponentType
          } else {
            operandTypes(i) = inferredTypes.last
          }
        }
      }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for user-defined functions
  // ----------------------------------------------------------------------------------------------

  /**
    * Remove StateView fields from accumulator type information.
    *
    * @param index index of aggregate function
    * @param aggFun aggregate function
    * @param accType accumulator type information, only support pojo type
    * @param isStateBackedDataViews is data views use state backend
    * @return mapping of accumulator type information and data view config which contains id,
    *         field name and state descriptor
    */
  def removeStateViewFieldsFromAccTypeInfo(
      index: Int,
      aggFun: AggregateFunction[_, _],
      accType: TypeInformation[_],
      isStateBackedDataViews: Boolean)
    : (TypeInformation[_], Option[Seq[DataViewSpec[_]]]) = {

    /** Recursively checks if composite type includes a data view type. */
    def includesDataView(ct: CompositeType[_]): Boolean = {
      (0 until ct.getArity).exists(i =>
        ct.getTypeAt(i) match {
          case nestedCT: CompositeType[_] => includesDataView(nestedCT)
          case t: TypeInformation[_] if t.getTypeClass == classOf[ListView[_]] => true
          case t: TypeInformation[_] if t.getTypeClass == classOf[MapView[_, _]] => true
          case _ => false
        }
      )
    }

    val acc = aggFun.createAccumulator()
    accType match {
      case pojoType: PojoTypeInfo[_] if pojoType.getArity > 0 =>
        val arity = pojoType.getArity
        val newPojoFields = new util.ArrayList[PojoField]()
        val accumulatorSpecs = new mutable.ArrayBuffer[DataViewSpec[_]]
        for (i <- 0 until arity) {
          val pojoField = pojoType.getPojoFieldAt(i)
          val field = pojoField.getField
          val fieldName = field.getName
          field.setAccessible(true)

          pojoField.getTypeInformation match {
            case ct: CompositeType[_] if includesDataView(ct) =>
              throw new TableException(
                "MapView and ListView only supported at first level of accumulators of Pojo type.")
            case map: MapViewTypeInfo[_, _] =>
              val mapView = field.get(acc).asInstanceOf[MapView[_, _]]
              if (mapView != null) {
                val keyTypeInfo = mapView.keyTypeInfo
                val valueTypeInfo = mapView.valueTypeInfo
                val newTypeInfo = if (keyTypeInfo != null && valueTypeInfo != null) {
                  new MapViewTypeInfo(keyTypeInfo, valueTypeInfo)
                } else {
                  map
                }

                // create map view specs with unique id (used as state name)
                var spec = MapViewSpec(
                  "agg" + index + "$" + fieldName,
                  field,
                  newTypeInfo)

                accumulatorSpecs += spec
                if (!isStateBackedDataViews) {
                  // add data view field if it is not backed by a state backend.
                  // data view fields which are backed by state backend are not serialized.
                  newPojoFields.add(new PojoField(field, newTypeInfo))
                }
              }

            case list: ListViewTypeInfo[_] =>
              val listView = field.get(acc).asInstanceOf[ListView[_]]
              if (listView != null) {
                val elementTypeInfo = listView.elementTypeInfo
                val newTypeInfo = if (elementTypeInfo != null) {
                  new ListViewTypeInfo(elementTypeInfo)
                } else {
                  list
                }

                // create list view specs with unique is (used as state name)
                var spec = ListViewSpec(
                  "agg" + index + "$" + fieldName,
                  field,
                  newTypeInfo)

                accumulatorSpecs += spec
                if (!isStateBackedDataViews) {
                  // add data view field if it is not backed by a state backend.
                  // data view fields which are backed by state backend are not serialized.
                  newPojoFields.add(new PojoField(field, newTypeInfo))
                }
              }

            case _ => newPojoFields.add(pojoField)
          }
        }
        (new PojoTypeInfo(accType.getTypeClass, newPojoFields), Some(accumulatorSpecs))
      case ct: CompositeType[_] if includesDataView(ct) =>
        throw new TableException(
          "MapView and ListView only supported in accumulators of POJO type.")
      case _ => (accType, None)
    }
  }

  /**
    * Tries to infer the TypeInformation of an AggregateFunction's return type.
    *
    * @param aggregateFunction The AggregateFunction for which the return type is inferred.
    * @param extractedType The implicitly inferred type of the result type.
    *
    * @return The inferred result type of the AggregateFunction.
    */
  def getResultTypeOfAggregateFunction(
      aggregateFunction: AggregateFunction[_, _],
      extractedType: TypeInformation[_] = null)
    : TypeInformation[_] = {

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
    * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
    *
    * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
    * @param extractedType The implicitly inferred type of the accumulator type.
    *
    * @return The inferred accumulator type of the AggregateFunction.
    */
  def getAccumulatorTypeOfAggregateFunction(
    aggregateFunction: AggregateFunction[_, _],
    extractedType: TypeInformation[_] = null)
  : TypeInformation[_] = {

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
      parameterTypePos).asInstanceOf[TypeInformation[_]]
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
    // time types
    candidate == classOf[Date] && (expected == classOf[Int] || expected == classOf[JInt])  ||
    candidate == classOf[Time] && (expected == classOf[Int] || expected == classOf[JInt]) ||
    candidate == classOf[Timestamp] && (expected == classOf[Long] || expected == classOf[JLong]) ||
    // arrays
    (candidate.isArray && expected.isArray &&
      (candidate.getComponentType == expected.getComponentType ||
        expected.getComponentType == classOf[Object]))

  /**
    * Creates a [[LogicalTableFunctionCall]] by parsing a String expression.
    *
    * @param tableEnv The table environment to lookup the function.
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
