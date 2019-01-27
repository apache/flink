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
import com.google.common.primitives.Primitives
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlFunction, SqlOperatorBinding}
import org.apache.commons.codec.binary.Base64
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils._
import org.apache.flink.table.api.functions._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, TableException, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.schema.DeferredTypeFlinkTableFunction
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, Decimal}
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.hive.functions._
import org.apache.flink.table.api.types._
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.types.Row
import org.apache.flink.util.InstantiationUtil

import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.udf.generic.{GenericUDAFResolver2, GenericUDF, GenericUDTF}

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
      parameters: Array[InternalType]): Method = {
    throw new ValidationException(
      s"Given parameters of function '$name' do not match any signature. \n" +
          s"Actual: ${signatureToString(parameters)} \n" +
          s"Expected: ${signaturesToString(func, "eval")}")
  }

  private def getParamClassesConsiderVarArgs(
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
      func: CustomTypeDefinedFunction,
      expectedTypes: Array[InternalType]): Array[Class[_]] = {
    val method = getEvalUserDefinedMethod(func, expectedTypes).getOrElse(
      throwValidationException(func.getClass.getCanonicalName, func, expectedTypes)
    )
    getParamClassesConsiderVarArgs(method.isVarArgs, method.getParameterTypes, expectedTypes.length)
  }

  def getAggUserDefinedInputTypes(
      func: AggregateFunction[_, _],
      externalAccType: DataType,
      expectedTypes: Array[InternalType]): Array[DataType] = {
    val accMethod = getAggFunctionUDIMethod(
      func, "accumulate", externalAccType, expectedTypes).getOrElse(
      throwValidationException(func.getClass.getCanonicalName, func, expectedTypes)
    )
    func.getUserDefinedInputTypes(
      getParamClassesConsiderVarArgs(
        accMethod.isVarArgs,
        // drop first, first must be Acc.
        accMethod.getParameterTypes.drop(1),
        expectedTypes.length)).zipWithIndex.map {
      case (t, i) =>
        // we don't trust GenericType.
        if (TypeUtils.isGeneric(t)) expectedTypes(i) else t
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
        TypeExtractor.getForClass(c).toInternalType
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(
            s"Parameter types of function '${function.getClass.getCanonicalName}' cannot be " +
              s"automatically determined. Please provide type information manually.")
      }
    }
  }

  def getEvalUserDefinedMethod(
      function: CustomTypeDefinedFunction,
      expectedTypes: Seq[InternalType])
      : Option[Method] = {
    getUserDefinedMethod(
      function,
      "eval",
      typesToClasses(expectedTypes),
      expectedTypes.toArray,
      (paraClasses) => function.getParameterTypes(paraClasses))
  }

  def getAggFunctionUDIMethod(
      function: AggregateFunction[_, _],
      methodName: String,
      accType: DataType,
      expectedTypes: Seq[InternalType])
      : Option[Method] = {
    val input = (Array(accType) ++ expectedTypes).toSeq
    getUserDefinedMethod(
      function,
      methodName,
      typesToClasses(input),
      input.map(_.toInternalType).toArray,
      (cls) => Array(accType) ++
          function.getUserDefinedInputTypes(cls.drop(1)))
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
      signature.map(_.toInternalType).toArray,
      (cls) => cls.indices.map((_) => null).toArray)
  }

  /**
    * Returns user defined method matching the given name and signature.
    *
    * @param function        function instance
    * @param methodName      method name
    * @param methodSignature an array of raw Java classes. We compare the raw Java classes not the
    *                        DateType. DateType does not matter during runtime (e.g.
    *                        within a MapFunction)
    */
  def getUserDefinedMethod(
      function: UserDefinedFunction,
      methodName: String,
      methodSignature: Array[Class[_]],
      internalTypes: Array[InternalType],
      parameterTypes: (Array[Class[_]] => Array[DataType]))
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
              parameterTypeEquals(methodSignature(i), clazz) ||
                  parameterDataTypeEquals(internalTypes(i), dataTypes(i))
            }
          }
        case cur if cur.isVarArgs =>
          val signatures = cur.getParameterTypes
          val dataTypes = parameterTypes(signatures)
          methodSignature.zipWithIndex.forall {
            // non-varargs
            case (clazz, i) if i < signatures.length - 1  =>
              parameterTypeEquals(clazz, signatures(i)) ||
                  parameterDataTypeEquals(internalTypes(i), dataTypes(i))
            // varargs
            case (clazz, i) if i >= signatures.length - 1 =>
              parameterTypeEquals(clazz, signatures.last.getComponentType) ||
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
              parameterTypeEquals(methodSignature(i), clazz) ||
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
    * Create [[SqlFunction]] for a [[ScalarFunction]]
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
    * Create [[SqlFunction]] for a [[TableFunction]]
    *
    * @param name function name
    * @param tableFunction table function
    * @param implicitResultType the implicit type information of returned table
    * @param typeFactory type factory
    * @return the TableSqlFunction
    */
  def createTableSqlFunction(
      name: String,
      displayName: String,
      tableFunction: TableFunction[_],
      implicitResultType: DataType,
      typeFactory: FlinkTypeFactory)
    : TableSqlFunction = {
    // we don't know the exact result type yet.
    val function = new DeferredTypeFlinkTableFunction(tableFunction, implicitResultType)
    new TableSqlFunction(name, displayName, tableFunction, implicitResultType,
      typeFactory, function)
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
      displayName: String,
      aggFunction: AggregateFunction[_, _],
      externalResultType: DataType,
      externalAccType: DataType,
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
      extractedType: DataType = null)
    : DataType = {

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
    extractedType: DataType = null): DataType = {

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
      parameterTypePos: Int): DataType = {

    new TypeInfoWrappedDataType(TypeExtractor.createTypeInfo(
      aggregateFunction,
      classOf[AggregateFunction[_, _]],
      aggregateFunction.getClass,
      parameterTypePos))
  }

  def getResultTypeOfScalarFunction(
      function: ScalarFunction,
      arguments: Array[AnyRef],
      argTypes: Array[InternalType]): DataType = {
    val userDefinedTypeInfo = function.getResultType(
      arguments, getEvalMethodSignature(function, argTypes))
    if (userDefinedTypeInfo != null) {
      userDefinedTypeInfo
    } else {
      extractTypeFromScalarFunc(function, argTypes)
    }
  }

  private[flink] def extractTypeFromScalarFunc(
      function: ScalarFunction,
      argTypes: Array[InternalType]): DataType = {
    try {TypeExtractor.getForClass(
        getResultTypeClassOfScalarFunction(function, argTypes))
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
      argTypes: Array[InternalType]): Class[_] = {
    // find method for signature
    getEvalUserDefinedMethod(function, argTypes).getOrElse(
      throw new IllegalArgumentException("Given signature is invalid.")).getReturnType
  }

  /**
    * Returns the return type of the evaluation method matching the given signature.
    */
  def getResultTypeClassOfPythonScalarFunction(returnType: InternalType): Class[_] = {
    returnType match {
      case _: StringType => classOf[org.apache.flink.table.dataformat.BinaryString]
      case _: BooleanType => classOf[java.lang.Boolean]
      case _: ByteType => classOf[java.lang.Byte]
      case _: ShortType => classOf[java.lang.Short]
      case _: IntType => classOf[java.lang.Integer]
      case _: LongType => classOf[java.lang.Long]
      case _: FloatType => classOf[java.lang.Float]
      case _: DoubleType => classOf[java.lang.Double]
      case _: DecimalType => classOf[java.math.BigDecimal]
      case _: DateType => classOf[java.lang.Integer]
      case _: TimeType => classOf[java.lang.Integer]
      case _: TimestampType => classOf[java.lang.Long]
      case _: ByteArrayType => classOf[Array[Byte]]
    }
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
    : (Array[String], Array[Int], Array[InternalType]) = {
    (
        TableEnvironment.getFieldNames(inputType),
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
    types.map { t =>
    if (t == null) {
      null
    } else {
      TypeUtils.getExternalClassForType(t)
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
    candidate == classOf[Object]  ||  // Special case when we don't know the type
    expected.isPrimitive && Primitives.wrap(expected) == candidate ||
    candidate == classOf[Date] && (expected == classOf[Int] || expected == classOf[JInt])  ||
    candidate == classOf[Time] && (expected == classOf[Int] || expected == classOf[JInt]) ||
    candidate == classOf[Timestamp] && (expected == classOf[Long] || expected == classOf[JLong]) ||
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

  private def parameterDataTypeEquals(internal: InternalType, parameterType: DataType): Boolean =
    // There is a special equal to GenericType. We need rewrite type extract to BaseRow etc...
    parameterType.toInternalType == internal || internal.getTypeClass == parameterType.getTypeClass

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
        val function = tableEnv.chainedFunctionCatalog.lookupFunction(name, args)
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

  private[table] def getResultTypeOfCTDFunction(
      func: CustomTypeDefinedFunction,
      params: Array[Expression],
      getImplicitResultType: () => DataType): DataType = {
    val arguments = params.map {
      case exp: Literal =>
        exp.value.asInstanceOf[AnyRef]
      case _ =>
        null
    }
    val signature = params.map { param =>
      if (param.valid) param.resultType
      else new GenericType(new Object().getClass)  // apply() can not decide type
    }
    val argTypes = getEvalMethodSignature(func, signature)
    val udt = func.getResultType(arguments, argTypes)
    if (udt != null) udt else getImplicitResultType()
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

  private[table] def buildRelDataType(
      typeFactory: RelDataTypeFactory,
      resultType: InternalType,
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

    val fieldTypes: Array[InternalType] =
      resultType match {
        case bt: RowType =>
          if (fieldNames.length != bt.getArity) {
            throw new TableException(
              s"Arity of type (" + bt.getFieldNames.deep + ") " +
                  "not equal to number of field names " + fieldNames.deep + ".")
          }
          fieldIndexes.map(i => bt.getInternalTypeAt(i).toInternalType)
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
          builder.add(f._1, flinkTypeFactory.createTypeFromInternalType(f._2, isNullable = true))
        }
    builder.build
  }

  def createUserDefinedFunction(
      classLoader: ClassLoader,
      funcName: String,
      functionDef: String): Any = {

    var func: Any = Nil
    val javaClass = functionDef.contains(".")

    if (javaClass) {
      try {
        func = classLoader.loadClass(functionDef).newInstance()
      } catch {
        case e: Exception =>
          throw new RuntimeException(
            TableErrors.INST.sqlCreateUserDefinedFuncError(
              funcName,
              functionDef,
              e.getClass.getCanonicalName + " : " + e.getMessage),
            e)
      }
    } else {
      try {
        // try deserialize first
        func = InstantiationUtil
          .deserializeObject[Object](Base64.decodeBase64(hexString2String(functionDef)),
          classLoader)
      } catch {
        case e: Exception =>
          try {
            // It might be a java class without package name
            func = classLoader.loadClass(functionDef).newInstance()
          } catch {
            case e: Exception =>
              throw new RuntimeException(
                TableErrors.INST.sqlCreateUserDefinedFuncError(
                  funcName,
                  functionDef,
                  e.getClass.getCanonicalName + " : " + e.getMessage),
                e)
          }
      }
    }

    // Check hive and covert to flink udf
    func match {
      case _: UDF =>
        func = new HiveSimpleUDF(new HiveFunctionWrapper[UDF](functionDef))
      case _: GenericUDF =>
        func = new HiveGenericUDF(new HiveFunctionWrapper[GenericUDF](functionDef))
      case _: UDAF =>
        func = new HiveUDAFFunction(new HiveFunctionWrapper[UDAF](functionDef))
      case _: GenericUDAFResolver2 =>
        func = new HiveUDAFFunction(new HiveFunctionWrapper[GenericUDAFResolver2](functionDef))
      case _: GenericUDTF =>
        func = new HiveGenericUDTF(new HiveFunctionWrapper[GenericUDTF](functionDef))
      case _ =>
    }
    func
  }

  def getImplicitResultType[T](tf: TableFunction[T]) = {
    val implicitResultType = try {
      TypeExtractor.createTypeInfo(tf, classOf[TableFunction[_]], tf.getClass, 0)
    } catch {
      case e: InvalidTypesException =>
        // may be we should get type from getResultType
        new GenericType(classOf[AnyRef])
    }
    implicitResultType
  }

  /**
    * Get implicit type from table function through reflection, We will consider getResultType first
    * then this, see [[getResultTypeIgnoreException]] for details.
    * @return Inferred implicit [[TypeInformation]], if [[InvalidTypesException]] throws, return
    *         GenericTypeInfo(classOf[AnyRef]) as fallback
    */
  def getImplicitResultTypeInfo[T](tf: TableFunction[T]): TypeInformation[T] = {
    val implicitResultType = try {
      TypeExtractor.createTypeInfo(tf, classOf[TableFunction[_]], tf.getClass, 0)
    } catch {
      case _: InvalidTypesException =>
        new GenericTypeInfo(classOf[AnyRef])
    }
    implicitResultType.asInstanceOf[TypeInformation[T]]
  }

  /**
    * Get result type from [[TableFunction]] while sans any exception.
    * @return null if any exception throws
    */
  def getResultTypeIgnoreException[T](tf: TableFunction[T],
    arguments: Array[AnyRef] = null,
    argTypes: Array[Class[_]] = null): DataType = {
    try {
      tf.getResultType(arguments, argTypes)
    } catch {
      case _: Exception =>
        null
    }
  }

  private def hexString2String(str: String): String = {
    str.sliding(2, 2).map(s =>
      Integer.parseInt(s, 16).asInstanceOf[Char].toString).mkString("")
  }
}
