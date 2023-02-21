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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.functions.{BuiltInFunctionDefinitions, FunctionIdentifier, TableFunction, UserDefinedFunctionHelper}
import org.apache.flink.table.functions.UserDefinedFunctionHelper.generateInlineFunctionName
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.utils.TableSqlFunction._
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.planner.plan.schema.FlinkTableFunction
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction

import java.lang.reflect.Method
import java.util

/**
 * Calcite wrapper for user-defined table functions.
 *
 * @param identifier
 *   function identifier to uniquely identify this function
 * @param udtf
 *   user-defined table function to be called
 * @param implicitResultType
 *   Implicit result type information
 * @param typeFactory
 *   type factory for converting Flink's between Calcite's types
 * @param functionImpl
 *   Calcite table function schema
 * @return
 *   [[TableSqlFunction]]
 * @deprecated
 *   Use [[BuiltInFunctionDefinitions]] that translates to [[BridgingSqlFunction]].
 */
@Deprecated
@deprecated
class TableSqlFunction(
    identifier: FunctionIdentifier,
    displayName: String,
    val udtf: TableFunction[_],
    implicitResultType: DataType,
    typeFactory: FlinkTypeFactory,
    functionImpl: FlinkTableFunction,
    operandMetadata: Option[SqlOperandMetadata] = None)
  extends SqlUserDefinedTableFunction(
    Option(identifier)
      .map(id => new SqlIdentifier(id.toList, SqlParserPos.ZERO))
      .getOrElse(new SqlIdentifier(generateInlineFunctionName(udtf), SqlParserPos.ZERO)),
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.CURSOR,
    // type inference has the UNKNOWN operand types.
    createOperandTypeInference(displayName, udtf, typeFactory),
    // only checker has the real operand types.
    operandMetadata.getOrElse(createOperandMetadata(displayName, udtf)),
    functionImpl) {

  /**
   * This is temporary solution for hive udf and should be removed once FLIP-65 is finished, please
   * pass the non-null input arguments.
   */
  def makeFunction(constants: Array[AnyRef], argTypes: Array[LogicalType]): TableFunction[_] =
    udtf

  /** Get the type information of the table returned by the table function. */
  def getImplicitResultType: DataType = implicitResultType

  override def isDeterministic: Boolean = udtf.isDeterministic

  override def toString: String = displayName

  override def getRowTypeInference: SqlReturnTypeInference = new SqlReturnTypeInference {
    override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
      val arguments = convertArguments(opBinding, functionImpl, getNameAsId)
      getRowType(opBinding.getTypeFactory, arguments)
    }
  }

  def getRowType(typeFactory: RelDataTypeFactory, arguments: util.List[Object]): RelDataType = {
    functionImpl.getRowType(typeFactory)
  }
}

object TableSqlFunction {

  private[flink] def createOperandTypeInference(
      name: String,
      udtf: TableFunction[_],
      typeFactory: FlinkTypeFactory): SqlOperandTypeInference = {

    /** Operand type inference based on [[TableFunction]] given information. */
    new SqlOperandTypeInference {
      override def inferOperandTypes(
          callBinding: SqlCallBinding,
          returnType: RelDataType,
          operandTypes: Array[RelDataType]): Unit = {
        inferOperandTypesInternal(name, udtf, typeFactory, callBinding, returnType, operandTypes)
      }
    }
  }

  def inferOperandTypesInternal(
      name: String,
      func: TableFunction[_],
      typeFactory: FlinkTypeFactory,
      callBinding: SqlCallBinding,
      returnType: RelDataType,
      operandTypes: Array[RelDataType]): Unit = {
    val parameters = getOperandType(callBinding).toArray
    if (getEvalUserDefinedMethod(func, parameters).isEmpty) {
      throwValidationException(name, func, parameters)
    }
    func
      .getParameterTypes(getEvalMethodSignature(func, parameters))
      .map(fromTypeInfoToLogicalType)
      .map(typeFactory.createFieldTypeFromLogicalType)
      .zipWithIndex
      .foreach { case (t, i) => operandTypes(i) = t }
  }

  private[flink] def createOperandMetadata(
      name: String,
      udtf: TableFunction[_]): SqlOperandMetadata = {
    new OperandMetadata(name, udtf, checkAndExtractMethods(udtf, "eval"))
  }

  /**
   * Converts arguments from [[org.apache.calcite.sql.SqlNode]] to java object format.
   *
   * @param callBinding
   *   Operator bound to arguments
   * @param function
   *   target function to get parameter types from
   * @param opName
   *   name of the operator to use in error message
   * @return
   *   converted list of arguments
   */
  private[flink] def convertArguments(
      callBinding: SqlOperatorBinding,
      function: org.apache.calcite.schema.Function,
      opName: SqlIdentifier): util.List[Object] = {
    val arguments = new util.ArrayList[Object](callBinding.getOperandCount)
    (0 until callBinding.getOperandCount).foreach {
      i =>
        val value: Object = if (callBinding.isOperandLiteral(i, true)) {
          callBinding.getOperandLiteralValue(i, classOf[Object])
        } else {
          null
        }
        arguments.add(value);
    }
    arguments
  }
}

/** Operand type checker based on [[TableFunction]] given information. */
class OperandMetadata(name: String, udtf: TableFunction[_], methods: Array[Method])
  extends SqlOperandMetadata {

  override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
    s"$opName[${signaturesToString(udtf, "eval")}]"
  }

  override def getOperandCountRange: SqlOperandCountRange = {
    var min = 254
    var max = -1
    var isVarargs = false
    methods.foreach(
      m => {
        var len = m.getParameterCount
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

  override def checkOperandTypes(callBinding: SqlCallBinding, throwOnFailure: Boolean): Boolean = {
    val operandTypes = getOperandType(callBinding)

    if (getEvalUserDefinedMethod(udtf, operandTypes).isEmpty) {
      if (throwOnFailure) {
        throw new ValidationException(
          s"Given parameters of function '$name' do not match any signature. \n" +
            s"Actual: ${signatureInternalToString(operandTypes)} \n" +
            s"Expected: ${signaturesToString(udtf, "eval")}")
      } else {
        false
      }
    } else {
      true
    }
  }

  override def isOptional(i: Int): Boolean = false

  override def getConsistency: Consistency = Consistency.NONE

  override def paramTypes(typeFactory: RelDataTypeFactory): util.List[RelDataType] =
    throw new UnsupportedOperationException(
      "SqlOperandMetadata.paramTypes "
        + "should never be invoked")

  override def paramNames(): util.List[String] =
    throw new UnsupportedOperationException(
      "SqlOperandMetadata.paramNames "
        + "should never be invoked")
}
