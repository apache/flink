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

import java.sql.Timestamp
import java.util

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeFactoryImpl}
import org.apache.calcite.schema.impl.ReflectiveFunctionBase
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.{SqlUserDefinedTableFunction, SqlUserDefinedTableMacro}
import org.apache.calcite.util.{NlsString, TimestampString}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BinaryString
import org.apache.flink.table.plan.schema.FlinkTableFunction
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.utils.TableSqlFunction._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Calcite wrapper for user-defined table functions.
  *
  * @param name function name (used by SQL parser)
  * @param udtf user-defined table function to be called
  * @param implicitResultType Implicit result type information
  * @param typeFactory type factory for converting Flink's between Calcite's types
  * @param functionImpl Calcite table function schema
  * @return [[TableSqlFunction]]
  */
class TableSqlFunction(
    name: String,
    displayName: String,
    udtf: TableFunction[_],
    implicitResultType: DataType,
    typeFactory: FlinkTypeFactory,
    functionImpl: FlinkTableFunction)
  extends SqlUserDefinedTableFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    ReturnTypes.CURSOR,
    createOperandTypeInference(name, udtf, typeFactory),
    createOperandTypeChecker(name, udtf),
    null,
    functionImpl) {

  /**
    * Get the user-defined table function.
    */
  def getTableFunction = udtf

  /**
    * Get the type information of the table returned by the table function.
    */
  def getImplicitResultType: DataType = implicitResultType

  override def isDeterministic: Boolean = udtf.isDeterministic

  override def toString: String = displayName

  override def getRowType(
      typeFactory: RelDataTypeFactory,
      operandList: util.List[SqlNode],
      relTypes: util.List[RelDataType]): RelDataType = {

    val types = relTypes.asScala.map(FlinkTypeFactory.toInternalType).toArray
    val parameterClasses = getEvalMethodSignature(udtf, types)
    val parameters = {
      val builder = ReflectiveFunctionBase.builder
      parameterClasses.foreach((cls) => builder.add(cls, null))
      builder.build()
    }

    val arguments = parameters.zip(operandList.asScala).map {
      case (parameter, relType) =>
        try {
          val o = SqlUserDefinedTableMacro.getValue(relType)
          // TODO Type should convert to internal
          val t = parameter.getType(typeFactory)
          if (o != null && o.isInstanceOf[NlsString] &&
              t.isInstanceOf[RelDataTypeFactoryImpl#JavaType] &&
              t.asInstanceOf[RelDataTypeFactoryImpl#JavaType].getJavaClass
                  == classOf[BinaryString]) {
            o.asInstanceOf[NlsString].getValue
          } else if (o != null && o.isInstanceOf[TimestampString] &&
            t.isInstanceOf[RelDataTypeFactoryImpl#JavaType] &&
            t.asInstanceOf[RelDataTypeFactoryImpl#JavaType].getJavaClass == classOf[Timestamp]) {
            new Timestamp(o.asInstanceOf[TimestampString].getMillisSinceEpoch)
          } else {
            SqlUserDefinedTableMacro.coerce(o, t)
          }
        } catch {
          case e: SqlUserDefinedTableMacro.NonLiteralException =>
            null
        }
    }.toArray

    functionImpl.getRowType(typeFactory, arguments, parameterClasses)
  }
}

object TableSqlFunction {

  private[flink] def createOperandTypeInference(
    name: String,
    udtf: TableFunction[_],
    typeFactory: FlinkTypeFactory)
  : SqlOperandTypeInference = {
    /**
      * Operand type inference based on [[TableFunction]] given information.
      */
    new SqlOperandTypeInference {
      override def inferOperandTypes(
          callBinding: SqlCallBinding,
          returnType: RelDataType,
          operandTypes: Array[RelDataType]): Unit = {
        ScalarSqlFunction.inferOperandTypes(
          name, udtf, typeFactory, callBinding, returnType, operandTypes)
      }
    }
  }

  private[flink] def createOperandTypeChecker(
    name: String,
    udtf: TableFunction[_])
  : SqlOperandTypeChecker = {

    val methods = checkAndExtractMethods(udtf, "eval")

    /**
      * Operand type checker based on [[TableFunction]] given information.
      */
    new SqlOperandTypeChecker {
      override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
        s"$opName[${signaturesToString(udtf, "eval")}]"
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
        val operandTypes = getOperandType(callBinding)

        if (getEvalUserDefinedMethod(udtf, operandTypes).isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypes)} \n" +
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

    }
  }
}
