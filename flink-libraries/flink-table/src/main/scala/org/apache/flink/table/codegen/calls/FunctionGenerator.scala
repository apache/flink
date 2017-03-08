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

package org.apache.flink.table.codegen.calls

import java.lang.reflect.Method

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.fun.SqlTrimFunction
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}
import org.apache.flink.table.functions.{EventTimeExtractor, ProcTimeExtractor}
import org.apache.flink.table.functions.utils.{ScalarSqlFunction, TableSqlFunction}

import scala.collection.mutable

/**
  * Global hub for user-defined and built-in advanced SQL functions.
  */
object FunctionGenerator {

  private val sqlFunctions: mutable.Map[(SqlOperator, Seq[TypeInformation[_]]), CallGenerator] =
    mutable.Map()

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    SUBSTRING,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.SUBSTRING.method)

  addSqlFunctionMethod(
    SUBSTRING,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.SUBSTRING.method)

  addSqlFunction(
    TRIM,
    Seq(new GenericTypeInfo(classOf[SqlTrimFunction.Flag]), STRING_TYPE_INFO, STRING_TYPE_INFO),
    new TrimCallGen())

  addSqlFunctionMethod(
    CHAR_LENGTH,
    Seq(STRING_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethod.CHAR_LENGTH.method)

  addSqlFunctionMethod(
    CHARACTER_LENGTH,
    Seq(STRING_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethod.CHAR_LENGTH.method)

  addSqlFunctionMethod(
    UPPER,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.UPPER.method)

  addSqlFunctionMethod(
    LOWER,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.LOWER.method)

  addSqlFunctionMethod(
    INITCAP,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.INITCAP.method)

  addSqlFunctionMethod(
    LIKE,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    BOOLEAN_TYPE_INFO,
    BuiltInMethod.LIKE.method)

  addSqlFunctionMethod(
    LIKE,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO),
    BOOLEAN_TYPE_INFO,
    BuiltInMethods.LIKE_WITH_ESCAPE)

  addSqlFunctionNotMethod(
    NOT_LIKE,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    BuiltInMethod.LIKE.method)

  addSqlFunctionMethod(
    SIMILAR_TO,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    BOOLEAN_TYPE_INFO,
    BuiltInMethod.SIMILAR.method)

  addSqlFunctionMethod(
    SIMILAR_TO,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO),
    BOOLEAN_TYPE_INFO,
    BuiltInMethods.SIMILAR_WITH_ESCAPE)

  addSqlFunctionNotMethod(
    NOT_SIMILAR_TO,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    BuiltInMethod.SIMILAR.method)

  addSqlFunctionMethod(
    POSITION,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethod.POSITION.method)

  addSqlFunctionMethod(
    OVERLAY,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.OVERLAY.method)

  addSqlFunctionMethod(
    OVERLAY,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.OVERLAY.method)

  // ----------------------------------------------------------------------------------------------
  // Arithmetic functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    LOG10,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.LOG10)

  addSqlFunctionMethod(
    LN,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.LN)

  addSqlFunctionMethod(
    EXP,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.EXP)

  addSqlFunctionMethod(
    POWER,
    Seq(DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.POWER)

  addSqlFunctionMethod(
    POWER,
    Seq(DOUBLE_TYPE_INFO, BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.POWER_DEC)

  addSqlFunction(
    ABS,
    Seq(DOUBLE_TYPE_INFO),
    new MultiTypeMethodCallGen(BuiltInMethods.ABS))

  addSqlFunction(
    ABS,
    Seq(BIG_DEC_TYPE_INFO),
    new MultiTypeMethodCallGen(BuiltInMethods.ABS_DEC))

  addSqlFunction(
    FLOOR,
    Seq(DOUBLE_TYPE_INFO),
    new FloorCeilCallGen(BuiltInMethod.FLOOR.method))

  addSqlFunction(
    FLOOR,
    Seq(BIG_DEC_TYPE_INFO),
    new FloorCeilCallGen(BuiltInMethod.FLOOR.method))

  addSqlFunction(
    CEIL,
    Seq(DOUBLE_TYPE_INFO),
    new FloorCeilCallGen(BuiltInMethod.CEIL.method))

  addSqlFunction(
    CEIL,
    Seq(BIG_DEC_TYPE_INFO),
    new FloorCeilCallGen(BuiltInMethod.CEIL.method))

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    EXTRACT_DATE,
    Seq(new GenericTypeInfo(classOf[TimeUnitRange]), LONG_TYPE_INFO),
    LONG_TYPE_INFO,
    BuiltInMethod.UNIX_DATE_EXTRACT.method)

  addSqlFunctionMethod(
    EXTRACT_DATE,
    Seq(new GenericTypeInfo(classOf[TimeUnitRange]), SqlTimeTypeInfo.DATE),
    LONG_TYPE_INFO,
    BuiltInMethod.UNIX_DATE_EXTRACT.method)

  addSqlFunction(
    FLOOR,
    Seq(SqlTimeTypeInfo.DATE, new GenericTypeInfo(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(SqlTimeTypeInfo.TIME, new GenericTypeInfo(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(SqlTimeTypeInfo.TIMESTAMP, new GenericTypeInfo(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method)))

  addSqlFunction(
    CEIL,
    Seq(SqlTimeTypeInfo.DATE, new GenericTypeInfo(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_DATE_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(SqlTimeTypeInfo.TIME, new GenericTypeInfo(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_DATE_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(SqlTimeTypeInfo.TIMESTAMP, new GenericTypeInfo(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_TIMESTAMP_CEIL.method)))

  addSqlFunction(
    CURRENT_DATE,
    Seq(),
    new CurrentTimePointCallGen(SqlTimeTypeInfo.DATE, local = false))

  addSqlFunction(
    CURRENT_TIME,
    Seq(),
    new CurrentTimePointCallGen(SqlTimeTypeInfo.TIME, local = false))

  addSqlFunction(
    CURRENT_TIMESTAMP,
    Seq(),
    new CurrentTimePointCallGen(SqlTimeTypeInfo.TIMESTAMP, local = false))

  addSqlFunction(
    LOCALTIME,
    Seq(),
    new CurrentTimePointCallGen(SqlTimeTypeInfo.TIME, local = true))

  addSqlFunction(
    LOCALTIMESTAMP,
    Seq(),
    new CurrentTimePointCallGen(SqlTimeTypeInfo.TIMESTAMP, local = true))

  // ----------------------------------------------------------------------------------------------

  /**
    * Returns a [[CallGenerator]] that generates all required code for calling the given
    * [[SqlOperator]].
    *
    * @param sqlOperator SQL operator (might be overloaded)
    * @param operandTypes actual operand types
    * @param resultType expected return type
    * @return [[CallGenerator]]
    */
  def getCallGenerator(
      sqlOperator: SqlOperator,
      operandTypes: Seq[TypeInformation[_]],
      resultType: TypeInformation[_])
    : Option[CallGenerator] = sqlOperator match {

    // user-defined scalar function
    case ssf: ScalarSqlFunction =>
      Some(
        new ScalarFunctionCallGen(
          ssf.getScalarFunction,
          operandTypes,
          resultType
        )
      )

    // user-defined table function
    case tsf: TableSqlFunction =>
      Some(
        new TableFunctionCallGen(
          tsf.getTableFunction,
          operandTypes,
          resultType
        )
      )

    // generate a constant for time indicator functions.
    // this is a temporary solution and will be removed when FLINK-5884 is implemented.
    case ProcTimeExtractor | EventTimeExtractor =>
      Some(new CallGenerator {
        override def generate(codeGenerator: CodeGenerator, operands: Seq[GeneratedExpression]) = {
          GeneratedExpression("0L", "false", "", SqlTimeTypeInfo.TIMESTAMP)
        }
      })

    // built-in scalar function
    case _ =>
      sqlFunctions.get((sqlOperator, operandTypes))
        .orElse(sqlFunctions.find(entry => entry._1._1 == sqlOperator
          && entry._1._2.length == operandTypes.length
          && entry._1._2.zip(operandTypes).forall {
          case (x: BasicTypeInfo[_], y: BasicTypeInfo[_]) => y.shouldAutocastTo(x) || x == y
          case _ => false
        }).map(_._2))

  }

  // ----------------------------------------------------------------------------------------------

  private def addSqlFunctionMethod(
      sqlOperator: SqlOperator,
      operandTypes: Seq[TypeInformation[_]],
      returnType: TypeInformation[_],
      method: Method)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = new MethodCallGen(returnType, method)
  }

  private def addSqlFunctionNotMethod(
      sqlOperator: SqlOperator,
      operandTypes: Seq[TypeInformation[_]],
      method: Method)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) =
      new NotCallGenerator(new MethodCallGen(BOOLEAN_TYPE_INFO, method))
  }

  private def addSqlFunction(
      sqlOperator: SqlOperator,
      operandTypes: Seq[TypeInformation[_]],
      callGenerator: CallGenerator)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = callGenerator
  }

}
