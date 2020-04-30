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

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.fun.SqlTrimFunction
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.functions.sql.ScalarSqlFunctions._
import org.apache.flink.table.functions.utils.{ScalarSqlFunction, TableSqlFunction}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo

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

  addSqlFunctionMethod(
    REGEXP_REPLACE,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.REGEXP_REPLACE)

  addSqlFunctionMethod(
    REPLACE,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.REPLACE.method)

  addSqlFunctionMethod(
    REGEXP_EXTRACT,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.REGEXP_EXTRACT)

  addSqlFunctionMethod(
    REGEXP_EXTRACT,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.REGEXP_EXTRACT_WITHOUT_INDEX)

  addSqlFunctionMethod(
    FROM_BASE64,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.FROMBASE64)

  addSqlFunctionMethod(
    TO_BASE64,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.TOBASE64)

  addSqlFunctionMethod(
    UUID,
    Seq(),
    STRING_TYPE_INFO,
    BuiltInMethods.UUID)

  addSqlFunctionMethod(
    LTRIM,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.LTRIM.method)

  addSqlFunctionMethod(
    RTRIM,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.RTRIM.method)

  addSqlFunctionMethod(
    REPEAT,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.REPEAT)

  // ----------------------------------------------------------------------------------------------
  // Arithmetic functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    LOG10,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.LOG10)

  addSqlFunctionMethod(
    LOG2,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.LOG2)

  addSqlFunctionMethod(
    COSH,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.COSH)

  addSqlFunctionMethod(
    COSH,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.COSH_DEC)

  addSqlFunctionMethod(
    LN,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.LN)

  addSqlFunctionMethod(
    SINH,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.SINH)

  addSqlFunctionMethod(
    SINH,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.SINH_DEC)

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

  addSqlFunctionMethod(
    POWER,
    Seq(BIG_DEC_TYPE_INFO, BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.POWER_DEC_DEC)

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

  addSqlFunctionMethod(
    SIN,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.SIN)

  addSqlFunctionMethod(
    SIN,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.SIN_DEC)

  addSqlFunctionMethod(
    COS,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.COS)

  addSqlFunctionMethod(
    COS,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.COS_DEC)

  addSqlFunctionMethod(
    TAN,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.TAN)

  addSqlFunctionMethod(
    TAN,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.TAN_DEC)

  addSqlFunctionMethod(
    TANH,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.TANH)

  addSqlFunctionMethod(
    TANH,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.TANH_DEC)

  addSqlFunctionMethod(
    COT,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.COT)

  addSqlFunctionMethod(
    COT,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.COT_DEC)

  addSqlFunctionMethod(
    ASIN,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ASIN)

  addSqlFunctionMethod(
    ASIN,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ASIN_DEC)

  addSqlFunctionMethod(
    ACOS,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ACOS)

  addSqlFunctionMethod(
    ACOS,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ACOS_DEC)

  addSqlFunctionMethod(
    ATAN,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ATAN)

  addSqlFunctionMethod(
    ATAN,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ATAN_DEC)

  addSqlFunctionMethod(
    ATAN2,
    Seq(DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ATAN2_DOUBLE_DOUBLE)

  addSqlFunctionMethod(
    ATAN2,
    Seq(BIG_DEC_TYPE_INFO, BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ATAN2_DEC_DEC)

  addSqlFunctionMethod(
    DEGREES,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.DEGREES)

  addSqlFunctionMethod(
    DEGREES,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.DEGREES_DEC)

  addSqlFunctionMethod(
    RADIANS,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.RADIANS)

  addSqlFunctionMethod(
    RADIANS,
    Seq(BIG_DEC_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.RADIANS_DEC)

  addSqlFunctionMethod(
    SIGN,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.SIGN_DOUBLE)

  addSqlFunctionMethod(
    SIGN,
    Seq(INT_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethods.SIGN_INT)

  addSqlFunctionMethod(
    SIGN,
    Seq(LONG_TYPE_INFO),
    LONG_TYPE_INFO,
    BuiltInMethods.SIGN_LONG)

  addSqlFunctionMethod(
    SIGN,
    Seq(BIG_DEC_TYPE_INFO),
    BIG_DEC_TYPE_INFO,
    BuiltInMethods.SIGN_DEC)

  addSqlFunctionMethod(
    ROUND,
    Seq(LONG_TYPE_INFO, INT_TYPE_INFO),
    LONG_TYPE_INFO,
    BuiltInMethods.ROUND_LONG)

  addSqlFunctionMethod(
    ROUND,
    Seq(INT_TYPE_INFO, INT_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethods.ROUND_INT)

  addSqlFunctionMethod(
    ROUND,
    Seq(BIG_DEC_TYPE_INFO, INT_TYPE_INFO),
    BIG_DEC_TYPE_INFO,
    BuiltInMethods.ROUND_DEC)

  addSqlFunctionMethod(
    ROUND,
    Seq(DOUBLE_TYPE_INFO, INT_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ROUND_DOUBLE)

  addSqlFunction(
    PI,
    Seq(),
    new ConstantCallGen(DOUBLE_TYPE_INFO, Math.PI.toString))

  addSqlFunction(
    E,
    Seq(),
    new ConstantCallGen(DOUBLE_TYPE_INFO, Math.E.toString))

  addSqlFunction(
    RAND,
    Seq(),
    new RandCallGen(isRandInteger = false, hasSeed = false))

  addSqlFunction(
    RAND,
    Seq(INT_TYPE_INFO),
    new RandCallGen(isRandInteger = false, hasSeed = true))

  addSqlFunction(
    RAND_INTEGER,
    Seq(INT_TYPE_INFO),
    new RandCallGen(isRandInteger = true, hasSeed = false))

  addSqlFunction(
    RAND_INTEGER,
    Seq(INT_TYPE_INFO, INT_TYPE_INFO),
    new RandCallGen(isRandInteger = true, hasSeed = true))

  addSqlFunctionMethod(
    ScalarSqlFunctions.LOG,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.LOG)

  addSqlFunctionMethod(
    ScalarSqlFunctions.LOG,
    Seq(DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.LOG_WITH_BASE)

  addSqlFunction(
    ScalarSqlFunctions.E,
    Seq(),
    new ConstantCallGen(DOUBLE_TYPE_INFO, Math.E.toString))

  addSqlFunctionMethod(
    ScalarSqlFunctions.BIN,
    Seq(LONG_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.BIN)

  addSqlFunctionMethod(
    ScalarSqlFunctions.HEX,
    Seq(LONG_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.HEX_LONG)

  addSqlFunctionMethod(
    ScalarSqlFunctions.HEX,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.HEX_STRING)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(LONG_TYPE_INFO),
    LONG_TYPE_INFO,
    BuiltInMethods.TRUNCATE_LONG_ONE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(INT_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethods.TRUNCATE_INT_ONE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(BIG_DEC_TYPE_INFO),
    BIG_DEC_TYPE_INFO,
    BuiltInMethods.TRUNCATE_DEC_ONE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.TRUNCATE_DOUBLE_ONE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(LONG_TYPE_INFO, INT_TYPE_INFO),
    LONG_TYPE_INFO,
    BuiltInMethods.TRUNCATE_LONG)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(INT_TYPE_INFO, INT_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethods.TRUNCATE_INT)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(BIG_DEC_TYPE_INFO, INT_TYPE_INFO),
    BIG_DEC_TYPE_INFO,
    BuiltInMethods.TRUNCATE_DEC)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(DOUBLE_TYPE_INFO, INT_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.TRUNCATE_DOUBLE)

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunction(
    EXTRACT,
    Seq(new GenericTypeInfo(classOf[TimeUnitRange]), LONG_TYPE_INFO),
    new ExtractCallGen(LONG_TYPE_INFO, BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(new GenericTypeInfo(classOf[TimeUnitRange]), TimeIntervalTypeInfo.INTERVAL_MILLIS),
    new ExtractCallGen(LONG_TYPE_INFO, BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(new GenericTypeInfo(classOf[TimeUnitRange]), SqlTimeTypeInfo.TIMESTAMP),
    new ExtractCallGen(LONG_TYPE_INFO, BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(new GenericTypeInfo(classOf[TimeUnitRange]), SqlTimeTypeInfo.TIME),
    new ExtractCallGen(LONG_TYPE_INFO, BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(new GenericTypeInfo(classOf[TimeUnitRange]), TimeIntervalTypeInfo.INTERVAL_MONTHS),
    new ExtractCallGen(LONG_TYPE_INFO, BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(new GenericTypeInfo(classOf[TimeUnitRange]), SqlTimeTypeInfo.DATE),
    new ExtractCallGen(LONG_TYPE_INFO, BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(
      new GenericTypeInfo(classOf[TimeUnit]),
      SqlTimeTypeInfo.TIMESTAMP,
      SqlTimeTypeInfo.TIMESTAMP),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(new GenericTypeInfo(classOf[TimeUnit]), SqlTimeTypeInfo.TIMESTAMP, SqlTimeTypeInfo.DATE),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(new GenericTypeInfo(classOf[TimeUnit]), SqlTimeTypeInfo.DATE, SqlTimeTypeInfo.TIMESTAMP),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(new GenericTypeInfo(classOf[TimeUnit]), SqlTimeTypeInfo.DATE, SqlTimeTypeInfo.DATE),
    new TimestampDiffCallGen)

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

  // TODO: fixme if CALCITE-3199 fixed, use BuiltInMethod.UNIX_DATE_CEIL
  //  https://issues.apache.org/jira/browse/CALCITE-3199
  addSqlFunction(
    CEIL,
    Seq(SqlTimeTypeInfo.DATE, new GenericTypeInfo(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethods.UNIX_DATE_CEIL)))

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

  addSqlFunction(
    ScalarSqlFunctions.DATE_FORMAT,
    Seq(SqlTimeTypeInfo.TIMESTAMP, STRING_TYPE_INFO),
    new DateFormatCallGen
  )
  addSqlFunctionMethod(
    ScalarSqlFunctions.LPAD,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO, STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.LPAD)

  addSqlFunctionMethod(
    ScalarSqlFunctions.RPAD,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO, STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethods.RPAD)

  // ----------------------------------------------------------------------------------------------
  // Cryptographic Hash functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunction(
    ScalarSqlFunctions.MD5,
    Seq(STRING_TYPE_INFO),
    new HashCalcCallGen("MD5")
  )

  addSqlFunction(
    ScalarSqlFunctions.SHA1,
    Seq(STRING_TYPE_INFO),
    new HashCalcCallGen("SHA-1")
  )

  addSqlFunction(
    ScalarSqlFunctions.SHA224,
    Seq(STRING_TYPE_INFO),
    new HashCalcCallGen("SHA-224")
  )

  addSqlFunction(
    ScalarSqlFunctions.SHA256,
    Seq(STRING_TYPE_INFO),
    new HashCalcCallGen("SHA-256")
  )

  addSqlFunction(
    ScalarSqlFunctions.SHA384,
    Seq(STRING_TYPE_INFO),
    new HashCalcCallGen("SHA-384")
  )

  addSqlFunction(
    ScalarSqlFunctions.SHA512,
    Seq(STRING_TYPE_INFO),
    new HashCalcCallGen("SHA-512")
  )

  addSqlFunction(
    ScalarSqlFunctions.SHA2,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO),
    new HashCalcCallGen("SHA-2")
  )

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
