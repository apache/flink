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

import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable._
import org.apache.flink.table.types.PlannerTypeUtils.isPrimitive
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot}

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.util.BuiltInMethod

import java.lang.reflect.Method

import scala.collection.mutable

object FunctionGenerator {

  val INTEGRAL_TYPES = Array(
    INTEGER,
    SMALLINT,
    INTEGER,
    BIGINT)

  val FRACTIONAL_TYPES = Array(FLOAT, DOUBLE)

  private val sqlFunctions: mutable.Map[(SqlOperator, Seq[LogicalTypeRoot]), CallGenerator] =
    mutable.Map()
  // ----------------------------------------------------------------------------------------------
  // Arithmetic functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    LOG10,
    Seq(DOUBLE),
    BuiltInMethods.LOG10)

  addSqlFunctionMethod(
    LOG10,
    Seq(DECIMAL),
    BuiltInMethods.LOG10_DEC)

  addSqlFunctionMethod(
    LN,
    Seq(DOUBLE),
    BuiltInMethods.LN)

  addSqlFunctionMethod(
    LN,
    Seq(DECIMAL),
    BuiltInMethods.LN_DEC)

  addSqlFunctionMethod(
    EXP,
    Seq(DOUBLE),
    BuiltInMethods.EXP)

  addSqlFunctionMethod(
    EXP,
    Seq(DECIMAL),
    BuiltInMethods.EXP_DEC)

  addSqlFunctionMethod(
    POWER,
    Seq(DOUBLE, DOUBLE),
    BuiltInMethods.POWER_NUM_NUM)

  addSqlFunctionMethod(
    POWER,
    Seq(DOUBLE, DECIMAL),
    BuiltInMethods.POWER_NUM_DEC)

  addSqlFunctionMethod(
    POWER,
    Seq(DECIMAL, DECIMAL),
    BuiltInMethods.POWER_DEC_DEC)

  addSqlFunctionMethod(
    POWER,
    Seq(DECIMAL, DOUBLE),
    BuiltInMethods.POWER_DEC_NUM)

  addSqlFunctionMethod(
    ABS,
    Seq(DOUBLE),
    BuiltInMethods.ABS)

  addSqlFunctionMethod(
    ABS,
    Seq(DECIMAL),
    BuiltInMethods.ABS_DEC)

  addSqlFunction(
    FLOOR,
    Seq(DOUBLE),
    new FloorCeilCallGen(BuiltInMethod.FLOOR.method))

  addSqlFunction(
    FLOOR,
    Seq(DECIMAL),
    new FloorCeilCallGen(BuiltInMethods.FLOOR_DEC))

  addSqlFunction(
    CEIL,
    Seq(DOUBLE),
    new FloorCeilCallGen(BuiltInMethod.CEIL.method))

  addSqlFunction(
    CEIL,
    Seq(DECIMAL),
    new FloorCeilCallGen(BuiltInMethods.CEIL_DEC))

  addSqlFunctionMethod(
    SIN,
    Seq(DOUBLE),
    BuiltInMethods.SIN)

  addSqlFunctionMethod(
    SIN,
    Seq(DECIMAL),
    BuiltInMethods.SIN_DEC)

  addSqlFunctionMethod(
    COS,
    Seq(DOUBLE),
    BuiltInMethods.COS)

  addSqlFunctionMethod(
    COS,
    Seq(DECIMAL),
    BuiltInMethods.COS_DEC)

  addSqlFunctionMethod(
    TAN,
    Seq(DOUBLE),
    BuiltInMethods.TAN)

  addSqlFunctionMethod(
    TAN,
    Seq(DECIMAL),
    BuiltInMethods.TAN_DEC)

  addSqlFunctionMethod(
    COT,
    Seq(DOUBLE),
    BuiltInMethods.COT)

  addSqlFunctionMethod(
    COT,
    Seq(DECIMAL),
    BuiltInMethods.COT_DEC)

  addSqlFunctionMethod(
    ASIN,
    Seq(DOUBLE),
    BuiltInMethods.ASIN)

  addSqlFunctionMethod(
    ASIN,
    Seq(DECIMAL),
    BuiltInMethods.ASIN_DEC)

  addSqlFunctionMethod(
    ACOS,
    Seq(DOUBLE),
    BuiltInMethods.ACOS)

  addSqlFunctionMethod(
    ACOS,
    Seq(DECIMAL),
    BuiltInMethods.ACOS_DEC)

  addSqlFunctionMethod(
    ATAN,
    Seq(DOUBLE),
    BuiltInMethods.ATAN)

  addSqlFunctionMethod(
    ATAN,
    Seq(DECIMAL),
    BuiltInMethods.ATAN_DEC)

  addSqlFunctionMethod(
    ATAN2,
    Seq(DOUBLE, DOUBLE),
    BuiltInMethods.ATAN2_DOUBLE_DOUBLE)

  addSqlFunctionMethod(
    ATAN2,
    Seq(DECIMAL, DECIMAL),
    BuiltInMethods.ATAN2_DEC_DEC)

  addSqlFunctionMethod(
    DEGREES,
    Seq(DOUBLE),
    BuiltInMethods.DEGREES)

  addSqlFunctionMethod(
    DEGREES,
    Seq(DECIMAL),
    BuiltInMethods.DEGREES_DEC)

  addSqlFunctionMethod(
    RADIANS,
    Seq(DOUBLE),
    BuiltInMethods.RADIANS)

  addSqlFunctionMethod(
    RADIANS,
    Seq(DECIMAL),
    BuiltInMethods.RADIANS_DEC)

  addSqlFunctionMethod(
    SIGN,
    Seq(DOUBLE),
    BuiltInMethods.SIGN_DOUBLE)

  addSqlFunctionMethod(
    SIGN,
    Seq(INTEGER),
    BuiltInMethods.SIGN_INT)

  addSqlFunctionMethod(
    SIGN,
    Seq(BIGINT),
    BuiltInMethods.SIGN_LONG)

  // note: calcite: SIGN(Decimal(p,s)) => Decimal(p,s). may return e.g. 1.0000
  addSqlFunctionMethod(
    SIGN,
    Seq(DECIMAL),
    BuiltInMethods.SIGN_DEC)

  addSqlFunctionMethod(
    ROUND,
    Seq(BIGINT, INTEGER),
    BuiltInMethods.ROUND_LONG)

  addSqlFunctionMethod(
    ROUND,
    Seq(INTEGER, INTEGER),
    BuiltInMethods.ROUND_INT)

  addSqlFunctionMethod(
    ROUND,
    Seq(DECIMAL, INTEGER),
    BuiltInMethods.ROUND_DEC)

  addSqlFunctionMethod(
    ROUND,
    Seq(DOUBLE, INTEGER),
    BuiltInMethods.ROUND_DOUBLE)

  addSqlFunctionMethod(
    ROUND,
    Seq(BIGINT),
    BuiltInMethods.ROUND_LONG_0)

  addSqlFunctionMethod(
    ROUND,
    Seq(INTEGER),
    BuiltInMethods.ROUND_INT_0)

  addSqlFunctionMethod(
    ROUND,
    Seq(DECIMAL),
    BuiltInMethods.ROUND_DEC_0)

  addSqlFunctionMethod(
    ROUND,
    Seq(DOUBLE),
    BuiltInMethods.ROUND_DOUBLE_0)

  addSqlFunction(
    PI,
    Seq(),
    new ConstantCallGen(Math.PI.toString, Math.PI))

  addSqlFunction(
    PI_FUNCTION,
    Seq(),
    new ConstantCallGen(Math.PI.toString, Math.PI))

  addSqlFunction(
    E,
    Seq(),
    new ConstantCallGen(Math.E.toString, Math.PI))

  addSqlFunction(
    RAND,
    Seq(),
    new RandCallGen(isRandInteger = false, hasSeed = false))

  addSqlFunction(
    RAND,
    Seq(INTEGER),
    new RandCallGen(isRandInteger = false, hasSeed = true))

  addSqlFunction(
    RAND_INTEGER,
    Seq(INTEGER),
    new RandCallGen(isRandInteger = true, hasSeed = false))

  addSqlFunction(
    RAND_INTEGER,
    Seq(INTEGER, INTEGER),
    new RandCallGen(isRandInteger = true, hasSeed = true))

  addSqlFunctionMethod(
    LOG,
    Seq(DOUBLE),
    BuiltInMethods.LOG)

  addSqlFunctionMethod(
    LOG,
    Seq(DECIMAL),
    BuiltInMethods.LOG_DEC)

  addSqlFunctionMethod(
    LOG,
    Seq(DOUBLE, DOUBLE),
    BuiltInMethods.LOG_WITH_BASE)

  addSqlFunctionMethod(
    LOG,
    Seq(DECIMAL, DECIMAL),
    BuiltInMethods.LOG_WITH_BASE_DEC)

  addSqlFunctionMethod(
    LOG,
    Seq(DECIMAL, DOUBLE),
    BuiltInMethods.LOG_WITH_BASE_DEC_DOU)

  addSqlFunctionMethod(
    LOG,
    Seq(DOUBLE, DECIMAL),
    BuiltInMethods.LOG_WITH_BASE_DOU_DEC)

  addSqlFunctionMethod(
    HEX,
    Seq(BIGINT),
    BuiltInMethods.HEX_LONG)

  addSqlFunctionMethod(
    HEX,
    Seq(VARCHAR),
    BuiltInMethods.HEX_STRING)

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    EXTRACT,
    Seq(ANY, BIGINT),
    BuiltInMethod.UNIX_DATE_EXTRACT.method)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(ANY, DATE),
    BuiltInMethod.UNIX_DATE_EXTRACT.method)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(ANY, TIME_WITHOUT_TIME_ZONE),
    BuiltInMethods.UNIX_TIME_EXTRACT)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(ANY, TIMESTAMP_WITHOUT_TIME_ZONE),
    BuiltInMethods.EXTRACT_FROM_TIMESTAMP)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(ANY, INTERVAL_DAY_TIME),
    BuiltInMethods.EXTRACT_FROM_DATE)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(ANY, INTERVAL_YEAR_MONTH),
    BuiltInMethods.EXTRACT_YEAR_MONTH)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(
      ANY,
      TIMESTAMP_WITHOUT_TIME_ZONE,
      TIMESTAMP_WITHOUT_TIME_ZONE),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(ANY, TIMESTAMP_WITHOUT_TIME_ZONE, DATE),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(ANY, DATE, TIMESTAMP_WITHOUT_TIME_ZONE),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(ANY, DATE, DATE),
    new TimestampDiffCallGen)

  addSqlFunction(
    FLOOR,
    Seq(DATE, ANY),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(TIME_WITHOUT_TIME_ZONE, ANY),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE, ANY),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethods.TIMESTAMP_FLOOR)))

  addSqlFunction(
    CEIL,
    Seq(DATE, ANY),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_DATE_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(TIME_WITHOUT_TIME_ZONE, ANY),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_DATE_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE, ANY),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethods.TIMESTAMP_CEIL)))

  addSqlFunction(
    CURRENT_DATE,
    Seq(),
    new CurrentTimePointCallGen(false))

  addSqlFunction(
    CURRENT_TIME,
    Seq(),
    new CurrentTimePointCallGen(false))

  addSqlFunction(
    CURRENT_TIMESTAMP,
    Seq(),
    new CurrentTimePointCallGen(false))

  addSqlFunction(
    LOCALTIME,
    Seq(),
    new CurrentTimePointCallGen(true))

  addSqlFunction(
    LOCALTIMESTAMP,
    Seq(),
    new CurrentTimePointCallGen(true))

  addSqlFunctionMethod(
    LOG2,
    Seq(DOUBLE),
    BuiltInMethods.LOG2)

  addSqlFunctionMethod(
    LOG2,
    Seq(DECIMAL),
    BuiltInMethods.LOG2_DEC)

  addSqlFunctionMethod(
    SINH,
    Seq(DOUBLE),
    BuiltInMethods.SINH)

  addSqlFunctionMethod(
    SINH,
    Seq(DECIMAL),
    BuiltInMethods.SINH_DEC)

  addSqlFunctionMethod(
    COSH,
    Seq(DOUBLE),
    BuiltInMethods.COSH)

  addSqlFunctionMethod(
    COSH,
    Seq(DECIMAL),
    BuiltInMethods.COSH_DEC)

  addSqlFunctionMethod(
    TANH,
    Seq(DOUBLE),
    BuiltInMethods.TANH)

  addSqlFunctionMethod(
    TANH,
    Seq(DECIMAL),
    BuiltInMethods.TANH_DEC)

  addSqlFunctionMethod(
    BITAND,
    Seq(INTEGER, INTEGER),
    BuiltInMethods.BITAND_BYTE)

  addSqlFunctionMethod(
    BITAND,
    Seq(SMALLINT, SMALLINT),
    BuiltInMethods.BITAND_SHORT)

  addSqlFunctionMethod(
    BITAND,
    Seq(INTEGER, INTEGER),
    BuiltInMethods.BITAND_INTEGER)

  addSqlFunctionMethod(
    BITAND,
    Seq(BIGINT, BIGINT),
    BuiltInMethods.BITAND_LONG)

  addSqlFunctionMethod(
    BITNOT,
    Seq(INTEGER),
    BuiltInMethods.BITNOT_BYTE)

  addSqlFunctionMethod(
    BITNOT,
    Seq(SMALLINT),
    BuiltInMethods.BITNOT_SHORT)

  addSqlFunctionMethod(
    BITNOT,
    Seq(INTEGER),
    BuiltInMethods.BITNOT_INTEGER)

  addSqlFunctionMethod(
    BITNOT,
    Seq(BIGINT),
    BuiltInMethods.BITNOT_LONG)

  addSqlFunctionMethod(
    BITOR,
    Seq(INTEGER, INTEGER),
    BuiltInMethods.BITOR_BYTE)

  addSqlFunctionMethod(
    BITOR,
    Seq(SMALLINT, SMALLINT),
    BuiltInMethods.BITOR_SHORT)

  addSqlFunctionMethod(
    BITOR,
    Seq(INTEGER, INTEGER),
    BuiltInMethods.BITOR_INTEGER)

  addSqlFunctionMethod(
    BITOR,
    Seq(BIGINT, BIGINT),
    BuiltInMethods.BITOR_LONG)

  addSqlFunctionMethod(
    BITXOR,
    Seq(INTEGER, INTEGER),
    BuiltInMethods.BITXOR_BYTE)

  addSqlFunctionMethod(
    BITXOR,
    Seq(SMALLINT, SMALLINT),
    BuiltInMethods.BITXOR_SHORT)

  addSqlFunctionMethod(
    BITXOR,
    Seq(INTEGER, INTEGER),
    BuiltInMethods.BITXOR_INTEGER)

  addSqlFunctionMethod(
    BITXOR,
    Seq(BIGINT, BIGINT),
    BuiltInMethods.BITXOR_LONG)

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, VARCHAR),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, BOOLEAN),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, INTEGER),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, SMALLINT),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, INTEGER),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, BIGINT),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, FLOAT),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, DOUBLE),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, DATE),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, TIMESTAMP_WITHOUT_TIME_ZONE),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, TIME_WITHOUT_TIME_ZONE),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(VARCHAR, DECIMAL),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(
      VARCHAR,
      VARBINARY),
    new PrintCallGen())

  addSqlFunctionMethod(
    NOW,
    Seq(),
    BuiltInMethods.NOW)

  addSqlFunctionMethod(
    NOW,
    Seq(INTEGER),
    BuiltInMethods.NOW_OFFSET)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(),
    BuiltInMethods.UNIX_TIMESTAMP)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE),
    BuiltInMethods.UNIX_TIMESTAMP_TS)

  addSqlFunctionMethod(
    DATEDIFF,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE,
      TIMESTAMP_WITHOUT_TIME_ZONE),
    BuiltInMethods.DATEDIFF_T_T)

  addSqlFunction(
    IF,
    Seq(BOOLEAN, VARCHAR, VARCHAR),
    new IfCallGen())

  addSqlFunction(
    IF,
    Seq(BOOLEAN, BOOLEAN, BOOLEAN),
    new IfCallGen())

  // This sequence must be in sync with [[NumericOrDefaultReturnTypeInference]]
  val numericTypes = Seq(
    INTEGER,
    SMALLINT,
    INTEGER,
    BIGINT,
    DECIMAL,
    FLOAT,
    DOUBLE)

  for (t1 <- numericTypes) {
    for (t2 <- numericTypes) {
      addSqlFunction(
        IF,
        Seq(BOOLEAN, t1, t2),
        new IfCallGen())
    }
  }

  addSqlFunction(
    IF,
    Seq(BOOLEAN, DATE, DATE),
    new IfCallGen())

  addSqlFunction(
    IF,
    Seq(BOOLEAN, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE),
    new IfCallGen())

  addSqlFunction(
    IF,
    Seq(BOOLEAN, TIME_WITHOUT_TIME_ZONE, TIME_WITHOUT_TIME_ZONE),
    new IfCallGen())

  addSqlFunction(
    IF,
    Seq(
      BOOLEAN,
      VARBINARY,
      VARBINARY),
    new IfCallGen())

  addSqlFunctionMethod(
    DIV_INT,
    Seq(INTEGER, INTEGER),
    BuiltInMethods.DIV_INT)

  addSqlFunction(
    DIV,
    Seq(DECIMAL, DOUBLE),
    new DivCallGen()
  )

  addSqlFunction(
    DIV,
    Seq(DECIMAL, DECIMAL),
    new DivCallGen()
  )

  addSqlFunction(
    DIV,
    Seq(DOUBLE, DOUBLE),
    new DivCallGen()
  )

  addSqlFunction(
    DIV,
    Seq(DOUBLE, DECIMAL),
    new DivCallGen()
  )

  addSqlFunction(
    HASH_CODE,
    Seq(BOOLEAN),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(INTEGER),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(SMALLINT),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(INTEGER),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(BIGINT),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(FLOAT),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DOUBLE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DATE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(TIME_WITHOUT_TIME_ZONE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(VARBINARY),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DECIMAL),
    new HashCodeCallGen())

  addSqlFunctionMethod(
    TO_DATE,
    Seq(INTEGER),
    BuiltInMethods.INT_TO_DATE)

  INTEGRAL_TYPES foreach (
    dt => addSqlFunctionMethod(TO_TIMESTAMP,
      Seq(dt),
      BuiltInMethods.LONG_TO_TIMESTAMP))

  FRACTIONAL_TYPES foreach (
    dt => addSqlFunctionMethod(TO_TIMESTAMP,
      Seq(dt),
      BuiltInMethods.DOUBLE_TO_TIMESTAMP))

  addSqlFunctionMethod(TO_TIMESTAMP,
    Seq(DECIMAL),
    BuiltInMethods.DECIMAL_TO_TIMESTAMP)

  addSqlFunctionMethod(
    FROM_TIMESTAMP,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE),
    BuiltInMethods.TIMESTAMP_TO_BIGINT)

  // Date/Time & BinaryString Converting -- start
  addSqlFunctionMethod(
    TO_DATE,
    Seq(VARCHAR),
    BuiltInMethod.STRING_TO_DATE.method)

  addSqlFunctionMethod(
    TO_DATE,
    Seq(VARCHAR, VARCHAR),
    BuiltInMethods.STRING_TO_DATE_WITH_FORMAT)

  addSqlFunctionMethod(
    TO_TIMESTAMP,
    Seq(VARCHAR),
    BuiltInMethods.STRING_TO_TIMESTAMP)

  addSqlFunctionMethod(
    TO_TIMESTAMP,
    Seq(VARCHAR, VARCHAR),
    BuiltInMethods.STRING_TO_TIMESTAMP_WITH_FORMAT)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(VARCHAR),
    BuiltInMethods.UNIX_TIMESTAMP_STR)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(VARCHAR, VARCHAR),
    BuiltInMethods.UNIX_TIMESTAMP_FORMAT)

  INTEGRAL_TYPES foreach (
    dt => addSqlFunctionMethod(
      FROM_UNIXTIME,
      Seq(dt),
      BuiltInMethods.FROM_UNIXTIME))

  FRACTIONAL_TYPES foreach (
    dt => addSqlFunctionMethod(
      FROM_UNIXTIME,
      Seq(dt),
      BuiltInMethods.FROM_UNIXTIME_AS_DOUBLE))

  addSqlFunctionMethod(
    FROM_UNIXTIME,
    Seq(DECIMAL),
    BuiltInMethods.FROM_UNIXTIME_AS_DECIMAL)

  addSqlFunctionMethod(
    FROM_UNIXTIME,
    Seq(BIGINT, VARCHAR),
    BuiltInMethods.FROM_UNIXTIME_FORMAT)

  addSqlFunctionMethod(
    DATEDIFF,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE, VARCHAR),
    BuiltInMethods.DATEDIFF_T_S)

  addSqlFunctionMethod(
    DATEDIFF,
    Seq(VARCHAR, TIMESTAMP_WITHOUT_TIME_ZONE),
    BuiltInMethods.DATEDIFF_S_T)

  addSqlFunctionMethod(
    DATEDIFF,
    Seq(VARCHAR, VARCHAR),
    BuiltInMethods.DATEDIFF_S_S)

  addSqlFunctionMethod(
    DATE_FORMAT,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE, VARCHAR),
    BuiltInMethods.DATE_FORMAT_LONG_STRING)

  addSqlFunctionMethod(
    DATE_FORMAT,
    Seq(VARCHAR, VARCHAR),
    BuiltInMethods.DATE_FORMAT_STIRNG_STRING)

  addSqlFunctionMethod(
    DATE_FORMAT,
    Seq(
      VARCHAR,
      VARCHAR,
      VARCHAR),
    BuiltInMethods.DATE_FORMAT_STRING_STRING_STRING)

  addSqlFunctionMethod(
    DATE_SUB,
    Seq(VARCHAR, INTEGER),
    BuiltInMethods.DATE_SUB_S)

  addSqlFunctionMethod(
    DATE_SUB,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE, INTEGER),
    BuiltInMethods.DATE_SUB_T)

  addSqlFunctionMethod(
    DATE_ADD,
    Seq(VARCHAR, INTEGER),
    BuiltInMethods.DATE_ADD_S)

  addSqlFunctionMethod(
    DATE_ADD,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE, INTEGER),
    BuiltInMethods.DATE_ADD_T)

  addSqlFunctionMethod(
    TO_TIMESTAMP_TZ,
    Seq(
      VARCHAR,
      VARCHAR),
    BuiltInMethods.STRING_TO_TIMESTAMP_TZ)

  addSqlFunctionMethod(
    TO_TIMESTAMP_TZ,
    Seq(
      VARCHAR,
      VARCHAR,
      VARCHAR),
    BuiltInMethods.STRING_TO_TIMESTAMP_FORMAT_TZ)

  addSqlFunctionMethod(
    DATE_FORMAT_TZ,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE, VARCHAR),
    BuiltInMethods.DATE_FORMAT_LONG_ZONE)

  addSqlFunctionMethod(
    DATE_FORMAT_TZ,
    Seq(
      TIMESTAMP_WITHOUT_TIME_ZONE,
      VARCHAR,
      VARCHAR),
    BuiltInMethods.DATE_FORMAT_LONG_STRING_ZONE)

  addSqlFunctionMethod(
    CONVERT_TZ,
    Seq(
      VARCHAR,
      VARCHAR,
      VARCHAR),
    BuiltInMethods.CONVERT_TZ)

  addSqlFunctionMethod(
    CONVERT_TZ,
    Seq(
      VARCHAR,
      VARCHAR,
      VARCHAR,
      VARCHAR),
    BuiltInMethods.CONVERT_FORMAT_TZ)

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
    operandTypes: Seq[LogicalType],
    resultType: LogicalType)
  : Option[CallGenerator] = sqlOperator match {
    // built-in scalar function
    case _ =>
      val typeRoots = operandTypes.map(_.getTypeRoot)
      sqlFunctions.get((sqlOperator, typeRoots))
        .orElse(sqlFunctions.find(entry => entry._1._1 == sqlOperator
          && entry._1._2.length == typeRoots.length
          && entry._1._2.zip(typeRoots).forall {
          case (DECIMAL, DECIMAL) => true
          case (x, y) if isPrimitive(x) && isPrimitive(y) => shouldAutoCastTo(y, x) || x == y
          case (x, y) => x == y
          case _ => false
        }).map(_._2))
  }

  /**
    * Returns whether this type should be automatically casted to
    * the target type in an arithmetic operation.
    */
  def shouldAutoCastTo(from: LogicalTypeRoot, to: LogicalTypeRoot): Boolean = {
    from match {
      case TINYINT =>
        (to eq SMALLINT) || (to eq INTEGER) || (to eq BIGINT) || (to eq FLOAT) || (to eq DOUBLE)
      case SMALLINT =>
        (to eq INTEGER) || (to eq BIGINT) || (to eq FLOAT) || (to eq DOUBLE)
      case INTEGER =>
        (to eq BIGINT) || (to eq FLOAT) || (to eq DOUBLE)
      case BIGINT =>
        (to eq FLOAT) || (to eq DOUBLE)
      case FLOAT =>
        to eq DOUBLE
      case _ =>
        false
    }
  }

  // ----------------------------------------------------------------------------------------------

  private def addSqlFunctionMethod(
    sqlOperator: SqlOperator,
    operandTypes: Seq[LogicalTypeRoot],
    method: Method)
  : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = new MethodCallGen(method)
  }

  private def addSqlFunction(
    sqlOperator: SqlOperator,
    operandTypes: Seq[LogicalTypeRoot],
    callGenerator: CallGenerator)
  : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = callGenerator
  }


}
