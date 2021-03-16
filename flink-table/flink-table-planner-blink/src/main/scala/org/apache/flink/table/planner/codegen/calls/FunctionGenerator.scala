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

package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable._
import org.apache.flink.table.runtime.types.PlannerTypeUtils.isPrimitive
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot}

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.util.BuiltInMethod

import java.lang.reflect.Method

import scala.collection.mutable

object FunctionGenerator {

  val INTEGRAL_TYPES = Array(
    TINYINT,
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

  addSqlFunctionMethod(HEX, Seq(VARCHAR), BuiltInMethods.HEX_STRING)
  addSqlFunctionMethod(HEX, Seq(CHAR), BuiltInMethods.HEX_STRING)

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunction(
    EXTRACT,
    Seq(RAW, BIGINT),
    new ExtractCallGen(BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(RAW, DATE),
    new ExtractCallGen(BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(RAW, TIME_WITHOUT_TIME_ZONE),
    new ExtractCallGen(BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(RAW, TIMESTAMP_WITHOUT_TIME_ZONE),
    new ExtractCallGen(BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(RAW, TIMESTAMP_WITH_LOCAL_TIME_ZONE),
    new MethodCallGen(BuiltInMethods.EXTRACT_FROM_TIMESTAMP_TIME_ZONE))

  addSqlFunction(
    EXTRACT,
    Seq(RAW, INTERVAL_DAY_TIME),
    new ExtractCallGen(BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    EXTRACT,
    Seq(RAW, INTERVAL_YEAR_MONTH),
    new ExtractCallGen(BuiltInMethod.UNIX_DATE_EXTRACT.method))

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(
      RAW,
      TIMESTAMP_WITHOUT_TIME_ZONE,
      TIMESTAMP_WITHOUT_TIME_ZONE),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(RAW, TIMESTAMP_WITHOUT_TIME_ZONE, DATE),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(RAW, DATE, TIMESTAMP_WITHOUT_TIME_ZONE),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(RAW, DATE, DATE),
    new TimestampDiffCallGen)

  addSqlFunction(
    FLOOR,
    Seq(DATE, RAW),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(TIME_WITHOUT_TIME_ZONE, RAW),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE, RAW),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(TIMESTAMP_WITH_LOCAL_TIME_ZONE, RAW),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethods.TIMESTAMP_FLOOR_TIME_ZONE)))

  // TODO: fixme if CALCITE-3199 fixed
  //  https://issues.apache.org/jira/browse/CALCITE-3199
  addSqlFunction(
    CEIL,
    Seq(DATE, RAW),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethods.UNIX_DATE_CEIL)))

  addSqlFunction(
    CEIL,
    Seq(TIME_WITHOUT_TIME_ZONE, RAW),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_DATE_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE, RAW),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_TIMESTAMP_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(TIMESTAMP_WITH_LOCAL_TIME_ZONE, RAW),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethods.TIMESTAMP_CEIL_TIME_ZONE)))

  addSqlFunction(
    CURRENT_DATE,
    Seq(),
    new CurrentTimePointCallGen(false))

  addSqlFunction(
    CURRENT_TIME,
    Seq(),
    new CurrentTimePointCallGen(false))

  addSqlFunction(
    NOW,
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
    UNIX_TIMESTAMP,
    Seq(),
    BuiltInMethods.UNIX_TIMESTAMP)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(TIMESTAMP_WITHOUT_TIME_ZONE),
    BuiltInMethods.UNIX_TIMESTAMP_TS)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(TIMESTAMP_WITH_LOCAL_TIME_ZONE),
    BuiltInMethods.UNIX_TIMESTAMP_TS)

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

  addSqlFunction(
    IF,
    Seq(
      BOOLEAN,
      BINARY,
      BINARY),
    new IfCallGen())

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
    Seq(BINARY),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DECIMAL),
    new HashCodeCallGen())

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

  INTEGRAL_TYPES foreach (
    dt => {
      addSqlFunctionMethod(
        TO_TIMESTAMP_LTZ,
        Seq(dt),
        BuiltInMethods.LONG_TO_TIMESTAMP_LTZ)
      addSqlFunctionMethod(
        TO_TIMESTAMP_LTZ,
        Seq(dt, INTEGER),
        BuiltInMethods.LONG_TO_TIMESTAMP_LTZ_WITH_PRECISION)})

  FRACTIONAL_TYPES foreach (
    dt => {
      addSqlFunctionMethod(
        TO_TIMESTAMP_LTZ,
        Seq(dt),
        BuiltInMethods.DOUBLE_TO_TIMESTAMP_LTZ)
      addSqlFunctionMethod(
        TO_TIMESTAMP_LTZ,
        Seq(dt, INTEGER),
        BuiltInMethods.DOUBLE_TO_TIMESTAMP_LTZ_WITH_PRECISION)})

  addSqlFunctionMethod(
    TO_TIMESTAMP_LTZ,
    Seq(DECIMAL),
    BuiltInMethods.DECIMAL_TO_TIMESTAMP_LTZ)

  addSqlFunctionMethod(
    TO_TIMESTAMP_LTZ,
    Seq(DECIMAL, INTEGER),
    BuiltInMethods.DECIMAL_TO_TIMESTAMP_LTZ_WITH_PRECISION)

  INTEGRAL_TYPES foreach (
    dt => addSqlFunctionMethod(
      FROM_UNIXTIME,
      Seq(dt),
      BuiltInMethods.FROM_UNIXTIME))

  addSqlFunctionMethod(FROM_UNIXTIME, Seq(BIGINT, VARCHAR), BuiltInMethods.FROM_UNIXTIME_FORMAT)
  addSqlFunctionMethod(FROM_UNIXTIME, Seq(BIGINT, CHAR), BuiltInMethods.FROM_UNIXTIME_FORMAT)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(BIGINT),
    BuiltInMethods.TRUNCATE_LONG_ONE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(INTEGER),
    BuiltInMethods.TRUNCATE_INT_ONE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(DECIMAL),
    BuiltInMethods.TRUNCATE_DEC_ONE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(DOUBLE),
    BuiltInMethods.TRUNCATE_DOUBLE_ONE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(FLOAT),
    BuiltInMethods.TRUNCATE_FLOAT_ONE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(BIGINT, INTEGER),
    BuiltInMethods.TRUNCATE_LONG)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(INTEGER, INTEGER),
    BuiltInMethods.TRUNCATE_INT)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(DECIMAL, INTEGER),
    BuiltInMethods.TRUNCATE_DEC)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(DOUBLE, INTEGER),
    BuiltInMethods.TRUNCATE_DOUBLE)

  addSqlFunctionMethod(
    TRUNCATE,
    Seq(FLOAT, INTEGER),
    BuiltInMethods.TRUNCATE_FLOAT)

  addSqlFunctionMethod(IS_JSON_VALUE, Seq(CHAR), BuiltInMethod.IS_JSON_VALUE.method)
  addSqlFunctionMethod(IS_JSON_VALUE, Seq(VARCHAR), BuiltInMethod.IS_JSON_VALUE.method)

  addSqlFunctionMethod(IS_JSON_OBJECT, Seq(CHAR), BuiltInMethod.IS_JSON_OBJECT.method)
  addSqlFunctionMethod(IS_JSON_OBJECT, Seq(VARCHAR), BuiltInMethod.IS_JSON_OBJECT.method)

  addSqlFunctionMethod(IS_JSON_ARRAY, Seq(CHAR), BuiltInMethod.IS_JSON_ARRAY.method)
  addSqlFunctionMethod(IS_JSON_ARRAY, Seq(VARCHAR), BuiltInMethod.IS_JSON_ARRAY.method)

  addSqlFunctionMethod(IS_JSON_SCALAR, Seq(CHAR), BuiltInMethod.IS_JSON_SCALAR.method)
  addSqlFunctionMethod(IS_JSON_SCALAR, Seq(VARCHAR), BuiltInMethod.IS_JSON_SCALAR.method)

  addSqlFunction(IS_NOT_JSON_VALUE, Seq(CHAR),
    new NotCallGen(
      new MethodCallGen(
        BuiltInMethod.IS_JSON_VALUE.method)))
  addSqlFunction(IS_NOT_JSON_VALUE, Seq(VARCHAR),
    new NotCallGen(
      new MethodCallGen(
        BuiltInMethod.IS_JSON_VALUE.method)))

  addSqlFunction(IS_NOT_JSON_OBJECT, Seq(CHAR),
    new NotCallGen(
      new MethodCallGen(
        BuiltInMethod.IS_JSON_OBJECT.method)))
  addSqlFunction(IS_NOT_JSON_OBJECT, Seq(VARCHAR),
    new NotCallGen(
      new MethodCallGen(
        BuiltInMethod.IS_JSON_OBJECT.method)))

  addSqlFunction(IS_NOT_JSON_ARRAY, Seq(CHAR),
    new NotCallGen(
      new MethodCallGen(
        BuiltInMethod.IS_JSON_ARRAY.method)))
  addSqlFunction(IS_NOT_JSON_ARRAY, Seq(VARCHAR),
    new NotCallGen(
      new MethodCallGen(
        BuiltInMethod.IS_JSON_ARRAY.method)))

  addSqlFunction(IS_NOT_JSON_SCALAR, Seq(CHAR),
    new NotCallGen(
      new MethodCallGen(
        BuiltInMethod.IS_JSON_SCALAR.method)))
  addSqlFunction(IS_NOT_JSON_SCALAR, Seq(VARCHAR),
    new NotCallGen(
      new MethodCallGen(
        BuiltInMethod.IS_JSON_SCALAR.method)))


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
    method: Method): Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = new MethodCallGen(method)
  }

  private def addSqlFunction(
    sqlOperator: SqlOperator,
    operandTypes: Seq[LogicalTypeRoot],
    callGenerator: CallGenerator): Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = callGenerator
  }
}
