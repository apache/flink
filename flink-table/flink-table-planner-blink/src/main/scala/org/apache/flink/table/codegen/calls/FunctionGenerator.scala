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

import org.apache.calcite.avatica.util.{TimeUnit, TimeUnitRange}
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.table.`type`._
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable._

import java.lang.reflect.Method

import scala.collection.mutable

object FunctionGenerator {

  // as a key to match any Decimal(p,s)
  val ANY_DEC_TYPE: DecimalType = DecimalType.SYSTEM_DEFAULT

  val INTEGRAL_TYPES = Array(
    InternalTypes.BYTE,
    InternalTypes.SHORT,
    InternalTypes.INT,
    InternalTypes.LONG)

  val FRACTIONAL_TYPES = Array(InternalTypes.FLOAT, InternalTypes.DOUBLE)

  private val sqlFunctions: mutable.Map[(SqlOperator, Seq[InternalType]), CallGenerator] =
    mutable.Map()
  // ----------------------------------------------------------------------------------------------
  // Arithmetic functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    LOG10,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.LOG10)

  addSqlFunctionMethod(
    LOG10,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.LOG10_DEC)

  addSqlFunctionMethod(
    LN,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.LN)

  addSqlFunctionMethod(
    LN,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.LN_DEC)

  addSqlFunctionMethod(
    EXP,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.EXP)

  addSqlFunctionMethod(
    EXP,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.EXP_DEC)

  addSqlFunctionMethod(
    POWER,
    Seq(InternalTypes.DOUBLE, InternalTypes.DOUBLE),
    BuiltInMethods.POWER_NUM_NUM)

  addSqlFunctionMethod(
    POWER,
    Seq(InternalTypes.DOUBLE, ANY_DEC_TYPE),
    BuiltInMethods.POWER_NUM_DEC)

  addSqlFunctionMethod(
    POWER,
    Seq(ANY_DEC_TYPE, ANY_DEC_TYPE),
    BuiltInMethods.POWER_DEC_DEC)

  addSqlFunctionMethod(
    POWER,
    Seq(ANY_DEC_TYPE, InternalTypes.DOUBLE),
    BuiltInMethods.POWER_DEC_NUM)

  addSqlFunctionMethod(
    ABS,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.ABS)

  addSqlFunctionMethod(
    ABS,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.ABS_DEC)

  addSqlFunction(
    FLOOR,
    Seq(InternalTypes.DOUBLE),
    new FloorCeilCallGen(BuiltInMethod.FLOOR.method))

  addSqlFunction(
    FLOOR,
    Seq(ANY_DEC_TYPE),
    new FloorCeilCallGen(BuiltInMethods.FLOOR_DEC))

  addSqlFunction(
    CEIL,
    Seq(InternalTypes.DOUBLE),
    new FloorCeilCallGen(BuiltInMethod.CEIL.method))

  addSqlFunction(
    CEIL,
    Seq(ANY_DEC_TYPE),
    new FloorCeilCallGen(BuiltInMethods.CEIL_DEC))

  addSqlFunctionMethod(
    SIN,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.SIN)

  addSqlFunctionMethod(
    SIN,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.SIN_DEC)

  addSqlFunctionMethod(
    COS,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.COS)

  addSqlFunctionMethod(
    COS,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.COS_DEC)

  addSqlFunctionMethod(
    TAN,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.TAN)

  addSqlFunctionMethod(
    TAN,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.TAN_DEC)

  addSqlFunctionMethod(
    COT,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.COT)

  addSqlFunctionMethod(
    COT,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.COT_DEC)

  addSqlFunctionMethod(
    ASIN,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.ASIN)

  addSqlFunctionMethod(
    ASIN,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.ASIN_DEC)

  addSqlFunctionMethod(
    ACOS,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.ACOS)

  addSqlFunctionMethod(
    ACOS,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.ACOS_DEC)

  addSqlFunctionMethod(
    ATAN,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.ATAN)

  addSqlFunctionMethod(
    ATAN,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.ATAN_DEC)

  addSqlFunctionMethod(
    ATAN2,
    Seq(InternalTypes.DOUBLE, InternalTypes.DOUBLE),
    BuiltInMethods.ATAN2_DOUBLE_DOUBLE)

  addSqlFunctionMethod(
    ATAN2,
    Seq(ANY_DEC_TYPE, ANY_DEC_TYPE),
    BuiltInMethods.ATAN2_DEC_DEC)

  addSqlFunctionMethod(
    DEGREES,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.DEGREES)

  addSqlFunctionMethod(
    DEGREES,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.DEGREES_DEC)

  addSqlFunctionMethod(
    RADIANS,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.RADIANS)

  addSqlFunctionMethod(
    RADIANS,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.RADIANS_DEC)

  addSqlFunctionMethod(
    SIGN,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.SIGN_DOUBLE)

  addSqlFunctionMethod(
    SIGN,
    Seq(InternalTypes.INT),
    BuiltInMethods.SIGN_INT)

  addSqlFunctionMethod(
    SIGN,
    Seq(InternalTypes.LONG),
    BuiltInMethods.SIGN_LONG)

  // note: calcite: SIGN(Decimal(p,s)) => Decimal(p,s). may return e.g. 1.0000
  addSqlFunctionMethod(
    SIGN,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.SIGN_DEC)

  addSqlFunctionMethod(
    ROUND,
    Seq(InternalTypes.LONG, InternalTypes.INT),
    BuiltInMethods.ROUND_LONG)

  addSqlFunctionMethod(
    ROUND,
    Seq(InternalTypes.INT, InternalTypes.INT),
    BuiltInMethods.ROUND_INT)

  addSqlFunctionMethod(
    ROUND,
    Seq(ANY_DEC_TYPE, InternalTypes.INT),
    BuiltInMethods.ROUND_DEC)

  addSqlFunctionMethod(
    ROUND,
    Seq(InternalTypes.DOUBLE, InternalTypes.INT),
    BuiltInMethods.ROUND_DOUBLE)

  addSqlFunctionMethod(
    ROUND,
    Seq(InternalTypes.LONG),
    BuiltInMethods.ROUND_LONG_0)

  addSqlFunctionMethod(
    ROUND,
    Seq(InternalTypes.INT),
    BuiltInMethods.ROUND_INT_0)

  addSqlFunctionMethod(
    ROUND,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.ROUND_DEC_0)

  addSqlFunctionMethod(
    ROUND,
    Seq(InternalTypes.DOUBLE),
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
    Seq(InternalTypes.INT),
    new RandCallGen(isRandInteger = false, hasSeed = true))

  addSqlFunction(
    RAND_INTEGER,
    Seq(InternalTypes.INT),
    new RandCallGen(isRandInteger = true, hasSeed = false))

  addSqlFunction(
    RAND_INTEGER,
    Seq(InternalTypes.INT, InternalTypes.INT),
    new RandCallGen(isRandInteger = true, hasSeed = true))

  addSqlFunctionMethod(
    LOG,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.LOG)

  addSqlFunctionMethod(
    LOG,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.LOG_DEC)

  addSqlFunctionMethod(
    LOG,
    Seq(InternalTypes.DOUBLE, InternalTypes.DOUBLE),
    BuiltInMethods.LOG_WITH_BASE)

  addSqlFunctionMethod(
    LOG,
    Seq(ANY_DEC_TYPE, ANY_DEC_TYPE),
    BuiltInMethods.LOG_WITH_BASE_DEC)

  addSqlFunctionMethod(
    LOG,
    Seq(ANY_DEC_TYPE, InternalTypes.DOUBLE),
    BuiltInMethods.LOG_WITH_BASE_DEC_DOU)

  addSqlFunctionMethod(
    LOG,
    Seq(InternalTypes.DOUBLE, ANY_DEC_TYPE),
    BuiltInMethods.LOG_WITH_BASE_DOU_DEC)

  addSqlFunctionMethod(
    HEX,
    Seq(InternalTypes.LONG),
    BuiltInMethods.HEX_LONG)

  addSqlFunctionMethod(
    HEX,
    Seq(InternalTypes.STRING),
    BuiltInMethods.HEX_STRING)

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), InternalTypes.LONG),
    BuiltInMethod.UNIX_DATE_EXTRACT.method)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), InternalTypes.DATE),
    BuiltInMethod.UNIX_DATE_EXTRACT.method)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), InternalTypes.TIME),
    BuiltInMethods.UNIX_TIME_EXTRACT)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), InternalTypes.TIMESTAMP),
    BuiltInMethods.EXTRACT_FROM_TIMESTAMP)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), InternalTypes.INTERVAL_MILLIS),
    BuiltInMethods.EXTRACT_FROM_DATE)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), InternalTypes.INTERVAL_MONTHS),
    BuiltInMethods.EXTRACT_YEAR_MONTH)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(
      new GenericType(classOf[TimeUnit]),
      InternalTypes.TIMESTAMP,
      InternalTypes.TIMESTAMP),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(new GenericType(classOf[TimeUnit]), InternalTypes.TIMESTAMP, InternalTypes.DATE),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(new GenericType(classOf[TimeUnit]), InternalTypes.DATE, InternalTypes.TIMESTAMP),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(new GenericType(classOf[TimeUnit]), InternalTypes.DATE, InternalTypes.DATE),
    new TimestampDiffCallGen)

  addSqlFunction(
    FLOOR,
    Seq(InternalTypes.DATE, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(InternalTypes.TIME, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(InternalTypes.TIMESTAMP, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethods.TIMESTAMP_FLOOR)))

  addSqlFunction(
    CEIL,
    Seq(InternalTypes.DATE, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_DATE_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(InternalTypes.TIME, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_DATE_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(InternalTypes.TIMESTAMP, new GenericType(classOf[TimeUnitRange])),
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
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.LOG2)

  addSqlFunctionMethod(
    LOG2,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.LOG2_DEC)

  addSqlFunctionMethod(
    SINH,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.SINH)

  addSqlFunctionMethod(
    SINH,
    Seq(DecimalType.SYSTEM_DEFAULT),
    BuiltInMethods.SINH_DEC)

  addSqlFunctionMethod(
    COSH,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.COSH)

  addSqlFunctionMethod(
    COSH,
    Seq(DecimalType.SYSTEM_DEFAULT),
    BuiltInMethods.COSH_DEC)

  addSqlFunctionMethod(
    TANH,
    Seq(InternalTypes.DOUBLE),
    BuiltInMethods.TANH)

  addSqlFunctionMethod(
    TANH,
    Seq(DecimalType.SYSTEM_DEFAULT),
    BuiltInMethods.TANH_DEC)

  addSqlFunctionMethod(
    BITAND,
    Seq(InternalTypes.BYTE, InternalTypes.BYTE),
    BuiltInMethods.BITAND_BYTE)

  addSqlFunctionMethod(
    BITAND,
    Seq(InternalTypes.SHORT, InternalTypes.SHORT),
    BuiltInMethods.BITAND_SHORT)

  addSqlFunctionMethod(
    BITAND,
    Seq(InternalTypes.INT, InternalTypes.INT),
    BuiltInMethods.BITAND_INTEGER)

  addSqlFunctionMethod(
    BITAND,
    Seq(InternalTypes.LONG, InternalTypes.LONG),
    BuiltInMethods.BITAND_LONG)

  addSqlFunctionMethod(
    BITNOT,
    Seq(InternalTypes.BYTE),
    BuiltInMethods.BITNOT_BYTE)

  addSqlFunctionMethod(
    BITNOT,
    Seq(InternalTypes.SHORT),
    BuiltInMethods.BITNOT_SHORT)

  addSqlFunctionMethod(
    BITNOT,
    Seq(InternalTypes.INT),
    BuiltInMethods.BITNOT_INTEGER)

  addSqlFunctionMethod(
    BITNOT,
    Seq(InternalTypes.LONG),
    BuiltInMethods.BITNOT_LONG)

  addSqlFunctionMethod(
    BITOR,
    Seq(InternalTypes.BYTE, InternalTypes.BYTE),
    BuiltInMethods.BITOR_BYTE)

  addSqlFunctionMethod(
    BITOR,
    Seq(InternalTypes.SHORT, InternalTypes.SHORT),
    BuiltInMethods.BITOR_SHORT)

  addSqlFunctionMethod(
    BITOR,
    Seq(InternalTypes.INT, InternalTypes.INT),
    BuiltInMethods.BITOR_INTEGER)

  addSqlFunctionMethod(
    BITOR,
    Seq(InternalTypes.LONG, InternalTypes.LONG),
    BuiltInMethods.BITOR_LONG)

  addSqlFunctionMethod(
    BITXOR,
    Seq(InternalTypes.BYTE, InternalTypes.BYTE),
    BuiltInMethods.BITXOR_BYTE)

  addSqlFunctionMethod(
    BITXOR,
    Seq(InternalTypes.SHORT, InternalTypes.SHORT),
    BuiltInMethods.BITXOR_SHORT)

  addSqlFunctionMethod(
    BITXOR,
    Seq(InternalTypes.INT, InternalTypes.INT),
    BuiltInMethods.BITXOR_INTEGER)

  addSqlFunctionMethod(
    BITXOR,
    Seq(InternalTypes.LONG, InternalTypes.LONG),
    BuiltInMethods.BITXOR_LONG)

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.STRING),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.BOOLEAN),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.BYTE),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.SHORT),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.INT),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.LONG),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.FLOAT),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.DOUBLE),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.DATE),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.TIMESTAMP),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, InternalTypes.TIME),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(InternalTypes.STRING, ANY_DEC_TYPE),
    new PrintCallGen())

  addSqlFunction(
    PRINT,
    Seq(
      InternalTypes.STRING,
      InternalTypes.BINARY),
    new PrintCallGen())

  addSqlFunctionMethod(
    NOW,
    Seq(),
    BuiltInMethods.NOW)

  addSqlFunctionMethod(
    NOW,
    Seq(InternalTypes.INT),
    BuiltInMethods.NOW_OFFSET)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(),
    BuiltInMethods.UNIX_TIMESTAMP)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(InternalTypes.TIMESTAMP),
    BuiltInMethods.UNIX_TIMESTAMP_TS)

  addSqlFunctionMethod(
    DATEDIFF,
    Seq(InternalTypes.TIMESTAMP,
      InternalTypes.TIMESTAMP),
    BuiltInMethods.DATEDIFF_T_T)

  addSqlFunction(
    IF,
    Seq(InternalTypes.BOOLEAN, InternalTypes.STRING, InternalTypes.STRING),
    new IfCallGen())

  addSqlFunction(
    IF,
    Seq(InternalTypes.BOOLEAN, InternalTypes.BOOLEAN, InternalTypes.BOOLEAN),
    new IfCallGen())

  // This sequence must be in sync with [[NumericOrDefaultReturnTypeInference]]
  val numericTypes = Seq(
    InternalTypes.BYTE,
    InternalTypes.SHORT,
    InternalTypes.INT,
    InternalTypes.LONG,
    DecimalType.SYSTEM_DEFAULT,
    InternalTypes.FLOAT,
    InternalTypes.DOUBLE)

  for (t1 <- numericTypes) {
    for (t2 <- numericTypes) {
      addSqlFunction(
        IF,
        Seq(InternalTypes.BOOLEAN, t1, t2),
        new IfCallGen())
    }
  }

  addSqlFunction(
    IF,
    Seq(InternalTypes.BOOLEAN, InternalTypes.DATE, InternalTypes.DATE),
    new IfCallGen())

  addSqlFunction(
    IF,
    Seq(InternalTypes.BOOLEAN, InternalTypes.TIMESTAMP, InternalTypes.TIMESTAMP),
    new IfCallGen())

  addSqlFunction(
    IF,
    Seq(InternalTypes.BOOLEAN, InternalTypes.TIME, InternalTypes.TIME),
    new IfCallGen())

  addSqlFunction(
    IF,
    Seq(
      InternalTypes.BOOLEAN,
      InternalTypes.BINARY,
      InternalTypes.BINARY),
    new IfCallGen())

  addSqlFunctionMethod(
    DIV_INT,
    Seq(InternalTypes.INT, InternalTypes.INT),
    BuiltInMethods.DIV_INT)

  addSqlFunction(
    DIV,
    Seq(ANY_DEC_TYPE, InternalTypes.DOUBLE),
    new DivCallGen()
  )

  addSqlFunction(
    DIV,
    Seq(ANY_DEC_TYPE, ANY_DEC_TYPE),
    new DivCallGen()
  )

  addSqlFunction(
    DIV,
    Seq(InternalTypes.DOUBLE, InternalTypes.DOUBLE),
    new DivCallGen()
  )

  addSqlFunction(
    DIV,
    Seq(InternalTypes.DOUBLE, ANY_DEC_TYPE),
    new DivCallGen()
  )

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.BOOLEAN),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.BYTE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.SHORT),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.INT),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.LONG),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.FLOAT),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.DOUBLE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.DATE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.TIME),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.TIMESTAMP),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(InternalTypes.BINARY),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DecimalType.SYSTEM_DEFAULT),
    new HashCodeCallGen())

  addSqlFunctionMethod(
    TO_DATE,
    Seq(InternalTypes.INT),
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
    Seq(DecimalType.SYSTEM_DEFAULT),
    BuiltInMethods.DECIMAL_TO_TIMESTAMP)

  addSqlFunctionMethod(
    FROM_TIMESTAMP,
    Seq(InternalTypes.TIMESTAMP),
    BuiltInMethods.TIMESTAMP_TO_BIGINT)

  // Date/Time & BinaryString Converting -- start
  addSqlFunctionMethod(
    TO_DATE,
    Seq(InternalTypes.STRING),
    BuiltInMethod.STRING_TO_DATE.method)

  addSqlFunctionMethod(
    TO_DATE,
    Seq(InternalTypes.STRING, InternalTypes.STRING),
    BuiltInMethods.STRING_TO_DATE_WITH_FORMAT)

  addSqlFunctionMethod(
    TO_TIMESTAMP,
    Seq(InternalTypes.STRING),
    BuiltInMethods.STRING_TO_TIMESTAMP)

  addSqlFunctionMethod(
    TO_TIMESTAMP,
    Seq(InternalTypes.STRING, InternalTypes.STRING),
    BuiltInMethods.STRING_TO_TIMESTAMP_WITH_FORMAT)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(InternalTypes.STRING),
    BuiltInMethods.UNIX_TIMESTAMP_STR)

  addSqlFunctionMethod(
    UNIX_TIMESTAMP,
    Seq(InternalTypes.STRING, InternalTypes.STRING),
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
    Seq(DecimalType.SYSTEM_DEFAULT),
    BuiltInMethods.FROM_UNIXTIME_AS_DECIMAL)

  addSqlFunctionMethod(
    FROM_UNIXTIME,
    Seq(InternalTypes.LONG, InternalTypes.STRING),
    BuiltInMethods.FROM_UNIXTIME_FORMAT)

  addSqlFunctionMethod(
    DATEDIFF,
    Seq(InternalTypes.TIMESTAMP, InternalTypes.STRING),
    BuiltInMethods.DATEDIFF_T_S)

  addSqlFunctionMethod(
    DATEDIFF,
    Seq(InternalTypes.STRING, InternalTypes.TIMESTAMP),
    BuiltInMethods.DATEDIFF_S_T)

  addSqlFunctionMethod(
    DATEDIFF,
    Seq(InternalTypes.STRING, InternalTypes.STRING),
    BuiltInMethods.DATEDIFF_S_S)

  addSqlFunctionMethod(
    DATE_FORMAT,
    Seq(InternalTypes.TIMESTAMP, InternalTypes.STRING),
    BuiltInMethods.DATE_FORMAT_LONG_STRING)

  addSqlFunctionMethod(
    DATE_FORMAT,
    Seq(InternalTypes.STRING, InternalTypes.STRING),
    BuiltInMethods.DATE_FORMAT_STIRNG_STRING)

  addSqlFunctionMethod(
    DATE_FORMAT,
    Seq(InternalTypes.STRING, InternalTypes.STRING, InternalTypes.STRING),
    BuiltInMethods.DATE_FORMAT_STRING_STRING_STRING)

  addSqlFunctionMethod(
    DATE_SUB,
    Seq(InternalTypes.STRING, InternalTypes.INT),
    BuiltInMethods.DATE_SUB_S)

  addSqlFunctionMethod(
    DATE_SUB,
    Seq(InternalTypes.TIMESTAMP, InternalTypes.INT),
    BuiltInMethods.DATE_SUB_T)

  addSqlFunctionMethod(
    DATE_ADD,
    Seq(InternalTypes.STRING, InternalTypes.INT),
    BuiltInMethods.DATE_ADD_S)

  addSqlFunctionMethod(
    DATE_ADD,
    Seq(InternalTypes.TIMESTAMP, InternalTypes.INT),
    BuiltInMethods.DATE_ADD_T)

  addSqlFunctionMethod(
    TO_TIMESTAMP_TZ,
    Seq(InternalTypes.STRING, InternalTypes.STRING),
    BuiltInMethods.STRING_TO_TIMESTAMP_TZ)

  addSqlFunctionMethod(
    TO_TIMESTAMP_TZ,
    Seq(InternalTypes.STRING, InternalTypes.STRING, InternalTypes.STRING),
    BuiltInMethods.STRING_TO_TIMESTAMP_FORMAT_TZ)

  addSqlFunctionMethod(
    DATE_FORMAT_TZ,
    Seq(InternalTypes.TIMESTAMP, InternalTypes.STRING),
    BuiltInMethods.DATE_FORMAT_LONG_ZONE)

  addSqlFunctionMethod(
    DATE_FORMAT_TZ,
    Seq(InternalTypes.TIMESTAMP, InternalTypes.STRING, InternalTypes.STRING),
    BuiltInMethods.DATE_FORMAT_LONG_STRING_ZONE)

  addSqlFunctionMethod(
    CONVERT_TZ,
    Seq(InternalTypes.STRING, InternalTypes.STRING, InternalTypes.STRING),
    BuiltInMethods.CONVERT_TZ)

  addSqlFunctionMethod(
    CONVERT_TZ,
    Seq(InternalTypes.STRING, InternalTypes.STRING, InternalTypes.STRING, InternalTypes.STRING),
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
    operandTypes: Seq[InternalType],
    resultType: InternalType)
  : Option[CallGenerator] = sqlOperator match {

    // TODO: support user-defined scalar function

    // TODO: support user-defined table function

    // built-in scalar function
    case _ =>
      sqlFunctions.get((sqlOperator, operandTypes))
        .orElse(sqlFunctions.find(entry => entry._1._1 == sqlOperator
          && entry._1._2.length == operandTypes.length
          && entry._1._2.zip(operandTypes).forall {
          case (x: DecimalType, y: DecimalType) => true
          case (x: PrimitiveType, y: PrimitiveType) => 
            InternalTypeUtils.shouldAutoCastTo(y, x) || x == y
          case (x: InternalType, y: InternalType) => x == y
          case _ => false
        }).map(_._2))

  }

  // ----------------------------------------------------------------------------------------------

  private def addSqlFunctionMethod(
    sqlOperator: SqlOperator,
    operandTypes: Seq[InternalType],
    method: Method)
  : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = new MethodCallGen(method)
  }

  private def addSqlFunction(
    sqlOperator: SqlOperator,
    operandTypes: Seq[InternalType],
    callGenerator: CallGenerator)
  : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = callGenerator
  }


}
