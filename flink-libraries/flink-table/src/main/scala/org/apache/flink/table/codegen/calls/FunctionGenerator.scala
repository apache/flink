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
import org.apache.calcite.avatica.util.{TimeUnit, TimeUnitRange}
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.table.api.types._
import org.apache.flink.table.functions.sql.ScalarSqlFunctions.{COSH, _}
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.functions.utils.{ScalarSqlFunction, TableSqlFunction}
import org.apache.flink.table.typeutils.TypeUtils

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Global hub for user-defined and built-in advanced SQL functions.
  */
object FunctionGenerator {

  // as a key to match any Decimal(p,s)
  val ANY_DEC_TYPE: DecimalType = DecimalType.SYSTEM_DEFAULT

  private val sqlFunctions: mutable.Map[(SqlOperator, Seq[InternalType]), CallGenerator] =
    mutable.Map()

  private val timeZoneFunctions: mutable.Set[Method] = mutable.Set()

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunction(
    ScalarSqlFunctions.PROCTIME,
    Seq(),
    new ProctimeCallGen()
  )

  // ----------------------------------------------------------------------------------------------
  // Arithmetic functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    LOG10,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG10)

  addSqlFunctionMethod(
    LOG10,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG10_DEC)

  addSqlFunctionMethod(
    LN,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.LN)

  addSqlFunctionMethod(
    LN,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.LN_DEC)

  addSqlFunctionMethod(
    EXP,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.EXP)

  addSqlFunctionMethod(
    EXP,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.EXP_DEC)

  addSqlFunctionMethod(
    POWER,
    Seq(DataTypes.DOUBLE, DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.POWER_NUM_NUM)

  addSqlFunctionMethod(
    POWER,
    Seq(DataTypes.DOUBLE, ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.POWER_NUM_DEC)

  addSqlFunctionMethod(
    POWER,
    Seq(ANY_DEC_TYPE, ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.POWER_DEC_DEC)

  addSqlFunctionMethod(
    POWER,
    Seq(ANY_DEC_TYPE, DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.POWER_DEC_NUM)

  addSqlFunctionMethod2(
    ABS,
    Seq(DataTypes.DOUBLE),
    BuiltInMethods.ABS)

  addSqlFunctionMethod2(
    ABS,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.ABS_DEC)

  addSqlFunction(
    FLOOR,
    Seq(DataTypes.DOUBLE),
    new FloorCeilCallGen(BuiltInMethod.FLOOR.method))

  addSqlFunction(
    FLOOR,
    Seq(ANY_DEC_TYPE),
    new FloorCeilCallGen(BuiltInMethods.FLOOR_DEC))

  addSqlFunction(
    CEIL,
    Seq(DataTypes.DOUBLE),
    new FloorCeilCallGen(BuiltInMethod.CEIL.method))

  addSqlFunction(
    CEIL,
    Seq(ANY_DEC_TYPE),
    new FloorCeilCallGen(BuiltInMethods.CEIL_DEC))

  addSqlFunctionMethod(
    SIN,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.SIN)

  addSqlFunctionMethod(
    SIN,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.SIN_DEC)

  addSqlFunctionMethod(
    COS,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.COS)

  addSqlFunctionMethod(
    COS,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.COS_DEC)

  addSqlFunctionMethod(
    TAN,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.TAN)

  addSqlFunctionMethod(
    TAN,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.TAN_DEC)

  addSqlFunctionMethod(
    COT,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.COT)

  addSqlFunctionMethod(
    COT,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.COT_DEC)

  addSqlFunctionMethod(
    ASIN,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.ASIN)

  addSqlFunctionMethod(
    ASIN,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.ASIN_DEC)

  addSqlFunctionMethod(
    ACOS,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.ACOS)

  addSqlFunctionMethod(
    ACOS,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.ACOS_DEC)

  addSqlFunctionMethod(
    ATAN,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.ATAN)

  addSqlFunctionMethod(
    ATAN,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.ATAN_DEC)

  addSqlFunctionMethod(
    ATAN2,
    Seq(DataTypes.DOUBLE, DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.ATAN2_DOUBLE_DOUBLE)

  addSqlFunctionMethod(
    ATAN2,
    Seq(ANY_DEC_TYPE, ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.ATAN2_DEC_DEC)

  addSqlFunctionMethod(
    DEGREES,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.DEGREES)

  addSqlFunctionMethod(
    DEGREES,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.DEGREES_DEC)

  addSqlFunctionMethod(
    RADIANS,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.RADIANS)

  addSqlFunctionMethod(
    RADIANS,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.RADIANS_DEC)

  addSqlFunctionMethod(
    SIGN,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.SIGN_DOUBLE)

  addSqlFunctionMethod(
    SIGN,
    Seq(DataTypes.INT),
    DataTypes.INT,
    BuiltInMethods.SIGN_INT)

  addSqlFunctionMethod(
    SIGN,
    Seq(DataTypes.LONG),
    DataTypes.LONG,
    BuiltInMethods.SIGN_LONG)

  addSqlFunctionMethod2(
    SIGN,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.SIGN_DEC)
  // note: calcite: SIGN(Decimal(p,s)) => Decimal(p,s). may return e.g. 1.0000

  addSqlFunctionMethod(
    ScalarSqlFunctions.ROUND,
    Seq(DataTypes.LONG, DataTypes.INT),
    DataTypes.LONG,
    BuiltInMethods.ROUND_LONG)

  addSqlFunctionMethod(
    ScalarSqlFunctions.ROUND,
    Seq(DataTypes.INT, DataTypes.INT),
    DataTypes.INT,
    BuiltInMethods.ROUND_INT)

  addSqlFunctionMethod2(
    ScalarSqlFunctions.ROUND,
    Seq(ANY_DEC_TYPE, DataTypes.INT),
    BuiltInMethods.ROUND_DEC)

  addSqlFunctionMethod(
    ScalarSqlFunctions.ROUND,
    Seq(DataTypes.DOUBLE, DataTypes.INT),
    DataTypes.DOUBLE,
    BuiltInMethods.ROUND_DOUBLE)

  addSqlFunctionMethod(
    ScalarSqlFunctions.ROUND,
    Seq(DataTypes.LONG),
    DataTypes.LONG,
    BuiltInMethods.ROUND_LONG_0)

  addSqlFunctionMethod(
    ScalarSqlFunctions.ROUND,
    Seq(DataTypes.INT),
    DataTypes.INT,
    BuiltInMethods.ROUND_INT_0)

  addSqlFunctionMethod2(
    ScalarSqlFunctions.ROUND,
    Seq(ANY_DEC_TYPE),
    BuiltInMethods.ROUND_DEC_0)

  addSqlFunctionMethod(
    ScalarSqlFunctions.ROUND,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.ROUND_DOUBLE_0)

  addSqlFunction(
    ScalarSqlFunctions.PI,
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
    Seq(DataTypes.INT),
    new RandCallGen(isRandInteger = false, hasSeed = true))

  addSqlFunction(
    RAND_INTEGER,
    Seq(DataTypes.INT),
    new RandCallGen(isRandInteger = true, hasSeed = false))

  addSqlFunction(
    RAND_INTEGER,
    Seq(DataTypes.INT, DataTypes.INT),
    new RandCallGen(isRandInteger = true, hasSeed = true))

  addSqlFunctionMethod(
    ScalarSqlFunctions.LOG,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG)

  addSqlFunctionMethod(
    ScalarSqlFunctions.LOG,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG_DEC)

  addSqlFunctionMethod(
    ScalarSqlFunctions.LOG,
    Seq(DataTypes.DOUBLE, DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG_WITH_BASE)

  addSqlFunctionMethod(
    ScalarSqlFunctions.LOG,
    Seq(ANY_DEC_TYPE, ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG_WITH_BASE_DEC)

  addSqlFunctionMethod(
    ScalarSqlFunctions.LOG,
    Seq(ANY_DEC_TYPE, DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG_WITH_BASE_DEC_DOU)

  addSqlFunctionMethod(
    ScalarSqlFunctions.LOG,
    Seq(DataTypes.DOUBLE, ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG_WITH_BASE_DOU_DEC)

  addSqlFunctionMethod(
    ScalarSqlFunctions.HEX,
    Seq(DataTypes.LONG),
    DataTypes.STRING,
    BuiltInMethods.HEX_LONG)

  addSqlFunctionMethod(
    ScalarSqlFunctions.HEX,
    Seq(DataTypes.STRING),
    DataTypes.STRING,
    BuiltInMethods.HEX_STRING)

  // ----------------------------------------------------------------------------------------------
  // Temporal functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), DataTypes.LONG),
    DataTypes.LONG,
    BuiltInMethod.UNIX_DATE_EXTRACT.method)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), DataTypes.DATE),
    DataTypes.LONG,
    BuiltInMethod.UNIX_DATE_EXTRACT.method)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), DataTypes.TIME),
    DataTypes.LONG,
    BuiltInMethods.UNIX_TIME_EXTRACT)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), DataTypes.TIMESTAMP),
    DataTypes.LONG,
    BuiltInMethods.EXTRACT_FROM_TIMESTAMP)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), DataTypes.INTERVAL_MILLIS),
    DataTypes.LONG,
    BuiltInMethods.EXTRACT_FROM_DATE)

  addSqlFunctionMethod(
    EXTRACT,
    Seq(new GenericType(classOf[TimeUnitRange]), DataTypes.INTERVAL_MONTHS),
    DataTypes.LONG,
    BuiltInMethods.EXTRACT_YEAR_MONTH)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(
      new GenericType(classOf[TimeUnit]),
      DataTypes.TIMESTAMP,
      DataTypes.TIMESTAMP),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(new GenericType(classOf[TimeUnit]), DataTypes.TIMESTAMP, DataTypes.DATE),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(new GenericType(classOf[TimeUnit]), DataTypes.DATE, DataTypes.TIMESTAMP),
    new TimestampDiffCallGen)

  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(new GenericType(classOf[TimeUnit]), DataTypes.DATE, DataTypes.DATE),
    new TimestampDiffCallGen)

  addSqlFunction(
    FLOOR,
    Seq(DataTypes.DATE, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(DataTypes.TIME, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethod.UNIX_DATE_FLOOR.method)))

  addSqlFunction(
    FLOOR,
    Seq(DataTypes.TIMESTAMP, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.FLOOR.method,
      Some(BuiltInMethods.TIMESTAMP_FLOOR)))

  addSqlFunction(
    CEIL,
    Seq(DataTypes.DATE, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_DATE_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(DataTypes.TIME, new GenericType(classOf[TimeUnitRange])),
    new FloorCeilCallGen(
      BuiltInMethod.CEIL.method,
      Some(BuiltInMethod.UNIX_DATE_CEIL.method)))

  addSqlFunction(
    CEIL,
    Seq(DataTypes.TIMESTAMP, new GenericType(classOf[TimeUnitRange])),
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
    ScalarSqlFunctions.LOG2,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG2)

  addSqlFunctionMethod(
    ScalarSqlFunctions.LOG2,
    Seq(ANY_DEC_TYPE),
    DataTypes.DOUBLE,
    BuiltInMethods.LOG2_DEC)

  addSqlFunctionMethod(
    SINH,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.SINH)

  addSqlFunctionMethod(
    SINH,
    Seq(DecimalType.SYSTEM_DEFAULT),
    DataTypes.DOUBLE,
    BuiltInMethods.SINH_DEC)

  addSqlFunctionMethod(
    COSH,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.COSH)

  addSqlFunctionMethod(
    COSH,
    Seq(DecimalType.SYSTEM_DEFAULT),
    DataTypes.DOUBLE,
    BuiltInMethods.COSH_DEC)

  addSqlFunctionMethod(
    TANH,
    Seq(DataTypes.DOUBLE),
    DataTypes.DOUBLE,
    BuiltInMethods.TANH)

  addSqlFunctionMethod(
    TANH,
    Seq(DecimalType.SYSTEM_DEFAULT),
    DataTypes.DOUBLE,
    BuiltInMethods.TANH_DEC)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITAND,
    Seq(DataTypes.BYTE, DataTypes.BYTE),
    DataTypes.BYTE,
    BuiltInMethods.BITAND_BYTE)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITAND,
    Seq(DataTypes.SHORT, DataTypes.SHORT),
    DataTypes.SHORT,
    BuiltInMethods.BITAND_SHORT)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITAND,
    Seq(DataTypes.INT, DataTypes.INT),
    DataTypes.INT,
    BuiltInMethods.BITAND_INTEGER)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITAND,
    Seq(DataTypes.LONG, DataTypes.LONG),
    DataTypes.LONG,
    BuiltInMethods.BITAND_LONG)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITNOT,
    Seq(DataTypes.BYTE),
    DataTypes.BYTE,
    BuiltInMethods.BITNOT_BYTE)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITNOT,
    Seq(DataTypes.SHORT),
    DataTypes.SHORT,
    BuiltInMethods.BITNOT_SHORT)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITNOT,
    Seq(DataTypes.INT),
    DataTypes.INT,
    BuiltInMethods.BITNOT_INTEGER)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITNOT,
    Seq(DataTypes.LONG),
    DataTypes.LONG,
    BuiltInMethods.BITNOT_LONG)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITOR,
    Seq(DataTypes.BYTE, DataTypes.BYTE),
    DataTypes.BYTE,
    BuiltInMethods.BITOR_BYTE)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITOR,
    Seq(DataTypes.SHORT, DataTypes.SHORT),
    DataTypes.SHORT,
    BuiltInMethods.BITOR_SHORT)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITOR,
    Seq(DataTypes.INT, DataTypes.INT),
    DataTypes.INT,
    BuiltInMethods.BITOR_INTEGER)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITOR,
    Seq(DataTypes.LONG, DataTypes.LONG),
    DataTypes.LONG,
    BuiltInMethods.BITOR_LONG)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITXOR,
    Seq(DataTypes.BYTE, DataTypes.BYTE),
    DataTypes.BYTE,
    BuiltInMethods.BITXOR_BYTE)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITXOR,
    Seq(DataTypes.SHORT, DataTypes.SHORT),
    DataTypes.SHORT,
    BuiltInMethods.BITXOR_SHORT)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITXOR,
    Seq(DataTypes.INT, DataTypes.INT),
    DataTypes.INT,
    BuiltInMethods.BITXOR_INTEGER)

  addSqlFunctionMethod(
    ScalarSqlFunctions.BITXOR,
    Seq(DataTypes.LONG, DataTypes.LONG),
    DataTypes.LONG,
    BuiltInMethods.BITXOR_LONG)

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.STRING),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.BOOLEAN),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.BYTE),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.SHORT),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.INT),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.LONG),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.FLOAT),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.DOUBLE),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.CHAR),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.DATE),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.TIMESTAMP),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, DataTypes.TIME),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(DataTypes.STRING, ANY_DEC_TYPE),
    new PrintCallGen())

  addSqlFunction(
    ScalarSqlFunctions.PRINT,
    Seq(
      DataTypes.STRING,
      DataTypes.BYTE_ARRAY),
    new PrintCallGen())

  addSqlFunctionMethod(
    ScalarSqlFunctions.NOW,
    Seq(),
    DataTypes.LONG,
    BuiltInMethods.NOW)

  addSqlFunctionMethod(
    ScalarSqlFunctions.NOW,
    Seq(DataTypes.INT),
    DataTypes.LONG,
    BuiltInMethods.NOW_OFFSET)

  addSqlFunctionMethod(
    ScalarSqlFunctions.UNIX_TIMESTAMP,
    Seq(),
    DataTypes.LONG,
    BuiltInMethods.UNIX_TIMESTAMP)

  addSqlFunctionMethod(
    ScalarSqlFunctions.UNIX_TIMESTAMP,
    Seq(DataTypes.TIMESTAMP),
    DataTypes.LONG,
    BuiltInMethods.UNIX_TIMESTAMP_TS)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATEDIFF,
    Seq(DataTypes.TIMESTAMP,
      DataTypes.TIMESTAMP),
    DataTypes.INT,
    BuiltInMethods.DATEDIFF_T_T)

  addSqlFunction(
    ScalarSqlFunctions.IF,
    Seq(DataTypes.BOOLEAN, DataTypes.STRING, DataTypes.STRING),
    new IfCallGen())

  addSqlFunction(
    ScalarSqlFunctions.IF,
    Seq(DataTypes.BOOLEAN, DataTypes.BOOLEAN, DataTypes.BOOLEAN),
    new IfCallGen())

  // This sequence must be in sync with [[NumericOrDefaultReturnTypeInference]]
  val numericTypes = Seq(
    DataTypes.BYTE,
    DataTypes.SHORT,
    DataTypes.INT,
    DataTypes.LONG,
    DecimalType.SYSTEM_DEFAULT,
    DataTypes.FLOAT,
    DataTypes.DOUBLE)

  for (t1 <- numericTypes) {
    for (t2 <- numericTypes) {
      addSqlFunction(
        ScalarSqlFunctions.IF,
        Seq(DataTypes.BOOLEAN, t1, t2),
        new IfCallGen())
    }
  }

  addSqlFunction(
    ScalarSqlFunctions.IF,
    Seq(DataTypes.BOOLEAN, DataTypes.CHAR, DataTypes.CHAR),
    new IfCallGen())

  addSqlFunction(
    ScalarSqlFunctions.IF,
    Seq(DataTypes.BOOLEAN, DataTypes.DATE, DataTypes.DATE),
    new IfCallGen())

  addSqlFunction(
    ScalarSqlFunctions.IF,
    Seq(DataTypes.BOOLEAN, DataTypes.TIMESTAMP, DataTypes.TIMESTAMP),
    new IfCallGen())

  addSqlFunction(
    ScalarSqlFunctions.IF,
    Seq(DataTypes.BOOLEAN, DataTypes.TIME, DataTypes.TIME),
    new IfCallGen())

  addSqlFunction(
    ScalarSqlFunctions.IF,
    Seq(
      DataTypes.BOOLEAN,
      DataTypes.BYTE_ARRAY,
      DataTypes.BYTE_ARRAY),
    new IfCallGen())

  addSqlFunctionMethod(
    ScalarSqlFunctions.DIV_INT,
    Seq(DataTypes.INT, DataTypes.INT),
    DataTypes.INT,
    BuiltInMethods.DIV_INT)

  addSqlFunction(
    DIV,
    Seq(ANY_DEC_TYPE, DataTypes.DOUBLE),
    new DivCallGen()
  )

  addSqlFunction(
    DIV,
    Seq(ANY_DEC_TYPE, ANY_DEC_TYPE),
    new DivCallGen()
  )

  addSqlFunction(
    DIV,
    Seq(DataTypes.DOUBLE, DataTypes.DOUBLE),
    new DivCallGen()
  )

  addSqlFunction(
    DIV,
    Seq(DataTypes.DOUBLE, ANY_DEC_TYPE),
    new DivCallGen()
  )

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.BOOLEAN),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.BYTE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.SHORT),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.INT),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.LONG),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.FLOAT),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.DOUBLE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.CHAR),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.DATE),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.TIME),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.TIMESTAMP),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DataTypes.BYTE_ARRAY),
    new HashCodeCallGen())

  addSqlFunction(
    HASH_CODE,
    Seq(DecimalType.SYSTEM_DEFAULT),
    new HashCodeCallGen())

  addSqlFunctionMethod(
    ScalarSqlFunctions.TO_DATE,
    Seq(DataTypes.INT),
    DataTypes.DATE,
    BuiltInMethods.INT_TO_DATE)

  DataTypes.INTEGRAL_TYPES foreach (
    dt => addSqlFunctionMethod(ScalarSqlFunctions.TO_TIMESTAMP,
      Seq(dt),
      DataTypes.TIMESTAMP,
      BuiltInMethods.LONG_TO_TIMESTAMP))

  DataTypes.FRACTIONAL_TYPES foreach (
    dt => addSqlFunctionMethod(ScalarSqlFunctions.TO_TIMESTAMP,
      Seq(dt),
      DataTypes.TIMESTAMP,
      BuiltInMethods.DOUBLE_TO_TIMESTAMP))

  addSqlFunctionMethod(ScalarSqlFunctions.TO_TIMESTAMP,
    Seq(DecimalType.SYSTEM_DEFAULT),
    DataTypes.TIMESTAMP,
    BuiltInMethods.DECIMAL_TO_TIMESTAMP)

  addSqlFunctionMethod(
    ScalarSqlFunctions.FROM_TIMESTAMP,
    Seq(DataTypes.TIMESTAMP),
    DataTypes.LONG,
    BuiltInMethods.TIMESTAMP_TO_BIGINT)

  // Date/Time & BinaryString Converting -- start
  addSqlFunctionMethod(
    ScalarSqlFunctions.TO_DATE,
    Seq(DataTypes.STRING),
    DataTypes.DATE,
    BuiltInMethod.STRING_TO_DATE.method)

  addSqlFunctionMethod(
    ScalarSqlFunctions.TO_DATE,
    Seq(DataTypes.STRING, DataTypes.STRING),
    DataTypes.DATE,
    BuiltInMethods.STRING_TO_DATE_WITH_FORMAT)

  addSqlFunctionMethod(
    ScalarSqlFunctions.TO_TIMESTAMP,
    Seq(DataTypes.STRING),
    DataTypes.TIMESTAMP,
    BuiltInMethods.STRING_TO_TIMESTAMP)

  addSqlFunctionMethod(
    ScalarSqlFunctions.TO_TIMESTAMP,
    Seq(DataTypes.STRING, DataTypes.STRING),
    DataTypes.TIMESTAMP,
    BuiltInMethods.STRING_TO_TIMESTAMP_WITH_FORMAT)

  addSqlFunctionMethod(
    ScalarSqlFunctions.UNIX_TIMESTAMP,
    Seq(DataTypes.STRING),
    DataTypes.LONG,
    BuiltInMethods.UNIX_TIMESTAMP_STR)

  addSqlFunctionMethod(
    ScalarSqlFunctions.UNIX_TIMESTAMP,
    Seq(DataTypes.STRING, DataTypes.STRING),
    DataTypes.LONG,
    BuiltInMethods.UNIX_TIMESTAMP_FORMAT)

  DataTypes.INTEGRAL_TYPES foreach (
    dt => addSqlFunctionMethod(
      ScalarSqlFunctions.FROM_UNIXTIME,
      Seq(dt),
      DataTypes.STRING,
      BuiltInMethods.FROM_UNIXTIME))

  DataTypes.FRACTIONAL_TYPES foreach (
    dt => addSqlFunctionMethod(
      ScalarSqlFunctions.FROM_UNIXTIME,
      Seq(dt),
      DataTypes.STRING,
      BuiltInMethods.FROM_UNIXTIME_AS_DOUBLE))

  addSqlFunctionMethod(
    ScalarSqlFunctions.FROM_UNIXTIME,
    Seq(DecimalType.SYSTEM_DEFAULT),
    DataTypes.STRING,
    BuiltInMethods.FROM_UNIXTIME_AS_DECIMAL)

  addSqlFunctionMethod(
    ScalarSqlFunctions.FROM_UNIXTIME,
    Seq(DataTypes.LONG, DataTypes.STRING),
    DataTypes.STRING,
    BuiltInMethods.FROM_UNIXTIME_FORMAT)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATEDIFF,
    Seq(DataTypes.TIMESTAMP, DataTypes.STRING),
    DataTypes.INT,
    BuiltInMethods.DATEDIFF_T_S)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATEDIFF,
    Seq(DataTypes.STRING, DataTypes.TIMESTAMP),
    DataTypes.INT,
    BuiltInMethods.DATEDIFF_S_T)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATEDIFF,
    Seq(DataTypes.STRING, DataTypes.STRING),
    DataTypes.INT,
    BuiltInMethods.DATEDIFF_S_S)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATE_FORMAT,
    Seq(DataTypes.TIMESTAMP, DataTypes.STRING),
    DataTypes.STRING,
    BuiltInMethods.DATE_FORMAT_LONG_STRING)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATE_FORMAT,
    Seq(DataTypes.STRING, DataTypes.STRING),
    DataTypes.STRING,
    BuiltInMethods.DATE_FORMAT_STIRNG_STRING)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATE_FORMAT,
    Seq(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING),
    DataTypes.STRING,
    BuiltInMethods.DATE_FORMAT_STRING_STRING_STRING)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATE_SUB,
    Seq(DataTypes.STRING, DataTypes.INT),
    DataTypes.STRING,
    BuiltInMethods.DATE_SUB_S)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATE_SUB,
    Seq(DataTypes.TIMESTAMP, DataTypes.INT),
    DataTypes.STRING,
    BuiltInMethods.DATE_SUB_T)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATE_ADD,
    Seq(DataTypes.STRING, DataTypes.INT),
    DataTypes.STRING,
    BuiltInMethods.DATE_ADD_S)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATE_ADD,
    Seq(DataTypes.TIMESTAMP, DataTypes.INT),
    DataTypes.STRING,
    BuiltInMethods.DATE_ADD_T)

  addSqlFunctionMethod(
    ScalarSqlFunctions.TO_TIMESTAMP_TZ,
    Seq(DataTypes.STRING, DataTypes.STRING),
    DataTypes.TIMESTAMP,
    BuiltInMethods.STRING_TO_TIMESTAMP_TZ)

  addSqlFunctionMethod(
    ScalarSqlFunctions.TO_TIMESTAMP_TZ,
    Seq(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING),
    DataTypes.TIMESTAMP,
    BuiltInMethods.STRING_TO_TIMESTAMP_FORMAT_TZ)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATE_FORMAT_TZ,
    Seq(DataTypes.TIMESTAMP, DataTypes.STRING),
    DataTypes.STRING,
    BuiltInMethods.DATE_FORMAT_LONG_ZONE)

  addSqlFunctionMethod(
    ScalarSqlFunctions.DATE_FORMAT_TZ,
    Seq(DataTypes.TIMESTAMP, DataTypes.STRING, DataTypes.STRING),
    DataTypes.STRING,
    BuiltInMethods.DATE_FORMAT_LONG_STRING_ZONE)

  addSqlFunctionMethod(
    ScalarSqlFunctions.CONVERT_TZ,
    Seq(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING),
    DataTypes.STRING,
    BuiltInMethods.CONVERT_TZ)

  addSqlFunctionMethod(
    ScalarSqlFunctions.CONVERT_TZ,
    Seq(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING, DataTypes.STRING),
    DataTypes.STRING,
    BuiltInMethods.CONVERT_FORMAT_TZ)
  // Date/Time & BinaryString Converting -- end

  timeZoneFunctions += (
    BuiltInMethods.TIMESTAMP_TO_STRING,
    BuiltInMethods.DATE_FORMAT_STIRNG_STRING,
    BuiltInMethods.DATE_FORMAT_LONG_STRING,
    BuiltInMethods.DATE_FORMAT_STRING_STRING_STRING,
    BuiltInMethods.EXTRACT_FROM_TIMESTAMP,
    BuiltInMethods.TIMESTAMP_FLOOR,
    BuiltInMethods.TIMESTAMP_CEIL,
    BuiltInMethods.UNIX_TIMESTAMP_STR,
    BuiltInMethods.UNIX_TIMESTAMP_FORMAT,
    BuiltInMethods.FROM_UNIXTIME,
    BuiltInMethods.FROM_UNIXTIME_FORMAT,
    BuiltInMethods.FROM_UNIXTIME_AS_DOUBLE,
    BuiltInMethods.FROM_UNIXTIME_AS_DECIMAL,
    BuiltInMethods.DATEDIFF_T_S,
    BuiltInMethods.DATEDIFF_S_T,
    BuiltInMethods.DATEDIFF_S_S,
    BuiltInMethods.DATEDIFF_T_T,
    BuiltInMethods.DATE_SUB_S,
    BuiltInMethods.DATE_SUB_T,
    BuiltInMethods.DATE_ADD_S,
    BuiltInMethods.DATE_ADD_T,
    BuiltInMethods.STRING_TO_TIMESTAMP,
    BuiltInMethods.STRING_TO_TIMESTAMP_WITH_FORMAT
  )
  
  // ----------------------------------------------------------------------------------------------

  /**
    * if the method need the timezone parameter
    * @param m
    * @return
    */
  def isFunctionWithTimeZone(m: Method): Boolean = {
    timeZoneFunctions.contains(m)
  }

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

    // user-defined scalar function
    case ssf: ScalarSqlFunction =>
      Some(new ScalarFunctionCallGen(ssf.getScalarFunction))

    // user-defined table function
    case tsf: TableSqlFunction =>
      Some(new TableFunctionCallGen(tsf.getTableFunction))

    // built-in scalar function
    case _ =>
      sqlFunctions.get((sqlOperator, operandTypes))
        .orElse(sqlFunctions.find(entry => entry._1._1 == sqlOperator
          && entry._1._2.length == operandTypes.length
          && entry._1._2.zip(operandTypes).forall {
          case (x: DecimalType, y: DecimalType) => true
          case (x: PrimitiveType, y: PrimitiveType) => TypeUtils.shouldAutoCastTo(y, x) || x == y
          case (x: InternalType, y: InternalType) => x == y
          case _ => false
        }).map(_._2))

  }

  // ----------------------------------------------------------------------------------------------

  // TODO: returnType isn't necessary here; it will be inferred by someone else
  //       and passed to generate(). Refactor to remove `returnType` parameter.
  //       see addSqlFunctionMethod2()
  private def addSqlFunctionMethod(
      sqlOperator: SqlOperator,
      operandTypes: Seq[InternalType],
      returnType: InternalType,
      method: Method,
      argNotNull: Boolean = true)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = new MethodCallGen(method, argNotNull)
  }

  private def addSqlFunctionMethod2(
      sqlOperator: SqlOperator,
      operandTypes: Seq[InternalType],
      method: Method,
      argNotNull: Boolean = true)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = new MethodCallGen(method, argNotNull)
  }

  private def addSqlFunctionNotMethod(
      sqlOperator: SqlOperator,
      operandTypes: Seq[InternalType],
      method: Method)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) =
      new NotCallGen(new MethodCallGen(method))
  }

  private def addSqlFunction(
      sqlOperator: SqlOperator,
      operandTypes: Seq[InternalType],
      callGenerator: CallGenerator)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = callGenerator
  }

}
