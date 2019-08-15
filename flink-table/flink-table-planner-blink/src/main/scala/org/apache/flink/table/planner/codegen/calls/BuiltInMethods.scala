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

import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.runtime.functions._

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.linq4j.tree.Types
import org.apache.calcite.runtime.SqlFunctions

import java.lang.reflect.Method
import java.lang.{Byte => JByte, Integer => JInteger, Long => JLong, Short => JShort}
import java.util.TimeZone

object BuiltInMethods {

  // ARITHMETIC FUNCTIONS

  val DIV_INT = Types.lookupMethod(
      classOf[SqlFunctionUtils],
      "divideInt",
      classOf[Int], classOf[Int])

  // LOG FUNCTIONS

  val LOG = Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Double])

  val LOG_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Decimal])

  val LOG_WITH_BASE =
    Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Double], classOf[Double])

  val LOG_WITH_BASE_DOU_DEC =
    Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Double], classOf[Decimal])

  val LOG_WITH_BASE_DEC_DOU =
    Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Decimal], classOf[Double])

  val LOG_WITH_BASE_DEC =
    Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Decimal], classOf[Decimal])

  val LOG10 = Types.lookupMethod(classOf[SqlFunctionUtils], "log10", classOf[Double])

  val LOG10_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "log10", classOf[Decimal])

  val LOG2 = Types.lookupMethod(classOf[SqlFunctionUtils], "log2", classOf[Double])

  val LOG2_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "log2", classOf[Decimal])

  // EXPR FUNCTIONS

  val EXP = Types.lookupMethod(classOf[Math], "exp", classOf[Double])

  val EXP_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "exp", classOf[Decimal])

  // POWER FUNCTIONS

  val POWER_NUM_NUM = Types.lookupMethod(classOf[Math], "pow", classOf[Double], classOf[Double])
  val POWER_NUM_DEC = Types.lookupMethod(
    classOf[SqlFunctionUtils], "power", classOf[Double], classOf[Decimal])
  val POWER_DEC_DEC = Types.lookupMethod(
    classOf[SqlFunctionUtils], "power", classOf[Decimal], classOf[Decimal])
  val POWER_DEC_NUM = Types.lookupMethod(
    classOf[SqlFunctionUtils], "power", classOf[Decimal], classOf[Double])


  // TRIGONOMETRIC FUNCTIONS

  val LN = Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Double])

  val LN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Decimal])

  val ABS = Types.lookupMethod(classOf[SqlFunctions], "abs", classOf[Double])
  val ABS_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "abs", classOf[Decimal])

  val FLOOR_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "floor", classOf[Decimal])
  val CEIL_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "ceil", classOf[Decimal])

  val SIN = Types.lookupMethod(classOf[Math], "sin", classOf[Double])
  val SIN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "sin", classOf[Decimal])

  val COS = Types.lookupMethod(classOf[Math], "cos", classOf[Double])
  val COS_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "cos", classOf[Decimal])

  val TAN = Types.lookupMethod(classOf[Math], "tan", classOf[Double])
  val TAN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "tan", classOf[Decimal])

  val COT = Types.lookupMethod(classOf[SqlFunctions], "cot", classOf[Double])
  val COT_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "cot", classOf[Decimal])

  val ASIN = Types.lookupMethod(classOf[Math], "asin", classOf[Double])
  val ASIN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "asin", classOf[Decimal])

  val ACOS = Types.lookupMethod(classOf[Math], "acos", classOf[Double])
  val ACOS_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "acos", classOf[Decimal])

  val ATAN = Types.lookupMethod(classOf[Math], "atan", classOf[Double])
  val ATAN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "atan", classOf[Decimal])

  val SINH = Types.lookupMethod(classOf[Math], "sinh", classOf[Double])
  val SINH_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "sinh", classOf[Decimal])

  val COSH = Types.lookupMethod(classOf[Math], "cosh", classOf[Double])
  val COSH_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "cosh", classOf[Decimal])

  val TANH = Types.lookupMethod(classOf[Math], "tanh", classOf[Double])
  val TANH_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "tanh", classOf[Decimal])

  val ATAN2_DOUBLE_DOUBLE = Types.lookupMethod(
    classOf[Math],
    "atan2",
    classOf[Double],
    classOf[Double])
  val ATAN2_DEC_DEC = Types.lookupMethod(
    classOf[SqlFunctionUtils],
    "atan2",
    classOf[Decimal],
    classOf[Decimal])

  val DEGREES = Types.lookupMethod(classOf[Math], "toDegrees", classOf[Double])
  val DEGREES_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "degrees", classOf[Decimal])

  val RADIANS = Types.lookupMethod(classOf[Math], "toRadians", classOf[Double])
  val RADIANS_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "radians", classOf[Decimal])

  val SIGN_DOUBLE = Types.lookupMethod(classOf[Math], "signum", classOf[Double])
  val SIGN_INT = Types.lookupMethod(classOf[Integer], "signum", classOf[Int])
  val SIGN_LONG = Types.lookupMethod(classOf[JLong], "signum", classOf[Long])
  val SIGN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "sign", classOf[Decimal])

  val ROUND_DOUBLE = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[Double],
    classOf[Int])
  val ROUND_INT = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[Int], classOf[Int])
  val ROUND_LONG = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[Long], classOf[Int])
  val ROUND_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "sround",
    classOf[Decimal], classOf[Int])

  val ROUND_DOUBLE_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Double])
  val ROUND_INT_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Int])
  val ROUND_LONG_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Long])
  val ROUND_DEC_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Decimal])

  // MISC

  val HEX_LONG: Method = Types.lookupMethod(classOf[SqlFunctionUtils], "hex", classOf[Long])
  val HEX_STRING: Method = Types.lookupMethod(classOf[SqlFunctionUtils], "hex", classOf[String])

  val BIN = Types.lookupMethod(classOf[JLong], "toBinaryString", classOf[Long])

  // BIT FUNCTIONS

  val BITAND_BYTE =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitAnd", classOf[JByte], classOf[JByte])
  val BITAND_SHORT =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitAnd", classOf[JShort], classOf[JShort])
  val BITAND_INTEGER =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitAnd", classOf[JInteger],
      classOf[JInteger])
  val BITAND_LONG =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitAnd", classOf[JLong], classOf[JLong])

  val BITNOT_BYTE =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitNot", classOf[JByte])
  val BITNOT_SHORT =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitNot", classOf[JShort])
  val BITNOT_INTEGER =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitNot", classOf[JInteger])
  val BITNOT_LONG =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitNot", classOf[JLong])

  val BITOR_BYTE =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitOr", classOf[JByte], classOf[JByte])
  val BITOR_SHORT =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitOr", classOf[JShort], classOf[JShort])
  val BITOR_INTEGER =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitOr", classOf[JInteger],
      classOf[JInteger])
  val BITOR_LONG =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitOr", classOf[JLong], classOf[JLong])

  val BITXOR_BYTE =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitXor", classOf[JByte], classOf[JByte])
  val BITXOR_SHORT =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitXor", classOf[JShort], classOf[JShort])
  val BITXOR_INTEGER =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitXor", classOf[JInteger],
      classOf[JInteger])
  val BITXOR_LONG =
    Types.lookupMethod(classOf[SqlFunctionUtils], "bitXor", classOf[JLong], classOf[JLong])


  // SQL DATE TIME FUNCTIONS

  val UNIX_TIME_TO_STRING = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "unixTimeToString",
    classOf[Int])

  val TIMESTAMP_TO_STRING = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "timestampToString",
    classOf[Long], classOf[Int])

  val TIMESTAMP_TO_STRING_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "timestampToString",
    classOf[Long], classOf[Int], classOf[TimeZone])

  val TIMESTAMP_TO_TIMESTAMP_WITH_LOCAL_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "timestampToTimestampWithLocalZone",
    classOf[Long], classOf[TimeZone])

  val TIMESTAMP_WITH_LOCAL_ZONE_TO_TIMESTAMP = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "timestampWithLocalZoneToTimestamp",
    classOf[Long], classOf[TimeZone])

  val STRING_TO_DATE_WITH_FORMAT = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "strToDate",
    classOf[String], classOf[String])

  val NOW = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "now")

  val DATE_FORMAT_STRING_STRING_STRING_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateFormat", classOf[String],
    classOf[String], classOf[String], classOf[TimeZone])

  val DATE_FORMAT_STIRNG_STRING_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateFormat", classOf[String], classOf[String], classOf[TimeZone])

  val DATE_FORMAT_LONG_STRING_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateFormat", classOf[Long], classOf[String], classOf[TimeZone])

  val DATE_FORMAT_STIRNG_STRING = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateFormat", classOf[String], classOf[String])

  val DATE_FORMAT_LONG_STRING = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateFormat", classOf[Long], classOf[String])

  val UNIX_TIMESTAMP_FORMAT = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "unixTimestamp",
    classOf[String],
    classOf[String],
    classOf[TimeZone])

  val UNIX_TIMESTAMP_STR = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "unixTimestamp", classOf[String], classOf[TimeZone])

  val UNIX_TIMESTAMP = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "unixTimestamp")

  val UNIX_TIMESTAMP_TS = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "unixTimestamp", classOf[Long])

  val FROM_UNIXTIME_FORMAT = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "fromUnixtime", classOf[Long], classOf[String], classOf[TimeZone])

  val FROM_UNIXTIME = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "fromUnixtime", classOf[Long], classOf[TimeZone])

  val DATEDIFF_T_S_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateDiff", classOf[Long], classOf[String], classOf[TimeZone])

  val DATEDIFF_S_S_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateDiff", classOf[String], classOf[String], classOf[TimeZone])

  val DATEDIFF_S_T_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateDiff", classOf[String], classOf[Long], classOf[TimeZone])

  val DATEDIFF_T_T_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateDiff", classOf[Long], classOf[Long], classOf[TimeZone])

  val DATEDIFF_T_S = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateDiff", classOf[Long], classOf[String])

  val DATEDIFF_S_S = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateDiff", classOf[String], classOf[String])

  val DATEDIFF_S_T = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateDiff", classOf[String], classOf[Long])

  val DATEDIFF_T_T = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateDiff", classOf[Long], classOf[Long])

  val DATE_SUB_S_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateSub", classOf[String], classOf[Int], classOf[TimeZone])

  val DATE_SUB_T_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateSub", classOf[Long], classOf[Int], classOf[TimeZone])

  val DATE_SUB_S = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateSub", classOf[String], classOf[Int])

  val DATE_SUB_T = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateSub", classOf[Long], classOf[Int])

  val DATE_ADD_S_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateAdd", classOf[String], classOf[Int], classOf[TimeZone])

  val DATE_ADD_T_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateAdd", classOf[Long], classOf[Int], classOf[TimeZone])

  val DATE_ADD_S = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateAdd", classOf[String], classOf[Int])

  val DATE_ADD_T = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateAdd", classOf[Long], classOf[Int])

  val LONG_TO_TIMESTAMP = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "toTimestamp",
    classOf[Long])

  val DOUBLE_TO_TIMESTAMP = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "toTimestamp",
    classOf[Double])

  val DECIMAL_TO_TIMESTAMP = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "toTimestamp",
    classOf[Decimal])

  val STRING_TO_TIMESTAMP = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "toTimestamp",
    classOf[String])

  val STRING_TO_TIMESTAMP_WITH_FORMAT = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "toTimestamp",
    classOf[String], classOf[String])

  val STRING_TO_TIMESTAMP_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "toTimestamp",
    classOf[String], classOf[TimeZone])

  val STRING_TO_TIMESTAMP_WITH_FORMAT_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "toTimestamp",
    classOf[String], classOf[String], classOf[TimeZone])

  val TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "timestampWithLocalZoneToDate",
    classOf[Long], classOf[TimeZone])

  val TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "timestampWithLocalZoneToTime",
    classOf[Long], classOf[TimeZone])

  val DATE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "dateToTimestampWithLocalZone",
    classOf[Int], classOf[TimeZone])

  val TIME_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "timeToTimestampWithLocalZone",
    classOf[Int], classOf[TimeZone])

  val TIMESTAMP_TO_BIGINT = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "fromTimestamp",
    classOf[Long])

  val EXTRACT_FROM_TIMESTAMP_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "extractFromTimestamp",
    classOf[TimeUnitRange], classOf[Long], classOf[TimeZone])

  val EXTRACT_FROM_DATE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "extractFromDate",
    classOf[TimeUnitRange], classOf[Long])

  val UNIX_TIME_EXTRACT = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "unixTimeExtract",
    classOf[TimeUnitRange], classOf[Int])

  val EXTRACT_YEAR_MONTH = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "extractYearMonth",
    classOf[TimeUnitRange], classOf[Int])

  val TIMESTAMP_FLOOR_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "timestampFloor",
    classOf[TimeUnitRange], classOf[Long], classOf[TimeZone])

  val TIMESTAMP_CEIL_TIME_ZONE = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "timestampCeil",
    classOf[TimeUnitRange], classOf[Long], classOf[TimeZone])

  val CONVERT_TZ = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "convertTz",
    classOf[String], classOf[String], classOf[String])

  val CONVERT_FORMAT_TZ = Types.lookupMethod(
    classOf[SqlDateTimeUtils],
    "convertTz",
    classOf[String], classOf[String], classOf[String], classOf[String])

  val STRING_TO_DATE = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "dateStringToUnixDate", classOf[String])

  val STRING_TO_TIME = Types.lookupMethod(
    classOf[SqlDateTimeUtils], "timeStringToUnixDate", classOf[String])

  val TRUNCATE_DOUBLE_ONE = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Double])
  val TRUNCATE_FLOAT_ONE = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Float])
  val TRUNCATE_INT_ONE = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Int])
  val TRUNCATE_LONG_ONE = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Long])
  val TRUNCATE_DEC_ONE = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Decimal])

  val TRUNCATE_DOUBLE = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Double], classOf[Int])
  val TRUNCATE_FLOAT = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Float], classOf[Int])
  val TRUNCATE_INT = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Int], classOf[Int])
  val TRUNCATE_LONG = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Long], classOf[Int])
  val TRUNCATE_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Decimal], classOf[Int])
}
