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

import org.apache.flink.table.api.{JsonExistsOnError, JsonQueryOnEmptyOrError, JsonQueryWrapper, JsonValueOnEmptyOrError}
import org.apache.flink.table.data.binary.{BinaryStringData, BinaryStringDataUtil}
import org.apache.flink.table.data.{DecimalData, DecimalDataUtils, TimestampData}
import org.apache.flink.table.functions.SqlLikeUtils
import org.apache.flink.table.runtime.functions._
import org.apache.flink.table.utils.DateTimeUtils
import org.apache.flink.table.utils.DateTimeUtils.TimeUnitRange

import org.apache.calcite.linq4j.tree.Types

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

  val LOG_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[DecimalData])

  val LOG_WITH_BASE =
    Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Double], classOf[Double])

  val LOG_WITH_BASE_DOU_DEC =
    Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Double], classOf[DecimalData])

  val LOG_WITH_BASE_DEC_DOU =
    Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[DecimalData], classOf[Double])

  val LOG_WITH_BASE_DEC =
    Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[DecimalData], classOf[DecimalData])

  val LOG10 = Types.lookupMethod(classOf[SqlFunctionUtils], "log10", classOf[Double])

  val LOG10_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "log10", classOf[DecimalData])

  val LOG2 = Types.lookupMethod(classOf[SqlFunctionUtils], "log2", classOf[Double])

  val LOG2_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "log2", classOf[DecimalData])

  // EXPR FUNCTIONS

  val EXP = Types.lookupMethod(classOf[Math], "exp", classOf[Double])

  val EXP_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "exp", classOf[DecimalData])

  // POWER FUNCTIONS

  val POWER_NUM_NUM = Types.lookupMethod(classOf[Math], "pow", classOf[Double], classOf[Double])
  val POWER_NUM_DEC = Types.lookupMethod(
    classOf[SqlFunctionUtils], "power", classOf[Double], classOf[DecimalData])
  val POWER_DEC_DEC = Types.lookupMethod(
    classOf[SqlFunctionUtils], "power", classOf[DecimalData], classOf[DecimalData])
  val POWER_DEC_NUM = Types.lookupMethod(
    classOf[SqlFunctionUtils], "power", classOf[DecimalData], classOf[Double])

  // TRIGONOMETRIC FUNCTIONS

  val LN = Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[Double])

  val LN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "log", classOf[DecimalData])

  val ABS = Types.lookupMethod(classOf[SqlFunctionUtils], "abs", classOf[Double])
  val ABS_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "abs", classOf[DecimalData])

  val FLOOR = Types.lookupMethod(classOf[SqlFunctionUtils], "floor", classOf[Double])
  val FLOOR_INTEGRAL = Types.lookupMethod(classOf[SqlFunctionUtils], "floor", classOf[Int],
    classOf[Int])
  val FLOOR_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "floor", classOf[DecimalData])
  val CEIL = Types.lookupMethod(classOf[SqlFunctionUtils], "ceil", classOf[Double])
  val CEIL_INTEGRAL = Types.lookupMethod(classOf[SqlFunctionUtils], "ceil", classOf[Int],
    classOf[Int])
  val CEIL_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "ceil", classOf[DecimalData])

  val SIN = Types.lookupMethod(classOf[Math], "sin", classOf[Double])
  val SIN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "sin", classOf[DecimalData])

  val COS = Types.lookupMethod(classOf[Math], "cos", classOf[Double])
  val COS_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "cos", classOf[DecimalData])

  val TAN = Types.lookupMethod(classOf[Math], "tan", classOf[Double])
  val TAN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "tan", classOf[DecimalData])

  val COT = Types.lookupMethod(classOf[SqlFunctionUtils], "cot", classOf[Double])
  val COT_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "cot", classOf[DecimalData])

  val ASIN = Types.lookupMethod(classOf[Math], "asin", classOf[Double])
  val ASIN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "asin", classOf[DecimalData])

  val ACOS = Types.lookupMethod(classOf[Math], "acos", classOf[Double])
  val ACOS_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "acos", classOf[DecimalData])

  val ATAN = Types.lookupMethod(classOf[Math], "atan", classOf[Double])
  val ATAN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "atan", classOf[DecimalData])

  val SINH = Types.lookupMethod(classOf[Math], "sinh", classOf[Double])
  val SINH_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "sinh", classOf[DecimalData])

  val COSH = Types.lookupMethod(classOf[Math], "cosh", classOf[Double])
  val COSH_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "cosh", classOf[DecimalData])

  val TANH = Types.lookupMethod(classOf[Math], "tanh", classOf[Double])
  val TANH_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "tanh", classOf[DecimalData])

  val ATAN2_DOUBLE_DOUBLE = Types.lookupMethod(
    classOf[Math],
    "atan2",
    classOf[Double],
    classOf[Double])
  val ATAN2_DEC_DEC = Types.lookupMethod(
    classOf[SqlFunctionUtils],
    "atan2",
    classOf[DecimalData],
    classOf[DecimalData])

  val DEGREES = Types.lookupMethod(classOf[Math], "toDegrees", classOf[Double])
  val DEGREES_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "degrees", classOf[DecimalData])

  val RADIANS = Types.lookupMethod(classOf[Math], "toRadians", classOf[Double])
  val RADIANS_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "radians", classOf[DecimalData])

  val SIGN_DOUBLE = Types.lookupMethod(classOf[Math], "signum", classOf[Double])
  val SIGN_INT = Types.lookupMethod(classOf[Integer], "signum", classOf[Int])
  val SIGN_LONG = Types.lookupMethod(classOf[JLong], "signum", classOf[Long])
  val SIGN_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "sign", classOf[DecimalData])

  val ROUND_DOUBLE = Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Double],
    classOf[Int])
  val ROUND_INT = Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Int],
    classOf[Int])
  val ROUND_LONG = Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Long],
    classOf[Int])
  val ROUND_BYTE = Types.lookupMethod(classOf[SqlFunctionUtils], "sround",
    classOf[Byte], classOf[Int])
  val ROUND_SHORT = Types.lookupMethod(classOf[SqlFunctionUtils], "sround",
    classOf[Short], classOf[Int])
  val ROUND_FLOAT = Types.lookupMethod(classOf[SqlFunctionUtils], "sround",
    classOf[Float], classOf[Int])
  val ROUND_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "sround",
    classOf[DecimalData], classOf[Int])

  val ROUND_DOUBLE_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Double])
  val ROUND_INT_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Int])
  val ROUND_LONG_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Long])
  val ROUND_BYTE_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Byte])
  val ROUND_SHORT_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Short])
  val ROUND_FLOAT_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[Float])
  val ROUND_DEC_0 =
    Types.lookupMethod(classOf[SqlFunctionUtils], "sround", classOf[DecimalData])

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
    classOf[DateTimeUtils],
    "formatTimestampMillis",
    classOf[Int], classOf[Int])

  val UNIX_DATE_TO_STRING = Types.lookupMethod(
    classOf[DateTimeUtils],
    "formatDate",
    classOf[Int])

  val INTERVAL_YEAR_MONTH_TO_STRING = Types.lookupMethod(
    classOf[DateTimeUtils],
    "formatIntervalYearMonth",
    classOf[Int])

  val INTERVAL_DAY_TIME_TO_STRING = Types.lookupMethod(
    classOf[DateTimeUtils],
    "formatIntervalDayTime",
    classOf[Long])

  val TIMESTAMP_TO_STRING_TIME_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "formatTimestamp",
    classOf[TimestampData], classOf[TimeZone], classOf[Int])

  val TIMESTAMP_TO_TIMESTAMP_WITH_LOCAL_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "timestampToTimestampWithLocalZone",
    classOf[TimestampData], classOf[TimeZone])

  val TIMESTAMP_WITH_LOCAL_ZONE_TO_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeUtils],
    "timestampWithLocalZoneToTimestamp",
    classOf[TimestampData], classOf[TimeZone])

  val STRING_TO_DATE_WITH_FORMAT = Types.lookupMethod(
    classOf[DateTimeUtils],
    "parseDate",
    classOf[String], classOf[String])

  val FORMAT_TIMESTAMP_STRING_STRING_STRING_TIME_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "formatTimestampString",
    classOf[String],
    classOf[String],
    classOf[String],
    classOf[TimeZone])

  val FORMAT_TIMESTAMP_STRING_STRING_TIME_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "formatTimestampString",
    classOf[String],
    classOf[String],
    classOf[TimeZone])

  val FORMAT_TIMESTAMP_STRING_FORMAT_STRING_STRING = Types.lookupMethod(
    classOf[DateTimeUtils],
    "formatTimestampString",
    classOf[String],
    classOf[String])

  val FORMAT_TIMESTAMP_DATA = Types.lookupMethod(
    classOf[DateTimeUtils], "formatTimestamp", classOf[TimestampData], classOf[String])

  val FORMAT_TIMESTAMP_DATA_WITH_TIME_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "formatTimestamp",
    classOf[TimestampData],
    classOf[String],
    classOf[TimeZone])

  val UNIX_TIMESTAMP_FORMAT = Types.lookupMethod(
    classOf[DateTimeUtils],
    "unixTimestamp",
    classOf[String],
    classOf[String],
    classOf[TimeZone])

  val UNIX_TIMESTAMP_STR = Types.lookupMethod(
    classOf[DateTimeUtils], "unixTimestamp", classOf[String], classOf[TimeZone])

  val UNIX_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeUtils], "unixTimestamp")

  val UNIX_TIMESTAMP_TS = Types.lookupMethod(
    classOf[DateTimeUtils], "unixTimestamp", classOf[Long])

  val FROM_UNIXTIME_FORMAT = Types.lookupMethod(
    classOf[DateTimeUtils],
    "formatUnixTimestamp",
    classOf[Long],
    classOf[String],
    classOf[TimeZone])

  val FROM_UNIXTIME = Types.lookupMethod(
    classOf[DateTimeUtils], "formatUnixTimestamp", classOf[Long], classOf[TimeZone])

  val LONG_TO_TIMESTAMP_LTZ_WITH_PRECISION = Types.lookupMethod(
    classOf[DateTimeUtils],
    "toTimestampData",
    classOf[Long], classOf[Int])

  val DOUBLE_TO_TIMESTAMP_LTZ_WITH_PRECISION = Types.lookupMethod(
    classOf[DateTimeUtils],
    "toTimestampData",
    classOf[Double], classOf[Int])

  val DECIMAL_TO_TIMESTAMP_LTZ_WITH_PRECISION = Types.lookupMethod(
    classOf[DateTimeUtils],
    "toTimestampData",
    classOf[DecimalData], classOf[Int])

  val STRING_TO_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeUtils],
    "parseTimestampData",
    classOf[String])

  val STRING_TO_TIMESTAMP_WITH_FORMAT = Types.lookupMethod(
    classOf[DateTimeUtils],
    "parseTimestampData",
    classOf[String], classOf[String])

  val TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "timestampWithLocalZoneToDate",
    classOf[TimestampData], classOf[TimeZone])

  val TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME = Types.lookupMethod(
    classOf[DateTimeUtils],
    "timestampWithLocalZoneToTime",
    classOf[TimestampData], classOf[TimeZone])

  val DATE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "dateToTimestampWithLocalZone",
    classOf[Int], classOf[TimeZone])

  val TIME_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "timeToTimestampWithLocalZone",
    classOf[Int], classOf[TimeZone])

  val TIMESTAMP_TO_BIGINT = Types.lookupMethod(
    classOf[DateTimeUtils],
    "fromTimestamp",
    classOf[Long])

  val EXTRACT_FROM_TIMESTAMP_TIME_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "extractFromTimestamp",
    classOf[TimeUnitRange], classOf[TimestampData], classOf[TimeZone])

  val TIMESTAMP_FLOOR_TIME_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "timestampFloor",
    classOf[TimeUnitRange], classOf[Long], classOf[TimeZone])

  val TIMESTAMP_CEIL_TIME_ZONE = Types.lookupMethod(
    classOf[DateTimeUtils],
    "timestampCeil",
    classOf[TimeUnitRange], classOf[Long], classOf[TimeZone])

  val CONVERT_TZ = Types.lookupMethod(
    classOf[DateTimeUtils],
    "convertTz",
    classOf[String], classOf[String], classOf[String])

  val STRING_TO_DATE = Types.lookupMethod(
    classOf[DateTimeUtils], "parseDate", classOf[String])

  val STRING_TO_TIME = Types.lookupMethod(
    classOf[DateTimeUtils], "parseTime", classOf[String])

  val TRUNCATE_DOUBLE_ONE = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Double])
  val TRUNCATE_FLOAT_ONE = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Float])
  val TRUNCATE_INT_ONE = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Int])
  val TRUNCATE_LONG_ONE = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Long])
  val TRUNCATE_DEC_ONE = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[DecimalData])

  val TRUNCATE_DOUBLE = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Double], classOf[Int])
  val TRUNCATE_FLOAT = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Float], classOf[Int])
  val TRUNCATE_INT = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Int], classOf[Int])
  val TRUNCATE_LONG = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[Long], classOf[Int])
  val TRUNCATE_DEC = Types.lookupMethod(classOf[SqlFunctionUtils], "struncate",
    classOf[DecimalData], classOf[Int])

  val UNIX_DATE_CEIL = Types.lookupMethod(classOf[DateTimeUtils], "unixDateCeil",
    classOf[TimeUnitRange], classOf[Long])

  val UNIX_TIMESTAMP_CEIL = Types.lookupMethod(classOf[DateTimeUtils], "unixTimestampCeil",
    classOf[TimeUnitRange], classOf[Long])

  val UNIX_DATE_FLOOR = Types.lookupMethod(classOf[DateTimeUtils], "unixDateFloor",
    classOf[TimeUnitRange], classOf[Long])

  val UNIX_TIMESTAMP_FLOOR = Types.lookupMethod(classOf[DateTimeUtils], "unixTimestampFloor",
    classOf[TimeUnitRange], classOf[Long])

  val UNIX_DATE_EXTRACT = Types.lookupMethod(classOf[DateTimeUtils], "extractFromDate",
    classOf[TimeUnitRange], classOf[Int])

  val TRUNCATE_SQL_TIMESTAMP = Types.lookupMethod(classOf[DateTimeUtils], "truncate",
    classOf[TimestampData], classOf[Int])

  val ADD_MONTHS = Types.lookupMethod(classOf[DateTimeUtils], "addMonths",
    classOf[Long], classOf[Int])

  val SUBTRACT_MONTHS = Types.lookupMethod(classOf[DateTimeUtils], "subtractMonths",
    classOf[Long], classOf[Long])

  // JSON functions

  val JSON_EXISTS = Types.lookupMethod(classOf[SqlJsonUtils], "jsonExists",
    classOf[String], classOf[String])

  val JSON_EXISTS_ON_ERROR = Types.lookupMethod(classOf[SqlJsonUtils], "jsonExists",
    classOf[String], classOf[String], classOf[JsonExistsOnError])

  val JSON_VALUE = Types.lookupMethod(classOf[SqlJsonUtils], "jsonValue",
    classOf[String], classOf[String],
    classOf[JsonValueOnEmptyOrError], classOf[Any],
    classOf[JsonValueOnEmptyOrError], classOf[Any]
  )

  val JSON_QUERY = Types.lookupMethod(classOf[SqlJsonUtils], "jsonQuery",
    classOf[String], classOf[String], classOf[JsonQueryWrapper],
    classOf[JsonQueryOnEmptyOrError], classOf[JsonQueryOnEmptyOrError])

  val IS_JSON_VALUE = Types.lookupMethod(classOf[SqlJsonUtils], "isJsonValue",
    classOf[String])

  val IS_JSON_OBJECT = Types.lookupMethod(classOf[SqlJsonUtils], "isJsonObject",
    classOf[String])

  val IS_JSON_ARRAY = Types.lookupMethod(classOf[SqlJsonUtils], "isJsonArray",
    classOf[String])

  val IS_JSON_SCALAR = Types.lookupMethod(classOf[SqlJsonUtils], "isJsonScalar",
    classOf[String])

  // STRING functions

  val BINARY_STRING_DATA_FROM_STRING = Types.lookupMethod(classOf[BinaryStringData], "fromString",
    classOf[String])

  val STRING_DATA_TO_BOOLEAN = Types.lookupMethod(
    classOf[BinaryStringDataUtil],
    "toBoolean",
    classOf[BinaryStringData])

  val STRING_DATA_TO_DECIMAL = Types.lookupMethod(
    classOf[BinaryStringDataUtil],
    "toDecimal",
    classOf[BinaryStringData],
    classOf[Int],
    classOf[Int])

  val STRING_DATA_TO_LONG = Types.lookupMethod(
    classOf[BinaryStringDataUtil],
    "toLong",
    classOf[BinaryStringData])

  val STRING_DATA_TO_INT = Types.lookupMethod(
    classOf[BinaryStringDataUtil],
    "toInt",
    classOf[BinaryStringData])

  val STRING_DATA_TO_SHORT = Types.lookupMethod(
    classOf[BinaryStringDataUtil],
    "toShort",
    classOf[BinaryStringData])

  val STRING_DATA_TO_BYTE = Types.lookupMethod(
    classOf[BinaryStringDataUtil],
    "toByte",
    classOf[BinaryStringData])

  val STRING_DATA_TO_FLOAT = Types.lookupMethod(
    classOf[BinaryStringDataUtil],
    "toFloat",
    classOf[BinaryStringData])

  val STRING_DATA_TO_DOUBLE = Types.lookupMethod(
    classOf[BinaryStringDataUtil],
    "toDouble",
    classOf[BinaryStringData])

  val STRING_DATA_TO_DATE = Types.lookupMethod(
    classOf[BinaryStringDataUtil], "toDate", classOf[BinaryStringData])

  val STRING_DATA_TO_TIME = Types.lookupMethod(
    classOf[BinaryStringDataUtil], "toTime", classOf[BinaryStringData])

  val STRING_DATA_TO_TIMESTAMP = Types.lookupMethod(
    classOf[BinaryStringDataUtil], "toTimestamp", classOf[BinaryStringData])

  val STRING_DATA_TO_TIMESTAMP_WITH_ZONE = Types.lookupMethod(
    classOf[BinaryStringDataUtil], "toTimestamp", classOf[BinaryStringData], classOf[TimeZone])

  val STRING_LIKE = Types.lookupMethod(
    classOf[SqlLikeUtils], "like", classOf[String], classOf[String])

  val STRING_LIKE_WITH_ESCAPE = Types.lookupMethod(
    classOf[SqlLikeUtils], "like", classOf[String], classOf[String], classOf[String])

  val STRING_SIMILAR = Types.lookupMethod(
    classOf[SqlLikeUtils], "similar", classOf[String], classOf[String])

  val STRING_SIMILAR_WITH_ESCAPE = Types.lookupMethod(
    classOf[SqlLikeUtils], "similar", classOf[String], classOf[String], classOf[String])

  val STRING_INITCAP = Types.lookupMethod(classOf[SqlFunctionUtils], "initcap", classOf[String])

  // DecimalData functions

  val DECIMAL_TO_DECIMAL = Types.lookupMethod(
    classOf[DecimalDataUtils],
    "castToDecimal", classOf[DecimalData], classOf[Int], classOf[Int])

  val DECIMAL_TO_INTEGRAL = Types.lookupMethod(
    classOf[DecimalDataUtils],
    "castToIntegral", classOf[DecimalData])

  val DECIMAL_TO_DOUBLE = Types.lookupMethod(
    classOf[DecimalDataUtils],
    "doubleValue", classOf[DecimalData])

  val DECIMAL_TO_BOOLEAN = Types.lookupMethod(
    classOf[DecimalDataUtils],
    "castToBoolean", classOf[DecimalData])

  val INTEGRAL_TO_DECIMAL = Types.lookupMethod(
    classOf[DecimalDataUtils],
    "castFrom", classOf[Long], classOf[Int], classOf[Int])

  val DOUBLE_TO_DECIMAL = Types.lookupMethod(
    classOf[DecimalDataUtils],
    "castFrom", classOf[Long], classOf[Int], classOf[Int])

  val DECIMAL_ZERO = Types.lookupMethod(
    classOf[DecimalData],
    "zero", classOf[Int], classOf[Int])

}
