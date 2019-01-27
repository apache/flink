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
import java.lang.{Byte => JByte, Integer => JInteger, Long => JLong, Short => JShort}
import java.util.{TimeZone, Date => JDate}
import java.sql.Time

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.linq4j.tree.Types
import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.runtime.functions.{BuildInScalarFunctions, DateTimeFunctions, ScalarFunctions}

object BuiltInMethods {

  val LOG = Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Double])

  val LOG_DEC = Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Decimal])

  val LOG_WITH_BASE =
    Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Double], classOf[Double])

  val LOG_WITH_BASE_DOU_DEC =
    Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Double], classOf[Decimal])

  val LOG_WITH_BASE_DEC_DOU =
    Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Decimal], classOf[Double])

  val LOG_WITH_BASE_DEC =
    Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Decimal], classOf[Decimal])

  val LOG10 = Types.lookupMethod(classOf[ScalarFunctions], "log10", classOf[Double])

  val LOG10_DEC = Types.lookupMethod(classOf[ScalarFunctions], "log10", classOf[Decimal])

  val EXP = Types.lookupMethod(classOf[Math], "exp", classOf[Double])

  val EXP_DEC = Types.lookupMethod(classOf[ScalarFunctions], "exp", classOf[Decimal])

  val POWER_NUM_NUM = Types.lookupMethod(classOf[Math], "pow", classOf[Double], classOf[Double])
  val POWER_NUM_DEC = Types.lookupMethod(
    classOf[ScalarFunctions], "power", classOf[Double], classOf[Decimal])
  val POWER_DEC_DEC = Types.lookupMethod(
    classOf[ScalarFunctions], "power", classOf[Decimal], classOf[Decimal])
  val POWER_DEC_NUM = Types.lookupMethod(
    classOf[ScalarFunctions], "power", classOf[Decimal], classOf[Double])

  val LN = Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Double])

  val LN_DEC = Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Decimal])

  val ABS = Types.lookupMethod(classOf[SqlFunctions], "abs", classOf[Double])
  val ABS_DEC = Types.lookupMethod(classOf[ScalarFunctions], "abs", classOf[Decimal])

  val FLOOR_DEC = Types.lookupMethod(classOf[ScalarFunctions], "floor", classOf[Decimal])
  val CEIL_DEC = Types.lookupMethod(classOf[ScalarFunctions], "ceil", classOf[Decimal])

  val LIKE_WITH_ESCAPE = Types.lookupMethod(classOf[SqlFunctions], "like",
    classOf[String], classOf[String], classOf[String])

  val SIMILAR_WITH_ESCAPE = Types.lookupMethod(classOf[SqlFunctions], "similar",
    classOf[String], classOf[String], classOf[String])

  val SIN = Types.lookupMethod(classOf[Math], "sin", classOf[Double])
  val SIN_DEC = Types.lookupMethod(classOf[ScalarFunctions], "sin", classOf[Decimal])

  val COS = Types.lookupMethod(classOf[Math], "cos", classOf[Double])
  val COS_DEC = Types.lookupMethod(classOf[ScalarFunctions], "cos", classOf[Decimal])

  val TAN = Types.lookupMethod(classOf[Math], "tan", classOf[Double])
  val TAN_DEC = Types.lookupMethod(classOf[ScalarFunctions], "tan", classOf[Decimal])

  val COT = Types.lookupMethod(classOf[SqlFunctions], "cot", classOf[Double])
  val COT_DEC = Types.lookupMethod(classOf[ScalarFunctions], "cot", classOf[Decimal])

  val ASIN = Types.lookupMethod(classOf[Math], "asin", classOf[Double])
  val ASIN_DEC = Types.lookupMethod(classOf[ScalarFunctions], "asin", classOf[Decimal])

  val ACOS = Types.lookupMethod(classOf[Math], "acos", classOf[Double])
  val ACOS_DEC = Types.lookupMethod(classOf[ScalarFunctions], "acos", classOf[Decimal])

  val ATAN = Types.lookupMethod(classOf[Math], "atan", classOf[Double])
  val ATAN_DEC = Types.lookupMethod(classOf[ScalarFunctions], "atan", classOf[Decimal])

  val SINH = Types.lookupMethod(classOf[Math], "sinh", classOf[Double])
  val SINH_DEC = Types.lookupMethod(classOf[ScalarFunctions], "sinh", classOf[Decimal])

  val COSH = Types.lookupMethod(classOf[Math], "cosh", classOf[Double])
  val COSH_DEC = Types.lookupMethod(classOf[ScalarFunctions], "cosh", classOf[Decimal])

  val TANH = Types.lookupMethod(classOf[Math], "tanh", classOf[Double])
  val TANH_DEC = Types.lookupMethod(classOf[ScalarFunctions], "tanh", classOf[Decimal])

  val ATAN2_DOUBLE_DOUBLE = Types.lookupMethod(
    classOf[Math],
    "atan2",
    classOf[Double],
    classOf[Double])
  val ATAN2_DEC_DEC = Types.lookupMethod(
    classOf[ScalarFunctions],
    "atan2",
    classOf[Decimal],
    classOf[Decimal])

  val DEGREES = Types.lookupMethod(classOf[Math], "toDegrees", classOf[Double])
  val DEGREES_DEC = Types.lookupMethod(classOf[ScalarFunctions], "degrees", classOf[Decimal])

  val RADIANS = Types.lookupMethod(classOf[Math], "toRadians", classOf[Double])
  val RADIANS_DEC = Types.lookupMethod(classOf[ScalarFunctions], "radians", classOf[Decimal])

  val SIGN_DOUBLE = Types.lookupMethod(classOf[Math], "signum", classOf[Double])
  val SIGN_INT = Types.lookupMethod(classOf[Integer], "signum", classOf[Int])
  val SIGN_LONG = Types.lookupMethod(classOf[JLong], "signum", classOf[Long])
  val SIGN_DEC = Types.lookupMethod(classOf[BuildInScalarFunctions], "sign", classOf[Decimal])

  val ROUND_DOUBLE = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[Double],
    classOf[Int])
  val ROUND_INT = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[Int], classOf[Int])
  val ROUND_LONG = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[Long], classOf[Int])
  val ROUND_DEC = Types.lookupMethod(classOf[BuildInScalarFunctions], "sround",
    classOf[Decimal], classOf[Int])

  val ROUND_DOUBLE_0 =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "sround", classOf[Double])
  val ROUND_INT_0 =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "sround", classOf[Int])
  val ROUND_LONG_0 =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "sround", classOf[Long])
  val ROUND_DEC_0 =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "sround", classOf[Decimal])

  val CONCAT = Types.lookupMethod(classOf[ScalarFunctions], "concat", classOf[Array[String]])
  val CONCAT_WS =
    Types.lookupMethod(
      classOf[ScalarFunctions], "concat_ws", classOf[String], classOf[Array[String]])


  // Blink-build-in functions
  val JSON_VALUE = Types.lookupMethod(
      classOf[BuildInScalarFunctions], "jsonValue", classOf[String], classOf[String])

  val STR_TO_MAP = Types.lookupMethod(classOf[BuildInScalarFunctions], "strToMap", classOf[String])

  val STR_TO_MAP_WITH_DELIMITER = Types.lookupMethod(
    classOf[BuildInScalarFunctions],
    "strToMap",
    classOf[String], classOf[String], classOf[String])

  val LOG2 = Types.lookupMethod(classOf[ScalarFunctions], "log2", classOf[Double])

  val LOG2_DEC = Types.lookupMethod(classOf[ScalarFunctions], "log2", classOf[Decimal])

  val IS_DECIMAL = Types.lookupMethod(
    classOf[BuildInScalarFunctions], "isDecimal", classOf[String])

  val IS_DIGIT = Types.lookupMethod(
    classOf[BuildInScalarFunctions], "isDigit", classOf[String])

  val IS_ALPHA = Types.lookupMethod(
    classOf[BuildInScalarFunctions], "isAlpha", classOf[String])

  val CHR = Types.lookupMethod(
    classOf[ScalarFunctions], "chr", classOf[Long])

  val LPAD = Types.lookupMethod(
    classOf[ScalarFunctions], "lpad", classOf[String], classOf[Int], classOf[String])

  val RPAD = Types.lookupMethod(
    classOf[ScalarFunctions], "rpad", classOf[String], classOf[Int], classOf[String])

  val REPEAT = Types.lookupMethod(
    classOf[ScalarFunctions], "repeat", classOf[String], classOf[Int])

  val REVERSE = Types.lookupMethod(
    classOf[ScalarFunctions], "reverse", classOf[String])

  val REPLACE = Types.lookupMethod(
    classOf[ScalarFunctions], "replace", classOf[String], classOf[String], classOf[String]
  )

  val SPLIT_INDEX_CHR = Types.lookupMethod(
    classOf[ScalarFunctions], "splitIndex", classOf[String], classOf[Int], classOf[Int])

  val SPLIT_INDEX_STR = Types.lookupMethod(
    classOf[ScalarFunctions], "splitIndex", classOf[String], classOf[String], classOf[Int])

  val REGEXP_REPLACE = Types.lookupMethod(
    classOf[ScalarFunctions], "regExpReplace", classOf[String], classOf[String], classOf[String])

  val REGEXP_EXTRACT = Types.lookupMethod(
    classOf[ScalarFunctions], "regExpExtract", classOf[String], classOf[String], classOf[Long])

  val KEYVALUE = Types.lookupMethod(
    classOf[ScalarFunctions], "keyValue",
    classOf[String], classOf[String], classOf[String], classOf[String])

  val HASH_CODE = Types.lookupMethod(
    classOf[BuildInScalarFunctions], "hashCode", classOf[String])

  val REGEXP = Types.lookupMethod(
    classOf[BuildInScalarFunctions], "regExp", classOf[String], classOf[String])

  val PARSE_URL = Types.lookupMethod(
    classOf[ScalarFunctions], "parseUrl", classOf[String], classOf[String])

  val PARSE_URL_KEY = Types.lookupMethod(
    classOf[ScalarFunctions], "parseUrl", classOf[String], classOf[String], classOf[String])

  val NOW = Types.lookupMethod(
    classOf[DateTimeFunctions], "now")

  val NOW_OFFSET = Types.lookupMethod(
    classOf[DateTimeFunctions], "now", classOf[Long])

  val DATE_FORMAT_STRING_STRING_STRING = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateFormat", classOf[String],
    classOf[String], classOf[String], classOf[TimeZone])

  val DATE_FORMAT_STIRNG_STRING = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateFormat", classOf[String], classOf[String], classOf[TimeZone])

  val DATE_FORMAT_LONG_STRING = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateFormat", classOf[Long], classOf[String], classOf[TimeZone])


  val UNIX_TIMESTAMP_FORMAT = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "unixTimestamp",
    classOf[String],
    classOf[String],
    classOf[TimeZone])

  val UNIX_TIMESTAMP_STR = Types.lookupMethod(
    classOf[DateTimeFunctions], "unixTimestamp", classOf[String], classOf[TimeZone])

  val UNIX_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeFunctions], "unixTimestamp")

  val UNIX_TIMESTAMP_TS = Types.lookupMethod(
    classOf[DateTimeFunctions], "unixTimestamp", classOf[Long])

  val FROM_UNIXTIME_FORMAT = Types.lookupMethod(
    classOf[DateTimeFunctions], "fromUnixtime", classOf[Long], classOf[String], classOf[TimeZone])

  val FROM_UNIXTIME = Types.lookupMethod(
    classOf[DateTimeFunctions], "fromUnixtime", classOf[Long], classOf[TimeZone])

  val FROM_UNIXTIME_AS_DOUBLE = Types.lookupMethod(
    classOf[DateTimeFunctions], "fromUnixtime", classOf[Double], classOf[TimeZone])

  val FROM_UNIXTIME_AS_DECIMAL = Types.lookupMethod(
    classOf[DateTimeFunctions], "fromUnixtime", classOf[Decimal], classOf[TimeZone])

  val DATEDIFF_T_S = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateDiff", classOf[Long], classOf[String], classOf[TimeZone])

  val DATEDIFF_S_S = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateDiff", classOf[String], classOf[String], classOf[TimeZone])

  val DATEDIFF_S_T = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateDiff", classOf[String], classOf[Long], classOf[TimeZone])

  val DATEDIFF_T_T = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateDiff", classOf[Long], classOf[Long], classOf[TimeZone])

  val DATE_SUB_S = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateSub", classOf[String], classOf[Int], classOf[TimeZone])

  val DATE_SUB_T = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateSub", classOf[Long], classOf[Int], classOf[TimeZone])

  val DATE_ADD_S = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateAdd", classOf[String], classOf[Int], classOf[TimeZone])

  val DATE_ADD_T = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateAdd", classOf[Long], classOf[Int], classOf[TimeZone])

  val BIN = Types.lookupMethod(classOf[JLong], "toBinaryString", classOf[Long])

  val BITAND_BYTE =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitAnd", classOf[JByte], classOf[JByte])
  val BITAND_SHORT =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitAnd", classOf[JShort], classOf[JShort])
  val BITAND_INTEGER =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitAnd", classOf[JInteger],
                       classOf[JInteger])
  val BITAND_LONG =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitAnd", classOf[JLong], classOf[JLong])

  val BITNOT_BYTE =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitNot", classOf[JByte])
  val BITNOT_SHORT =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitNot", classOf[JShort])
  val BITNOT_INTEGER =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitNot", classOf[JInteger])
  val BITNOT_LONG =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitNot", classOf[JLong])

  val BITOR_BYTE =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitOr", classOf[JByte], classOf[JByte])
  val BITOR_SHORT =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitOr", classOf[JShort], classOf[JShort])
  val BITOR_INTEGER =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitOr", classOf[JInteger],
                       classOf[JInteger])
  val BITOR_LONG =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitOr", classOf[JLong], classOf[JLong])

  val BITXOR_BYTE =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitXor", classOf[JByte], classOf[JByte])
  val BITXOR_SHORT =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitXor", classOf[JShort], classOf[JShort])
  val BITXOR_INTEGER =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitXor", classOf[JInteger],
                       classOf[JInteger])
  val BITXOR_LONG =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "bitXor", classOf[JLong], classOf[JLong])

  val DIV =
    Types.lookupMethod(classOf[BuildInScalarFunctions], "divide", classOf[Double], classOf[Double])
  val DIV_INT =
    Types.lookupMethod(
      classOf[ScalarFunctions],
      "divideInteger",
      classOf[Integer],
      classOf[Integer])

  val TO_BASE64 = Types.lookupMethod(
    classOf[BuildInScalarFunctions],
    "toBase64",
    classOf[Array[Byte]])

  val FROM_BASE64 = Types.lookupMethod(
    classOf[BuildInScalarFunctions],
    "fromBase64",
    classOf[String])

  val HEX_LONG: Method = Types.lookupMethod(classOf[ScalarFunctions], "hex", classOf[Long])
  val HEX_STRING: Method = Types.lookupMethod(classOf[ScalarFunctions], "hex", classOf[String])

  val UUID = Types.lookupMethod(
    classOf[BuildInScalarFunctions],
    "uuid")

  val UUID_BIN = Types.lookupMethod(
    classOf[BuildInScalarFunctions],
    "uuid", classOf[Array[Byte]])

  val SUBSTRING = Types.lookupMethod(
    classOf[ScalarFunctions],
    "subString",
    classOf[String], classOf[Long])

  val SUBSTRING_WITHLEN = Types.lookupMethod(
    classOf[ScalarFunctions],
    "subString",
    classOf[String], classOf[Long], classOf[Long])

  val OVERLAY = Types.lookupMethod(
    classOf[ScalarFunctions],
    "overlay",
    classOf[String], classOf[String], classOf[Long])

  val OVERLAY_WITHLEN = Types.lookupMethod(
    classOf[ScalarFunctions],
    "overlay",
    classOf[String], classOf[String], classOf[Long], classOf[Long])

  val INTERNAL_TO_DATE = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "internalToDate",
    classOf[Int], classOf[TimeZone])

  val INTERNAL_TO_TIME = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "internalToTime",
    classOf[Int], classOf[TimeZone])

  val INTERNAL_TO_TIMESTAMP = Types.lookupMethod(
    classOf[BuildInScalarFunctions],
    "internalToTimestamp",
    classOf[Long])

  val DATE_TO_INT = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "dateToInternal",
    classOf[JDate], classOf[TimeZone])

  val TIME_TO_INT = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "timeToInternal",
    classOf[Time], classOf[TimeZone])

  val TIMESTAMP_TO_LONG = Types.lookupMethod(
    classOf[BuildInScalarFunctions],
    "toLong",
    classOf[JDate])

  val INT_TO_DATE = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "toDate",
    classOf[Int])

  val LONG_TO_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "toTimestamp",
    classOf[Long])

  val DOUBLE_TO_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "toTimestamp",
    classOf[Double])

  val DECIMAL_TO_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "toTimestamp",
    classOf[Decimal])

  val STRING_TO_DATE_WITH_FORMAT = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "toDate",
    classOf[String], classOf[String])

  val STRING_TO_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "toTimestamp",
    classOf[String], classOf[TimeZone])

  val STRING_TO_TIMESTAMP_WITH_FORMAT = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "toTimestamp",
    classOf[String], classOf[String], classOf[TimeZone])

  val TIMESTAMP_TO_BIGINT = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "fromTimestamp",
    classOf[Long])

  val EXTRACT_FROM_TIMESTAMP = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "extractFromTimestamp",
    classOf[TimeUnitRange], classOf[Long], classOf[TimeZone])

  val EXTRACT_FROM_DATE = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "extractFromDate",
    classOf[TimeUnitRange], classOf[Long])

  val UNIX_TIME_EXTRACT = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "unixTimeExtract",
    classOf[TimeUnitRange], classOf[Int])

  val EXTRACT_YEAR_MONTH = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "extractYearMonth",
    classOf[TimeUnitRange], classOf[Int])

  val UNIX_TIME_TO_STRING = Types.lookupMethod(
    classOf[BuildInScalarFunctions],
    "unixTimeToString",
    classOf[Int])

  val TIMESTAMP_TO_STRING = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "timestampToStringPrecision",
    classOf[Long], classOf[Int], classOf[TimeZone])

  val TIMESTAMP_FLOOR = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "timestampFloor",
    classOf[TimeUnitRange], classOf[Long], classOf[TimeZone])

  val TIMESTAMP_CEIL = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "timestampCeil",
    classOf[TimeUnitRange], classOf[Long], classOf[TimeZone])

  val STRING_TO_TIMESTAMP_TZ = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "toTimestampTz",
    classOf[String], classOf[String])

  val STRING_TO_TIMESTAMP_FORMAT_TZ = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "toTimestampTz",
    classOf[String], classOf[String], classOf[String])

  val DATE_FORMAT_LONG_ZONE = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateFormatTz", classOf[Long], classOf[String])

  val DATE_FORMAT_LONG_STRING_ZONE = Types.lookupMethod(
    classOf[DateTimeFunctions], "dateFormatTz", classOf[Long], classOf[String], classOf[String])

  val CONVERT_TZ = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "convertTz",
    classOf[String], classOf[String], classOf[String])

  val CONVERT_FORMAT_TZ = Types.lookupMethod(
    classOf[DateTimeFunctions],
    "convertTz",
    classOf[String], classOf[String], classOf[String], classOf[String])

  val REGEXP_EXTRACT_WITHOUT_INDEX = Types.lookupMethod(
    classOf[ScalarFunctions],
    "regExpExtract",
    classOf[String],
    classOf[String])
}
