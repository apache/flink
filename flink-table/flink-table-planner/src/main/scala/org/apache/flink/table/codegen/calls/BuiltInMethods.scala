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
import java.lang.{Long => JLong}
import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.linq4j.tree.Types
import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.table.runtime.functions.ScalarFunctions

/**
  * Contains references to built-in functions.
  *
  * NOTE: When adding functions here. Check if Calcite provides it in
  * [[org.apache.calcite.util.BuiltInMethod]]. The function generator supports Java's auto casting
  * so we don't need the full matrix of data types for every function. Only [[JBigDecimal]] needs
  * special handling.
  */
object BuiltInMethods {

  val LOG = Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Double])

  val LOG_WITH_BASE =
    Types.lookupMethod(classOf[ScalarFunctions], "log", classOf[Double], classOf[Double])

  val LOG10 = Types.lookupMethod(classOf[Math], "log10", classOf[Double])

  val LOG2 = Types.lookupMethod(classOf[ScalarFunctions], "log2", classOf[Double])

  val EXP = Types.lookupMethod(classOf[Math], "exp", classOf[Double])

  val POWER = Types.lookupMethod(classOf[Math], "pow", classOf[Double], classOf[Double])
  val POWER_DEC = Types.lookupMethod(
    classOf[ScalarFunctions], "power", classOf[Double], classOf[JBigDecimal])
  val POWER_DEC_DEC = Types.lookupMethod(
    classOf[SqlFunctions], "power", classOf[JBigDecimal], classOf[JBigDecimal])

  val LN = Types.lookupMethod(classOf[Math], "log", classOf[Double])

  val ABS = Types.lookupMethod(classOf[SqlFunctions], "abs", classOf[Double])
  val ABS_DEC = Types.lookupMethod(classOf[SqlFunctions], "abs", classOf[JBigDecimal])

  val LIKE_WITH_ESCAPE = Types.lookupMethod(classOf[SqlFunctions], "like",
    classOf[String], classOf[String], classOf[String])

  val SIMILAR_WITH_ESCAPE = Types.lookupMethod(classOf[SqlFunctions], "similar",
    classOf[String], classOf[String], classOf[String])

  val SIN = Types.lookupMethod(classOf[Math], "sin", classOf[Double])
  val SIN_DEC = Types.lookupMethod(classOf[SqlFunctions], "sin", classOf[JBigDecimal])

  val COS = Types.lookupMethod(classOf[Math], "cos", classOf[Double])
  val COS_DEC = Types.lookupMethod(classOf[SqlFunctions], "cos", classOf[JBigDecimal])

  val TAN = Types.lookupMethod(classOf[Math], "tan", classOf[Double])
  val TAN_DEC = Types.lookupMethod(classOf[SqlFunctions], "tan", classOf[JBigDecimal])

  val TANH = Types.lookupMethod(classOf[Math], "tanh", classOf[Double])
  val TANH_DEC = Types.lookupMethod(classOf[ScalarFunctions], "tanh", classOf[JBigDecimal])

  val COT = Types.lookupMethod(classOf[SqlFunctions], "cot", classOf[Double])
  val COT_DEC = Types.lookupMethod(classOf[SqlFunctions], "cot", classOf[JBigDecimal])

  val ASIN = Types.lookupMethod(classOf[Math], "asin", classOf[Double])
  val ASIN_DEC = Types.lookupMethod(classOf[SqlFunctions], "asin", classOf[JBigDecimal])

  val ACOS = Types.lookupMethod(classOf[Math], "acos", classOf[Double])
  val ACOS_DEC = Types.lookupMethod(classOf[SqlFunctions], "acos", classOf[JBigDecimal])

  val SINH = Types.lookupMethod(classOf[Math], "sinh", classOf[Double])
  val SINH_DEC = Types.lookupMethod(classOf[ScalarFunctions], "sinh", classOf[JBigDecimal])

  val ATAN = Types.lookupMethod(classOf[Math], "atan", classOf[Double])
  val ATAN_DEC = Types.lookupMethod(classOf[SqlFunctions], "atan", classOf[JBigDecimal])

  val COSH = Types.lookupMethod(classOf[Math], "cosh", classOf[Double])
  val COSH_DEC = Types.lookupMethod(classOf[ScalarFunctions], "cosh", classOf[JBigDecimal])

  val ATAN2_DOUBLE_DOUBLE = Types.lookupMethod(
    classOf[Math],
    "atan2",
    classOf[Double],
    classOf[Double])
  val ATAN2_DEC_DEC = Types.lookupMethod(
    classOf[SqlFunctions],
    "atan2",
    classOf[JBigDecimal],
    classOf[JBigDecimal])

  val DEGREES = Types.lookupMethod(classOf[Math], "toDegrees", classOf[Double])
  val DEGREES_DEC = Types.lookupMethod(classOf[SqlFunctions], "degrees", classOf[JBigDecimal])

  val RADIANS = Types.lookupMethod(classOf[Math], "toRadians", classOf[Double])
  val RADIANS_DEC = Types.lookupMethod(classOf[SqlFunctions], "radians", classOf[JBigDecimal])

  val SIGN_DOUBLE = Types.lookupMethod(classOf[Math], "signum", classOf[Double])
  val SIGN_INT = Types.lookupMethod(classOf[Integer], "signum", classOf[Int])
  val SIGN_LONG = Types.lookupMethod(classOf[JLong], "signum", classOf[Long])
  val SIGN_DEC = Types.lookupMethod(classOf[SqlFunctions], "sign", classOf[JBigDecimal])

  val ROUND_DOUBLE = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[Double],
    classOf[Int])
  val ROUND_INT = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[Int], classOf[Int])
  val ROUND_LONG = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[Long], classOf[Int])
  val ROUND_DEC = Types.lookupMethod(classOf[SqlFunctions], "sround", classOf[JBigDecimal],
    classOf[Int])

  val CONCAT = Types.lookupMethod(classOf[ScalarFunctions], "concat", classOf[Array[String]])
  val CONCAT_WS =
    Types.lookupMethod(
      classOf[ScalarFunctions], "concat_ws", classOf[String], classOf[Array[String]])

  val LPAD = Types.lookupMethod(
    classOf[ScalarFunctions],
    "lpad",
    classOf[String],
    classOf[Integer],
    classOf[String])
  val RPAD = Types.lookupMethod(
    classOf[ScalarFunctions],
    "rpad",
    classOf[String],
    classOf[Integer],
    classOf[String])

  val BIN = Types.lookupMethod(classOf[JLong], "toBinaryString", classOf[Long])

  val REGEXP_REPLACE = Types.lookupMethod(
    classOf[ScalarFunctions],
    "regexp_replace",
    classOf[String],
    classOf[String],
    classOf[String])

  val REGEXP_EXTRACT = Types.lookupMethod(
    classOf[ScalarFunctions],
    "regexp_extract",
    classOf[String],
    classOf[String],
    classOf[Integer])

  val REGEXP_EXTRACT_WITHOUT_INDEX = Types.lookupMethod(
    classOf[ScalarFunctions],
    "regexp_extract",
    classOf[String],
    classOf[String])

  val FROMBASE64 = Types.lookupMethod(classOf[ScalarFunctions], "fromBase64", classOf[String])

  val TOBASE64 = Types.lookupMethod(classOf[ScalarFunctions], "toBase64", classOf[String])

  val HEX_LONG: Method = Types.lookupMethod(classOf[ScalarFunctions], "hex", classOf[Long])
  val HEX_STRING: Method = Types.lookupMethod(classOf[ScalarFunctions], "hex", classOf[String])

  val UUID: Method = Types.lookupMethod(classOf[ScalarFunctions], "uuid")

  val REPEAT: Method = Types.lookupMethod(
    classOf[ScalarFunctions],
    "repeat",
    classOf[String],
    classOf[Int])

  val TRUNCATE_DOUBLE_ONE = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Double])
  val TRUNCATE_INT_ONE = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Int])
  val TRUNCATE_LONG_ONE = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Long])
  val TRUNCATE_DEC_ONE = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[JBigDecimal])

  val TRUNCATE_DOUBLE = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Double], classOf[Int])
  val TRUNCATE_INT = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Int], classOf[Int])
  val TRUNCATE_LONG = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[Long], classOf[Int])
  val TRUNCATE_DEC = Types.lookupMethod(classOf[SqlFunctions], "struncate",
    classOf[JBigDecimal], classOf[Int])
}
