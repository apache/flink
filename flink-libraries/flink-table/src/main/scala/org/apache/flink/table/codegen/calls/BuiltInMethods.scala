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

import java.lang.{Long => JLong}
import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.linq4j.tree.Types
import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.table.functions.utils.MathFunctions

object BuiltInMethods {
  val LOG10 = Types.lookupMethod(classOf[Math], "log10", classOf[Double])

  val EXP = Types.lookupMethod(classOf[Math], "exp", classOf[Double])

  val POWER = Types.lookupMethod(classOf[Math], "pow", classOf[Double], classOf[Double])
  val POWER_DEC = Types.lookupMethod(
    classOf[MathFunctions], "power", classOf[Double], classOf[JBigDecimal])
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

  val COT = Types.lookupMethod(classOf[SqlFunctions], "cot", classOf[Double])
  val COT_DEC = Types.lookupMethod(classOf[SqlFunctions], "cot", classOf[JBigDecimal])

  val ASIN = Types.lookupMethod(classOf[Math], "asin", classOf[Double])
  val ASIN_DEC = Types.lookupMethod(classOf[SqlFunctions], "asin", classOf[JBigDecimal])

  val ACOS = Types.lookupMethod(classOf[Math], "acos", classOf[Double])
  val ACOS_DEC = Types.lookupMethod(classOf[SqlFunctions], "acos", classOf[JBigDecimal])

  val ATAN = Types.lookupMethod(classOf[Math], "atan", classOf[Double])
  val ATAN_DEC = Types.lookupMethod(classOf[SqlFunctions], "atan", classOf[JBigDecimal])

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
}
