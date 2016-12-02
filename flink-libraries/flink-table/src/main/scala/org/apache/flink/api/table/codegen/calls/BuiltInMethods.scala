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
package org.apache.flink.api.table.codegen.calls

import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.linq4j.tree.Types
import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.api.table.functions.utils.MathFunctions

object BuiltInMethods {
  val LOG10 = Types.lookupMethod(classOf[Math], "log10", classOf[Double])
  val EXP = Types.lookupMethod(classOf[Math], "exp", classOf[Double])
  val POWER = Types.lookupMethod(classOf[Math], "pow", classOf[Double], classOf[Double])
  val POWER_DEC = Types.lookupMethod(
    classOf[MathFunctions], "power", classOf[Double], classOf[JBigDecimal])
  val LN = Types.lookupMethod(classOf[Math], "log", classOf[Double])
  val ABS = Types.lookupMethod(classOf[SqlFunctions], "abs", classOf[Double])
  val ABS_DEC = Types.lookupMethod(classOf[SqlFunctions], "abs", classOf[JBigDecimal])
  val LIKE_WITH_ESCAPE = Types.lookupMethod(classOf[SqlFunctions], "like",
    classOf[String], classOf[String], classOf[String])
  val SIMILAR_WITH_ESCAPE = Types.lookupMethod(classOf[SqlFunctions], "similar",
    classOf[String], classOf[String], classOf[String])
}
