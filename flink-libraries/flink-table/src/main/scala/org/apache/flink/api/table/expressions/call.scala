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
package org.apache.flink.api.table.expressions

/**
  * General expression for unresolved function calls. The function can be a built-in
  * scalar function or a user-defined scalar function.
  */
case class Call(functionName: String, args: Expression*) extends Expression {

  override def children: Seq[Expression] = args

  override def toString = s"\\$functionName(${args.mkString(", ")})"

  override def makeCopy(newArgs: Seq[AnyRef]): this.type = {
    val copy = Call(
      newArgs.head.asInstanceOf[String],
      newArgs.drop(1).asInstanceOf[Seq[Expression]]: _*)

    copy.asInstanceOf[this.type]
  }
}

/**
  * Enumeration of common function names.
  */
object BuiltInFunctionNames {
  val SUBSTRING = "SUBSTRING"
  val TRIM = "TRIM"
  val CHAR_LENGTH = "CHARLENGTH"
  val UPPER_CASE = "UPPERCASE"
  val LOWER_CASE = "LOWERCASE"
  val INIT_CAP = "INITCAP"
  val LIKE = "LIKE"
  val SIMILAR = "SIMILAR"
  val MOD = "MOD"
  val EXP = "EXP"
  val LOG10 = "LOG10"
  val POWER = "POWER"
  val LN = "LN"
  val ABS = "ABS"
}

/**
  * Enumeration of common function flags.
  */
object BuiltInFunctionConstants {
  val TRIM_BOTH = Literal(0)
  val TRIM_LEADING = Literal(1)
  val TRIM_TRAILING = Literal(2)
  val TRIM_DEFAULT_CHAR = Literal(" ")
}
