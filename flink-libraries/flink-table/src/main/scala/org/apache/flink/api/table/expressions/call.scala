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

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder

/**
  * General expression for unresolved function calls. The function can be a built-in
  * scalar function or a user-defined scalar function.
  */
case class Call(functionName: String, args: Expression*) extends Expression {

  override def children: Seq[Expression] = args

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(
      BuiltInFunctionNames.toSqlOperator(functionName),
      args.map(_.toRexNode): _*)
  }

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

  def toSqlOperator(name: String): SqlOperator = {
    name match {
      case BuiltInFunctionNames.SUBSTRING => SqlStdOperatorTable.SUBSTRING
      case BuiltInFunctionNames.TRIM => SqlStdOperatorTable.TRIM
      case BuiltInFunctionNames.CHAR_LENGTH => SqlStdOperatorTable.CHAR_LENGTH
      case BuiltInFunctionNames.UPPER_CASE => SqlStdOperatorTable.UPPER
      case BuiltInFunctionNames.LOWER_CASE => SqlStdOperatorTable.LOWER
      case BuiltInFunctionNames.INIT_CAP => SqlStdOperatorTable.INITCAP
      case BuiltInFunctionNames.LIKE => SqlStdOperatorTable.LIKE
      case BuiltInFunctionNames.SIMILAR => SqlStdOperatorTable.SIMILAR_TO
      case BuiltInFunctionNames.EXP => SqlStdOperatorTable.EXP
      case BuiltInFunctionNames.LOG10 => SqlStdOperatorTable.LOG10
      case BuiltInFunctionNames.POWER => SqlStdOperatorTable.POWER
      case BuiltInFunctionNames.LN => SqlStdOperatorTable.LN
      case BuiltInFunctionNames.ABS => SqlStdOperatorTable.ABS
      case BuiltInFunctionNames.MOD => SqlStdOperatorTable.MOD
      case _ => ???
    }
  }
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
