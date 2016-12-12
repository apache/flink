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

import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

abstract sealed class GroupFunction extends Expression {

  override def toString = s"GroupFunction($children)"
}

case class GroupId() extends GroupFunction {

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def children = Nil

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(SqlStdOperatorTable.GROUP_ID)
  }
}

case class Grouping(expression: Expression) extends GroupFunction {

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO


  override private[flink] def children = Seq(expression)

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(SqlStdOperatorTable.GROUPING, expression.toRexNode)
  }
}

case class GroupingId(expressions: Expression*) extends GroupFunction {

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO


  override private[flink] def children = expressions

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(SqlStdOperatorTable.GROUPING_ID, expressions.map(_.toRexNode): _*)
  }
}

