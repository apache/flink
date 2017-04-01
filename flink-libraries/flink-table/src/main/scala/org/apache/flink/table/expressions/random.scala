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

package org.apache.flink.table.expressions

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

import scala.collection.JavaConversions._

case class Rand(seed: Expression) extends Expression with InputTypeSpec {

  def this() = this(null)

  override private[flink] def children: Seq[Expression] = if (seed != null) {
    seed :: Nil
  } else {
    Nil
  }

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = if (seed != null) {
    INT_TYPE_INFO :: Nil
  } else {
    Nil
  }

  override def toString: String = if (seed != null) {
    s"rand($seed)"
  } else {
    s"rand()"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.RAND, children.map(_.toRexNode))
  }

}

case class RandInteger(seed: Expression, bound: Expression) extends Expression with InputTypeSpec {

  def this(bound: Expression) = this(null, bound)

  override private[flink] def children: Seq[Expression] = if (seed != null) {
    seed :: bound :: Nil
  } else {
    bound :: Nil
  }

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.INT_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = if (seed != null) {
    INT_TYPE_INFO :: INT_TYPE_INFO :: Nil
  } else {
    INT_TYPE_INFO :: Nil
  }

  override def toString: String = if (seed != null) {
    s"rand($seed, $bound)"
  } else {
    s"rand($bound)"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.RAND_INTEGER, children.map(_.toRexNode))
  }

}
