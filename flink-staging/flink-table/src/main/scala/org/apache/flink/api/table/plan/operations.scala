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
package org.apache.flink.api.table.plan

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.trees.TreeNode

/**
 * Base class for all Table API operations.
 */
sealed abstract class PlanNode extends TreeNode[PlanNode] { self: Product =>
  def outputFields: Seq[(String, TypeInformation[_])]
}

/**
 * Operation that transforms a [[org.apache.flink.api.scala.DataSet]] or
 * [[org.apache.flink.streaming.api.scala.DataStream]] into a [[org.apache.flink.api.table.Table]].
 */
case class Root[T](input: T, outputFields: Seq[(String, TypeInformation[_])]) extends PlanNode {
  val children = Nil
  override def toString = s"Root($outputFields)"
}

/**
 * Operation that joins two [[org.apache.flink.api.table.Table]]s. A "filter" and a "select"
 * should be applied after a join operation.
 */
case class Join(left: PlanNode, right: PlanNode) extends PlanNode {

  val children = Seq(left, right)

  def outputFields = left.outputFields ++ right.outputFields

  override def toString = s"Join($left, $right)"
}

/**
 * Operation that filters out elements that do not match the predicate expression.
 */
case class Filter(input: PlanNode, predicate: Expression) extends PlanNode {

  val children = Seq(input)

  def outputFields = input.outputFields

  override def toString = s"Filter($input, $predicate)"
}

/**
 * Selection expression. Similar to an SQL SELECT statement. The expressions can select fields
 * and perform arithmetic or logic operations. The expressions can also perform aggregates
 * on fields.
 */
case class Select(input: PlanNode, selection: Seq[Expression]) extends PlanNode {

  val children = Seq(input)

  def outputFields = selection.toSeq map { e => (e.name, e.typeInfo) }

  override def toString = s"Select($input, ${selection.mkString(",")})"
}

/**
 * Operation that gives new names to fields. Use this to disambiguate fields before a join
 * operation.
 */
case class As(input: PlanNode, names: Seq[String]) extends PlanNode {

  val children = Seq(input)

  val outputFields = input.outputFields.zip(names) map {
    case ((_, tpe), newName) => (newName, tpe)
  }

  override def toString = s"As($input, ${names.mkString(",")})"
}

/**
 * Grouping operation. Keys are specified using field references. A group by operation os only
 * useful when performing a select with aggregates afterwards.
 * @param input
 * @param fields
 */
case class GroupBy(input: PlanNode, fields: Seq[Expression]) extends PlanNode {

  val children = Seq(input)

  def outputFields = input.outputFields

  override def toString = s"GroupBy($input, ${fields.mkString(",")})"
}

/**
 * Internal operation. Selection operations containing aggregates are expanded to an [[Aggregate]]
 * and a simple [[Select]].
 */
case class Aggregate(
    input: PlanNode,
    aggregations: Seq[(String, Aggregations)]) extends PlanNode {

  val children = Seq(input)

  def outputFields = input.outputFields

  override def toString = s"Aggregate($input, ${aggregations.mkString(",")})"
}

/**
 * UnionAll operation, union all elements from left and right.
 */
case class UnionAll(left: PlanNode, right: PlanNode) extends PlanNode{
  val children = Seq(left, right)

  def outputFields = left.outputFields

  override def toString = s"Union($left, $right)"
}
