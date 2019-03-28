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
package org.apache.flink.table.plan.logical

import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.{TableEnvironment, TableSchema, ValidationException}
import org.apache.flink.table.expressions._
import org.apache.flink.table.operations.TableOperation
import org.apache.flink.table.plan.TreeNode

/**
  * LogicalNode is a logical representation of a table that can translate itself into [[RelNode]].
  */
abstract class LogicalNode extends TreeNode[LogicalNode] with TableOperation {
  def output: Seq[Attribute]

  override def getTableSchema: TableSchema = {
    val attributes = output
    new TableSchema(attributes.map(_.name).toArray, attributes.map(_.resultType).toArray)
  }

  final def toRelNode(relBuilder: RelBuilder): RelNode = construct(relBuilder).build()

  protected[logical] def construct(relBuilder: RelBuilder): RelBuilder

  protected def failValidation(msg: String): Nothing = {
    throw new ValidationException(msg)
  }
}

abstract class LeafNode extends LogicalNode {
  override def children: Seq[LogicalNode] = Nil
}

abstract class UnaryNode extends LogicalNode {
  def child: LogicalNode

  override def children: Seq[LogicalNode] = child :: Nil
}

abstract class BinaryNode extends LogicalNode {
  def left: LogicalNode
  def right: LogicalNode

  override def children: Seq[LogicalNode] = left :: right :: Nil
}
