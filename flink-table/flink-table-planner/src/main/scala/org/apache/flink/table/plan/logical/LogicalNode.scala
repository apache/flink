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

import java.util

import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.expressions._
import org.apache.flink.table.operations.{TableOperation, TableOperationVisitor}

import scala.collection.JavaConverters._

/**
  * LogicalNode is a logical representation of a table that can translate itself into [[RelNode]].
  */
abstract class LogicalNode extends TableOperation {
  def output: Seq[Attribute] = throw new TableException("Should not be called. Unless overridden")

  def children: Seq[TableOperation]

  override def getTableSchema: TableSchema = {
    val attributes = output
    new TableSchema(attributes.map(_.name).toArray, attributes.map(_.resultType).toArray)
  }

  def toRelNode(relBuilder: RelBuilder): RelNode

  override def getChildren: util.List[TableOperation] = children.asJava

  override def accept[T](visitor: TableOperationVisitor[T]): T = visitor.visitOther(this)

}

abstract class LeafNode extends LogicalNode {
  override def children: Seq[TableOperation] = Nil
}

abstract class UnaryNode extends LogicalNode {
  def child: TableOperation

  override def children: Seq[TableOperation] = child :: Nil
}

abstract class BinaryNode extends LogicalNode {
  def left: TableOperation
  def right: TableOperation

  override def children: Seq[TableOperation] = left :: right :: Nil
}
