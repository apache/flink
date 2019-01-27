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

package org.apache.flink.table.plan.util

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.logical._
import org.apache.flink.util.Preconditions

object LogicalNodeUtil {
  def cloneLogicalNode(node: LogicalNode, children: Seq[LogicalNode]): LogicalNode = {
    node match {
      case CatalogNode(tableName, rowType) => CatalogNode(tableName, rowType)
      case LogicalRelNode(relNode) => LogicalRelNode(relNode)
      case l: LogicalTableFunctionCall => l
      case u: UnaryNode =>
        Preconditions.checkArgument(children.length == 1)
        val child = children.head
        u match {
          case Filter(condition, _) => Filter(condition, child)
          case Aggregate(grouping, aggregate, _) => Aggregate(grouping, aggregate, child)
          case Limit(offset, fetch, _) => Limit(offset, fetch, child)
          case WindowAggregate(grouping, window, property, aggregate, _) =>
            WindowAggregate(grouping, window, property, aggregate, child)
          case Distinct(_) => Distinct(child)
          case SinkNode(_, sink, sinkName) => SinkNode(child, sink, sinkName)
          case AliasNode(aliasList, _) => AliasNode(aliasList, child)
          case Project(projectList, _) => Project(projectList, child)
          case Sort(order, _) => Sort(order, child)
          case _ => throw new TableException(s"Unsupported UnaryNode node: $node")
        }
      case b: BinaryNode =>
        Preconditions.checkArgument(children.length == 2)
        val left = children.head
        val right = children.last
        b match {
          case Join(_, _, joinType, condition, correlated) =>
            Join(left, right, joinType, condition, correlated)
          case Union(_, _, all) => Union(left, right, all)
          case Intersect(_, _, all) => Intersect(left, right, all)
          case Minus(_, _, all) => Minus(left, right, all)
          case _ => throw new TableException(s"Unsupported BinaryNode node: $node")
        }
      case _ => throw new TableException(s"Unsupported LogicalNode node: $node")
    }
  }
}
