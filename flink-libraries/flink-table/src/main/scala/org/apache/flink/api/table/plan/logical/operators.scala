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
package org.apache.flink.api.table.plan.logical

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.schema.{Table => CTable}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.typeutils.TypeConverter
import org.apache.flink.api.table.validate.ValidationException

import scala.collection.JavaConverters._

case class Project(projectList: Seq[NamedExpression], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
}

case class Filter(condition: Expression, child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[Expression],
    child: LogicalNode) extends UnaryNode {

  override def output: Seq[Attribute] = {
    aggregateExpressions.map { agg =>
      agg match {
        case ne: NamedExpression => ne
        case e => Alias(e, e.toString)
      }
    }
  }
}

case class Union(left: LogicalNode, right: LogicalNode) extends BinaryNode {
  override def output: Seq[Attribute] = left.output

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    left.toRelNode(relBuilder)
    right.toRelNode(relBuilder)
    relBuilder.union(true)
  }
}

case class Join(
    left: LogicalNode,
    right: LogicalNode,
    joinType: JoinType,
    condition: Option[Expression]) extends BinaryNode {

  override def output: Seq[Attribute] = {
    joinType match {
      case JoinType.INNER => left.output ++ right.output
      case j => throw new ValidationException(s"Unsupported JoinType: $j")
    }
  }

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    joinType match {
      case JoinType.INNER =>
        left.toRelNode(relBuilder)
        right.toRelNode(relBuilder)
        relBuilder.join(JoinRelType.INNER,
          condition.map(_.toRexNode(relBuilder)).getOrElse(relBuilder.literal(true)))
      case _ =>
        throw new ValidationException(s"Unsupported JoinType: $joinType")
    }
  }
}

case class CatalogNode(
    tableName: String,
    table: CTable,
    private val typeFactory: RelDataTypeFactory) extends LeafNode {

  val rowType = table.getRowType(typeFactory)

  val output: Seq[Attribute] = rowType.getFieldList.asScala.map { field =>
    ResolvedFieldReference(
      field.getName, TypeConverter.sqlTypeToTypeInfo(field.getType.getSqlTypeName))()
  }

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    relBuilder.scan(tableName)
  }
}

case class LogicalRelNode(
    relNode: RelNode) extends LeafNode {

  val output: Seq[Attribute] = relNode.getRowType.getFieldList.asScala.map { field =>
    ResolvedFieldReference(
      field.getName, TypeConverter.sqlTypeToTypeInfo(field.getType.getSqlTypeName))()
  }

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    relBuilder.push(relNode)
  }
}
