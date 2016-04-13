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

import scala.collection.JavaConverters._

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.schema.{Table => CTable}
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.typeutils.TypeConverter
import org.apache.flink.api.table.validate.ValidationException

case class Project(projectList: Seq[NamedExpression], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    def allAlias: Boolean = {
      projectList.forall { proj =>
        proj match {
          case Alias(r: ResolvedFieldReference, name) => true
          case _ => false
        }
      }
    }
    child.toRelNode(relBuilder)
    if (allAlias) {
      // Calcite's RelBuilder does not translate identity projects even if they rename fields.
      //   Add a projection ourselves (will be automatically removed by translation rules).
      relBuilder.push(
        LogicalProject.create(relBuilder.peek(),
          projectList.map(_.toRexNode(relBuilder)).asJava,
          projectList.map(_.name).asJava))
    } else {
      relBuilder.project(projectList.map(_.toRexNode(relBuilder)): _*)
    }
  }
}

case class AliasNode(aliasList: Seq[Expression], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] =
    throw new UnresolvedException("Invalid call to output on AliasNode")

  override def toRelNode(relBuilder: RelBuilder): RelBuilder =
    throw new UnresolvedException("Invalid call to toRelNode on AliasNode")

  override lazy val resolved: Boolean = false
}

case class Distinct(child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    child.toRelNode(relBuilder)
    relBuilder.distinct()
  }
}

case class Sort(order: Seq[Ordering], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    child.toRelNode(relBuilder)
    relBuilder.sort(order.map(_.toRexNode(relBuilder)).asJava)
  }
}

case class Filter(condition: Expression, child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    child.toRelNode(relBuilder)
    relBuilder.filter(condition.toRexNode(relBuilder))
  }
}

case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalNode) extends UnaryNode {

  override def output: Seq[Attribute] = {
    (groupingExpressions ++ aggregateExpressions) map { agg =>
      agg match {
        case ne: NamedExpression => ne.toAttribute
        case e => Alias(e, e.toString).toAttribute
      }
    }
  }

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    child.toRelNode(relBuilder)
    relBuilder.aggregate(
      relBuilder.groupKey(groupingExpressions.map(_.toRexNode(relBuilder)).asJava),
      aggregateExpressions.filter(_.isInstanceOf[Alias]).map { e =>
        e match {
          case Alias(agg: Aggregation, name) => agg.toAggCall(name)(relBuilder)
          case _ => null // this should never happen
        }
      }.asJava)
  }
}

case class Union(left: LogicalNode, right: LogicalNode) extends BinaryNode {
  override def output: Seq[Attribute] = left.output

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    left.toRelNode(relBuilder)
    right.toRelNode(relBuilder)
    relBuilder.union(true)
  }

  override lazy val resolved: Boolean = {
    childrenResolved &&
      left.output.length == right.output.length &&
      left.output.zip(right.output).forall { case (l, r) =>
        l.dataType == r.dataType && l.name == r.name }
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

  def noAmbiguousName: Boolean =
    left.output.map(_.name).toSet.intersect(right.output.map(_.name).toSet).isEmpty

  // Joins are only resolved if they don't introduce ambiguous names.
  override lazy val resolved: Boolean = {
    childrenResolved &&
      noAmbiguousName &&
      condition.forall(_.dataType == BasicTypeInfo.BOOLEAN_TYPE_INFO)
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

  override lazy val resolved: Boolean = true
}

/**
  * Wrapper for valid logical plans generated from SQL String.
  */
case class LogicalRelNode(
    relNode: RelNode) extends LeafNode {

  val output: Seq[Attribute] = relNode.getRowType.getFieldList.asScala.map { field =>
    ResolvedFieldReference(
      field.getName, TypeConverter.sqlTypeToTypeInfo(field.getType.getSqlTypeName))()
  }

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    relBuilder.push(relNode)
  }

  override lazy val resolved: Boolean = true
}
