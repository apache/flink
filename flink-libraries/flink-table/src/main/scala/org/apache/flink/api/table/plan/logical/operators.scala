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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.{StreamTableEnvironment, TableEnvironment}
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.typeutils.TypeConverter
import org.apache.flink.api.table.validate.ValidationException

case class Project(projectList: Seq[NamedExpression], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def resolveExpressions(tableEnv: TableEnvironment): LogicalNode = {
    val afterResolve = super.resolveExpressions(tableEnv).asInstanceOf[Project]
    val newProjectList =
      afterResolve.projectList.zipWithIndex.map { case (e, i) =>
        e match {
          case u @ UnresolvedAlias(child, optionalAliasName) => child match {
            case ne: NamedExpression => ne
            case e if !e.valid => u
            case c @ Cast(ne: NamedExpression, _) => Alias(c, ne.name)
            case other => Alias(other, optionalAliasName.getOrElse(s"_c$i"))
          }
          case _ => throw new IllegalArgumentException
        }
    }
    Project(newProjectList, child)
  }

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    val allAlias = projectList.forall(_.isInstanceOf[Alias])
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

  override def resolveExpressions(tableEnv: TableEnvironment): LogicalNode = {
    if (aliasList.length > child.output.length) {
      failValidation("Aliasing more fields than we actually have")
    } else if (!aliasList.forall(_.isInstanceOf[UnresolvedFieldReference])) {
      failValidation("`as` only allow string arguments")
    } else {
      val names = aliasList.map(_.asInstanceOf[UnresolvedFieldReference].name)
      val input = child.output
      Project(
        names.zip(input).map { case (name, attr) =>
          Alias(attr, name)} ++ input.drop(names.length), child)
    }
  }
}

case class Distinct(child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    child.toRelNode(relBuilder)
    relBuilder.distinct()
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      failValidation(s"Distinct on stream tables is currently not supported.")
    }
    this
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

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    val resolvedFilter = super.validate(tableEnv).asInstanceOf[Filter]
    if (resolvedFilter.condition.dataType != BOOLEAN_TYPE_INFO) {
      failValidation(s"filter expression ${resolvedFilter.condition} of" +
        s" ${resolvedFilter.condition.dataType} is not a boolean")
    }
    resolvedFilter
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

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      failValidation(s"Aggregate on stream tables is currently not supported.")
    }

    val resolvedAggregate = super.validate(tableEnv).asInstanceOf[Aggregate]
    val groupingExprs = resolvedAggregate.groupingExpressions
    val aggregateExprs = resolvedAggregate.aggregateExpressions
    aggregateExprs.foreach(validateAggregateExpression)
    groupingExprs.foreach(validateGroupingExpression)

    def validateAggregateExpression(expr: Expression): Unit = expr match {
      // check no nested aggregation exists.
      case aggExpr: Aggregation =>
        aggExpr.children.foreach { child =>
          child.foreach {
            case agg: Aggregation =>
              failValidation(
                "It's not allowed to use an aggregate function as " +
                  "input of another aggregate function")
            case _ => // OK
          }
        }
      case a: Attribute if !groupingExprs.exists(_.semanticEquals(a)) =>
        failValidation(
          s"expression '$a' is invalid because it is neither" +
            " present in group by nor an aggregate function")
      case e if groupingExprs.exists(_.semanticEquals(e)) => // OK
      case e => e.children.foreach(validateAggregateExpression)
    }

    def validateGroupingExpression(expr: Expression): Unit = {
      if (!expr.dataType.isKeyType) {
        failValidation(
          s"expression $expr cannot be used as a grouping expression " +
            "because it's not a valid key type")
      }
    }
    resolvedAggregate
  }
}

case class Union(left: LogicalNode, right: LogicalNode) extends BinaryNode {
  override def output: Seq[Attribute] = left.output

  override def toRelNode(relBuilder: RelBuilder): RelBuilder = {
    left.toRelNode(relBuilder)
    right.toRelNode(relBuilder)
    relBuilder.union(true)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    val resolvedUnion = super.validate(tableEnv).asInstanceOf[Union]
    if (left.output.length != right.output.length) {
      failValidation(s"Union two table of different column sizes:" +
        s" ${left.output.size} and ${right.output.size}")
    }
    val sameSchema = left.output.zip(right.output).forall { case (l, r) =>
      l.dataType == r.dataType && l.name == r.name }
    if (!sameSchema) {
      failValidation(s"Union two table of different schema:" +
        s" [${left.output.map(a => (a.name, a.dataType)).mkString(", ")}] and" +
        s" [${right.output.map(a => (a.name, a.dataType)).mkString(", ")}]")
    }
    resolvedUnion
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

  private def ambiguousName: Set[String] =
    left.output.map(_.name).toSet.intersect(right.output.map(_.name).toSet)

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      failValidation(s"Join on stream tables is currently not supported.")
    }

    val resolvedJoin = super.validate(tableEnv).asInstanceOf[Join]
    if (!resolvedJoin.condition.forall(_.dataType == BOOLEAN_TYPE_INFO)) {
      failValidation(s"filter expression ${resolvedJoin.condition} is not a boolean")
    } else if (!ambiguousName.isEmpty) {
      failValidation(s"join relations with ambiguous names: ${ambiguousName.mkString(", ")}")
    }
    resolvedJoin
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

  override def validate(tableEnv: TableEnvironment): LogicalNode = this
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

  override def validate(tableEnv: TableEnvironment): LogicalNode = this
}
