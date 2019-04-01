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

import java.util.{Optional, List => JList}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{CorrelationId, JoinRelType}
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.calcite.FlinkRelBuilder
import org.apache.flink.table.expressions._
import org.apache.flink.table.operations.JoinOperationFactory.JoinType
import org.apache.flink.table.operations.{TableOperation, TableOperationVisitor}
import org.apache.flink.table.util.JavaScalaConversionUtil

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Legacy logical representation of a [[org.apache.flink.table.api.Table]]. Should be subsumed by
  * [[TableOperation]].
  */
abstract class LogicalNode extends TableOperation {
  protected def output: Seq[Attribute]

  override def getTableSchema: TableSchema = {
    val attributes = output
    new TableSchema(attributes.map(_.name).toArray, attributes.map(_.resultType).toArray)
  }

  def toRelNode(relBuilder: RelBuilder): RelNode

  override def accept[T](visitor: TableOperationVisitor[T]): T = visitor.visitOther(this)
}

case class Join(
    left: TableOperation,
    right: TableOperation,
    joinType: JoinType,
    condition: Optional[PlannerExpression],
    correlated: Boolean) extends LogicalNode {

  override def output: Seq[Attribute] = throw new TableException(
    "Should never be called. Call getTableSchema instead.")

  override def getTableSchema: TableSchema = new TableSchema(
    left.getTableSchema.getFieldNames ++ right.getTableSchema.getFieldNames,
    left.getTableSchema.getFieldTypes ++ right.getTableSchema.getFieldTypes)

  private case class JoinFieldReference(
    name: String,
    resultType: TypeInformation[_],
    left: TableOperation,
    right: TableOperation) extends Attribute {

    val isFromLeftInput: Boolean = left.getTableSchema.getFieldNames.contains(name)

    val (indexInInput, indexInJoin) = if (isFromLeftInput) {
      val indexInLeft = left.getTableSchema.getFieldNames.indexOf(name)
      (indexInLeft, indexInLeft)
    } else {
      val indexInRight = right.getTableSchema.getFieldNames.indexOf(name)
      (indexInRight, indexInRight + left.getTableSchema.getFieldCount)
    }

    override def toString = s"'$name"

    override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
      // look up type of field
      val fieldType = relBuilder.field(2, if (isFromLeftInput) 0 else 1, name).getType
      // create a new RexInputRef with index offset
      new RexInputRef(indexInJoin, fieldType)
    }

    override def withName(newName: String): Attribute = {
      if (newName == name) {
        this
      } else {
        JoinFieldReference(newName, resultType, left, right)
      }
    }
  }

  def replaceFieldReferences(): Option[PlannerExpression] = {
    val partialFunction: PartialFunction[PlannerExpression, PlannerExpression] = {
      case field: ResolvedFieldReference => JoinFieldReference(
        field.name,
        field.resultType,
        left,
        right)
    }
    JavaScalaConversionUtil.toScala(condition).map(_.postOrderTransform(partialFunction))
  }

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    val corSet = mutable.Set[CorrelationId]()
    if (correlated) {
      corSet += relBuilder.peek().getCluster.createCorrel()
    }

    val resolvedCondition = replaceFieldReferences()
    relBuilder.join(
      convertJoinType(joinType),
      resolvedCondition.map(_.toRexNode(relBuilder)).getOrElse(relBuilder.literal(true)),
      corSet.asJava)
      .build()
  }

  private def convertJoinType(joinType: JoinType) = joinType match {
    case JoinType.INNER => JoinRelType.INNER
    case JoinType.LEFT_OUTER => JoinRelType.LEFT
    case JoinType.RIGHT_OUTER => JoinRelType.RIGHT
    case JoinType.FULL_OUTER => JoinRelType.FULL
  }

  override def getChildren: JList[TableOperation] = (left +: right +: Nil).asJava
}

case class WindowAggregate(
  groupingExpressions: JList[PlannerExpression],
    window: LogicalWindow,
    propertyExpressions: JList[PlannerExpression],
    aggregateExpressions: JList[PlannerExpression],
    child: TableOperation)
  extends LogicalNode {

  override def output: Seq[Attribute] = {
    val expressions = groupingExpressions.asScala ++ aggregateExpressions.asScala ++
      propertyExpressions.asScala
    expressions map {
      case ne: NamedExpression => ne.toAttribute
      case e => Alias(e, e.toString).toAttribute
    }
  }

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    val flinkRelBuilder = relBuilder.asInstanceOf[FlinkRelBuilder]
    flinkRelBuilder.aggregate(
      window,
      relBuilder.groupKey(groupingExpressions.asScala.map(_.toRexNode(relBuilder)).asJava),
      propertyExpressions.asScala.map {
        case Alias(prop: WindowProperty, name, _) => prop.toNamedWindowProperty(name)
        case _ => throw new RuntimeException("This should never happen.")
      },
      aggregateExpressions.asScala.map {
        case Alias(agg: Aggregation, name, _) => agg.toAggCall(name)(relBuilder)
        case _ => throw new RuntimeException("This should never happen.")
      }.asJava).build()
  }

  override def getChildren: JList[TableOperation] = (child +: Nil).asJava
}
