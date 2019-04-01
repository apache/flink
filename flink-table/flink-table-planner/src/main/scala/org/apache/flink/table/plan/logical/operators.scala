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

import java.util.{Collections, Optional, List => JList}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{CorrelationId, JoinRelType}
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.operations.JoinOperationFactory.JoinType
import org.apache.flink.table.operations.TableOperation
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.table.util.JavaScalaConversionUtil

import scala.collection.JavaConverters._
import scala.collection.mutable

case class Distinct(child: TableOperation) extends UnaryNode {

  override def getTableSchema: TableSchema = child.getTableSchema

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    relBuilder.distinct().build()
  }
}

case class Sort(order: JList[PlannerExpression], child: TableOperation) extends UnaryNode {
  override def getTableSchema: TableSchema = child.getTableSchema

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    relBuilder.sort(order.asScala.map(_.toRexNode(relBuilder)).asJava).build()
  }
}

case class Limit(offset: Int, fetch: Int = -1, child: TableOperation) extends UnaryNode {
  override def getTableSchema: TableSchema = child.getTableSchema

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    relBuilder.limit(offset, fetch).build()
  }

}

case class Filter(condition: PlannerExpression, child: TableOperation) extends UnaryNode {
  override def getTableSchema: TableSchema = child.getTableSchema

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    relBuilder.filter(condition.toRexNode(relBuilder)).build()
  }
}

case class Aggregate(
    groupingExpressions: JList[PlannerExpression],
    aggregateExpressions: JList[PlannerExpression],
    child: TableOperation) extends UnaryNode {

  override def output: Seq[Attribute] = {
    (groupingExpressions.asScala ++ aggregateExpressions.asScala) map {
      case ne: NamedExpression => ne.toAttribute
      case e => Alias(e, e.toString).toAttribute
    }
  }

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    relBuilder.aggregate(
      relBuilder.groupKey(groupingExpressions.asScala.map(_.toRexNode(relBuilder)).asJava),
      aggregateExpressions.asScala.map {
        case Alias(agg: Aggregation, name, _) => agg.toAggCall(name)(relBuilder)
        case _ => throw new RuntimeException("This should never happen.")
      }.asJava).build()
  }
}

case class Join(
    left: TableOperation,
    right: TableOperation,
    joinType: JoinType,
    condition: Optional[PlannerExpression],
    correlated: Boolean) extends BinaryNode {

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

  def resolveCondition(): Option[PlannerExpression] = {
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

    val resolvedCondition = resolveCondition()
    relBuilder.join(
      convertJoinType(joinType),
      resolvedCondition.map(_.toRexNode(relBuilder)).getOrElse(relBuilder.literal(true)),
      corSet.asJava).build()
  }

  private def convertJoinType(joinType: JoinType) = joinType match {
    case JoinType.INNER => JoinRelType.INNER
    case JoinType.LEFT_OUTER => JoinRelType.LEFT
    case JoinType.RIGHT_OUTER => JoinRelType.RIGHT
    case JoinType.FULL_OUTER => JoinRelType.FULL
  }
}

case class CatalogNode(
    tablePath: Seq[String],
    rowType: RelDataType) extends LeafNode {

  override def getTableSchema: TableSchema = new TableSchema(
    rowType.getFieldNames.asScala.toArray,
    rowType.getFieldList.asScala.map(f => FlinkTypeFactory.toTypeInfo(f.getType)).toArray
  )

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    relBuilder.scan(tablePath.asJava).build()
  }
}

/**
  * Wrapper for valid logical plans generated from SQL String.
  */
case class LogicalRelNode(
    relNode: RelNode) extends LeafNode {

  override def getTableSchema: TableSchema = new TableSchema(
    relNode.getRowType.getFieldNames.asScala.toArray,
    relNode.getRowType.getFieldList.asScala.map(f => FlinkTypeFactory.toTypeInfo(f.getType)).toArray
  )

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    relBuilder.push(relNode).build()
  }
}

case class WindowAggregate(
    groupingExpressions: JList[PlannerExpression],
    window: LogicalWindow,
    propertyExpressions: JList[PlannerExpression],
    aggregateExpressions: JList[PlannerExpression],
    child: TableOperation)
  extends UnaryNode {

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
}

/**
  * LogicalNode for calling a user-defined table functions.
  *
  * @param tableFunction table function to be called (might be overloaded)
  * @param parameters actual parameters
  * @param fieldNames output field names
  */
case class CalculatedTable(
    tableFunction: TableFunction[_],
    parameters: JList[PlannerExpression],
    resultType: TypeInformation[_],
    fieldNames: Array[String])
  extends LeafNode {

  private val (generatedNames, _, fieldTypes) = getFieldInfo(resultType)

  override def output: Seq[Attribute] = {
    if (fieldNames.isEmpty) {
      generatedNames.zip(fieldTypes).map {
        case (n, t) => ResolvedFieldReference(n, t)
      }
    } else {
      fieldNames.zip(fieldTypes).map {
        case (n, t) => ResolvedFieldReference(n, t)
      }
    }
  }

  override def toRelNode(relBuilder: RelBuilder): RelNode = {
    val fieldIndexes = getFieldInfo(resultType)._2
    val function = new FlinkTableFunctionImpl(
      resultType,
      fieldIndexes,
      if (fieldNames.isEmpty) generatedNames else fieldNames
    )
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val sqlFunction = new TableSqlFunction(
      tableFunction.functionIdentifier,
      tableFunction.toString,
      tableFunction,
      resultType,
      typeFactory,
      function)

    LogicalTableFunctionScan.create(
      relBuilder.peek().getCluster,
      Collections.emptyList(),
      relBuilder.call(sqlFunction, parameters.asScala.map(_.toRexNode(relBuilder)).asJava),
      function.getElementType(null),
      function.getRowType(relBuilder.getTypeFactory, null),
      null)
  }
}
