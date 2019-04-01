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

import java.util.{Collections, List => JList}

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{CorrelationId, JoinRelType}
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.table.api.{StreamTableEnvironment, TableEnvironment, Types}
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl

import scala.collection.JavaConverters._
import scala.collection.mutable

case class Project(
    projectList: JList[PlannerExpression],
    child: LogicalNode,
    explicitAlias: Boolean = false)
  extends UnaryNode {

  override def output: Seq[Attribute] = projectList.asScala
    .map(_.asInstanceOf[NamedExpression].toAttribute)

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)

    val projectNames = projectList.asScala.map(_.asInstanceOf[NamedExpression].name).asJava
    val exprs = if (explicitAlias) {
      projectList.asScala
    } else {
      // remove AS expressions, according to Calcite they should not be in a final RexNode
      projectList.asScala.map {
        case Alias(e: PlannerExpression, _, _) => e
        case e: PlannerExpression => e
      }
    }

    relBuilder.project(
      exprs.map(_.toRexNode(relBuilder)).asJava,
      projectNames,
      true)
  }
}

case class Distinct(child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.distinct()
  }
}

case class Sort(order: JList[PlannerExpression], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.sort(order.asScala.map(_.toRexNode(relBuilder)).asJava)
  }
}

case class Limit(offset: Int, fetch: Int = -1, child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.limit(offset, fetch)
  }

}

case class Filter(condition: PlannerExpression, child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.filter(condition.toRexNode(relBuilder))
  }
}

case class Aggregate(
    groupingExpressions: JList[PlannerExpression],
    aggregateExpressions: JList[PlannerExpression],
    child: LogicalNode) extends UnaryNode {

  override def output: Seq[Attribute] = {
    (groupingExpressions.asScala ++ aggregateExpressions.asScala) map {
      case ne: NamedExpression => ne.toAttribute
      case e => Alias(e, e.toString).toAttribute
    }
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.aggregate(
      relBuilder.groupKey(groupingExpressions.asScala.map(_.toRexNode(relBuilder)).asJava),
      aggregateExpressions.asScala.map {
        case Alias(agg: Aggregation, name, _) => agg.toAggCall(name)(relBuilder)
        case _ => throw new RuntimeException("This should never happen.")
      }.asJava)
  }
}

case class Minus(left: LogicalNode, right: LogicalNode, all: Boolean) extends BinaryNode {
  override def output: Seq[Attribute] = left.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    left.construct(relBuilder)
    right.construct(relBuilder)
    relBuilder.minus(all)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      failValidation(s"Minus on stream tables is currently not supported.")
    }

    val resolvedMinus = super.validate(tableEnv).asInstanceOf[Minus]
    if (left.output.length != right.output.length) {
      failValidation(s"Minus two table of different column sizes:" +
        s" ${left.output.size} and ${right.output.size}")
    }
    val sameSchema = left.output.zip(right.output).forall { case (l, r) =>
      l.resultType == r.resultType
    }
    if (!sameSchema) {
      failValidation(s"Minus two table of different schema:" +
        s" [${left.output.map(a => (a.name, a.resultType)).mkString(", ")}] and" +
        s" [${right.output.map(a => (a.name, a.resultType)).mkString(", ")}]")
    }
    resolvedMinus
  }
}

case class Union(left: LogicalNode, right: LogicalNode, all: Boolean) extends BinaryNode {
  override def output: Seq[Attribute] = left.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    left.construct(relBuilder)
    right.construct(relBuilder)
    relBuilder.union(all)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (tableEnv.isInstanceOf[StreamTableEnvironment] && !all) {
      failValidation(s"Union on stream tables is currently not supported.")
    }

    val resolvedUnion = super.validate(tableEnv).asInstanceOf[Union]
    if (left.output.length != right.output.length) {
      failValidation(s"Union two tables of different column sizes:" +
        s" ${left.output.size} and ${right.output.size}")
    }
    val sameSchema = left.output.zip(right.output).forall { case (l, r) =>
      l.resultType == r.resultType
    }
    if (!sameSchema) {
      failValidation(s"Union two tables of different schema:" +
        s" [${left.output.map(a => (a.name, a.resultType)).mkString(", ")}] and" +
        s" [${right.output.map(a => (a.name, a.resultType)).mkString(", ")}]")
    }
    resolvedUnion
  }
}

case class Intersect(left: LogicalNode, right: LogicalNode, all: Boolean) extends BinaryNode {
  override def output: Seq[Attribute] = left.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    left.construct(relBuilder)
    right.construct(relBuilder)
    relBuilder.intersect(all)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      failValidation(s"Intersect on stream tables is currently not supported.")
    }

    val resolvedIntersect = super.validate(tableEnv).asInstanceOf[Intersect]
    if (left.output.length != right.output.length) {
      failValidation(s"Intersect two tables of different column sizes:" +
        s" ${left.output.size} and ${right.output.size}")
    }
    // allow different column names between tables
    val sameSchema = left.output.zip(right.output).forall { case (l, r) =>
      l.resultType == r.resultType
    }
    if (!sameSchema) {
      failValidation(s"Intersect two tables of different schema:" +
        s" [${left.output.map(a => (a.name, a.resultType)).mkString(", ")}] and" +
        s" [${right.output.map(a => (a.name, a.resultType)).mkString(", ")}]")
    }
    resolvedIntersect
  }
}

case class Join(
    left: LogicalNode,
    right: LogicalNode,
    joinType: JoinType,
    condition: Option[PlannerExpression],
    correlated: Boolean) extends BinaryNode {

  override def output: Seq[Attribute] = {
    left.output ++ right.output
  }

  private case class JoinFieldReference(
    name: String,
    resultType: TypeInformation[_],
    left: LogicalNode,
    right: LogicalNode) extends Attribute {

    val isFromLeftInput: Boolean = left.output.map(_.name).contains(name)

    val (indexInInput, indexInJoin) = if (isFromLeftInput) {
      val indexInLeft = left.output.map(_.name).indexOf(name)
      (indexInLeft, indexInLeft)
    } else {
      val indexInRight = right.output.map(_.name).indexOf(name)
      (indexInRight, indexInRight + left.output.length)
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
    condition.map(_.postOrderTransform(partialFunction))
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    left.construct(relBuilder)
    right.construct(relBuilder)

    val corSet = mutable.Set[CorrelationId]()
    if (correlated) {
      corSet += relBuilder.peek().getCluster.createCorrel()
    }

    relBuilder.join(
      convertJoinType(joinType),
      condition.map(_.toRexNode(relBuilder)).getOrElse(relBuilder.literal(true)),
      corSet.asJava)
  }

  private def convertJoinType(joinType: JoinType) = joinType match {
    case JoinType.INNER => JoinRelType.INNER
    case JoinType.LEFT_OUTER => JoinRelType.LEFT
    case JoinType.RIGHT_OUTER => JoinRelType.RIGHT
    case JoinType.FULL_OUTER => JoinRelType.FULL
  }

  private def ambiguousName: Set[String] =
    left.output.map(_.name).toSet.intersect(right.output.map(_.name).toSet)

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (!condition.forall(_.resultType == BOOLEAN_TYPE_INFO)) {
      failValidation(s"Filter operator requires a boolean expression as input, " +
        s"but $condition is of type $joinType")
    } else if (ambiguousName.nonEmpty) {
      failValidation(s"join relations with ambiguous names: ${ambiguousName.mkString(", ")}")
    }

    val resolvedCondition = resolveCondition()
    resolvedCondition.foreach(testJoinCondition)
    Join(left, right, joinType, resolvedCondition, correlated)
  }

  private def testJoinCondition(expression: PlannerExpression): Unit = {

    def checkIfJoinCondition(exp: BinaryComparison) = exp.children match {
      case (x: JoinFieldReference) :: (y: JoinFieldReference) :: Nil
        if x.isFromLeftInput != y.isFromLeftInput => true
      case _ => false
    }

    def checkIfFilterCondition(exp: BinaryComparison) = exp.children match {
      case (x: JoinFieldReference) :: (y: JoinFieldReference) :: Nil => false
      case (x: JoinFieldReference) :: (_) :: Nil => true
      case (_) :: (y: JoinFieldReference) :: Nil => true
      case _ => false
    }

    var equiJoinPredicateFound = false
    // Whether the predicate is literal true.
    val alwaysTrue = expression match {
      case x: Literal if x.value.equals(true) => true
      case _ => false
    }

    def validateConditions(exp: PlannerExpression, isAndBranch: Boolean): Unit = exp match {
      case x: And => x.children.foreach(validateConditions(_, isAndBranch))
      case x: Or => x.children.foreach(validateConditions(_, isAndBranch = false))
      case x: EqualTo =>
        if (isAndBranch && checkIfJoinCondition(x)) {
          equiJoinPredicateFound = true
        }
      case x: BinaryComparison =>
      // The boolean literal should be a valid condition type.
      case x: Literal if x.resultType == Types.BOOLEAN =>
      case x => failValidation(
        s"Unsupported condition type: ${x.getClass.getSimpleName}. Condition: $x")
    }

    validateConditions(expression, isAndBranch = true)

    // Due to a bug in Apache Calcite (see CALCITE-2004 and FLINK-7865) we cannot accept join
    // predicates except literal true for TableFunction left outer join.
    if (correlated && right.isInstanceOf[CalculatedTable] && joinType != JoinType.INNER ) {
      if (!alwaysTrue) failValidation("TableFunction left outer join predicate can only be " +
        "empty or literal true.")
    } else {
      if (!equiJoinPredicateFound) {
        failValidation(
          s"Invalid join condition: $expression. At least one equi-join predicate is " +
            s"required.")
      }
    }
  }
}

case class CatalogNode(
    tablePath: Seq[String],
    rowType: RelDataType) extends LeafNode {

  val output: Seq[Attribute] = rowType.getFieldList.asScala.map { field =>
    ResolvedFieldReference(field.getName, FlinkTypeFactory.toTypeInfo(field.getType))
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    relBuilder.scan(tablePath.asJava)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = this
}

/**
  * Wrapper for valid logical plans generated from SQL String.
  */
case class LogicalRelNode(
    relNode: RelNode) extends LeafNode {

  val output: Seq[Attribute] = relNode.getRowType.getFieldList.asScala.map { field =>
    ResolvedFieldReference(field.getName, FlinkTypeFactory.toTypeInfo(field.getType))
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    relBuilder.push(relNode)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = this
}

case class WindowAggregate(
    groupingExpressions: JList[PlannerExpression],
    window: LogicalWindow,
    propertyExpressions: JList[PlannerExpression],
    aggregateExpressions: JList[PlannerExpression],
    child: LogicalNode)
  extends UnaryNode {

  override def output: Seq[Attribute] = {
    val expressions = groupingExpressions.asScala ++ aggregateExpressions.asScala ++
      propertyExpressions.asScala
    expressions map {
      case ne: NamedExpression => ne.toAttribute
      case e => Alias(e, e.toString).toAttribute
    }
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    val flinkRelBuilder = relBuilder.asInstanceOf[FlinkRelBuilder]
    child.construct(flinkRelBuilder)
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
      }.asJava)
  }
}

/**
  * LogicalNode for calling a user-defined table functions.
  *
  * @param tableFunction table function to be called (might be overloaded)
  * @param parameters actual parameters
  * @param fieldNames output field names
  * @param child child logical node
  */
case class CalculatedTable(
    tableFunction: TableFunction[_],
    parameters: JList[PlannerExpression],
    resultType: TypeInformation[_],
    fieldNames: Array[String],
    child: LogicalNode)
  extends UnaryNode {

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

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
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

    val scan = LogicalTableFunctionScan.create(
      relBuilder.peek().getCluster,
      Collections.emptyList(),
      relBuilder.call(sqlFunction, parameters.asScala.map(_.toRexNode(relBuilder)).asJava),
      function.getElementType(null),
      function.getRowType(relBuilder.getTypeFactory, null),
      null)

    relBuilder.push(scan)
  }
}
