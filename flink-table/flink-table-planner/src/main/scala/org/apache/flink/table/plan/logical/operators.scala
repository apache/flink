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

import java.lang.reflect.Method
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
import org.apache.flink.table.expressions.PlannerExpressionUtils.isRowCountLiteral
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.table.validate.{ValidationFailure, ValidationSuccess}

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

case class Sort(order: Seq[PlannerExpression], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.sort(order.map(_.toRexNode(relBuilder)).asJava)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      failValidation(s"Sort on stream tables is currently not supported.")
    }
    this
  }
}

case class Limit(offset: Int, fetch: Int = -1, child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.limit(offset, fetch)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      failValidation(s"Limit on stream tables is currently not supported.")
    }
    if (!child.isInstanceOf[Sort]) {
      failValidation(s"Limit operator must be preceded by an OrderBy operator.")
    }
    if (offset < 0) {
      failValidation(s"Offset should be greater than or equal to zero.")
    }
    super.validate(tableEnv)
  }
}

case class Filter(condition: PlannerExpression, child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.filter(condition.toRexNode(relBuilder))
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (condition.resultType != BOOLEAN_TYPE_INFO) {
      failValidation(s"Filter operator requires a boolean expression as input," +
        s" but $condition is of type ${condition.resultType}")
    }
    this
  }
}

case class Aggregate(
    groupingExpressions: Seq[PlannerExpression],
    aggregateExpressions: Seq[PlannerExpression],
    child: LogicalNode) extends UnaryNode {

  override def output: Seq[Attribute] = {
    (groupingExpressions ++ aggregateExpressions) map {
      case ne: NamedExpression => ne.toAttribute
      case e => Alias(e, e.toString).toAttribute
    }
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.aggregate(
      relBuilder.groupKey(groupingExpressions.map(_.toRexNode(relBuilder)).asJava),
      aggregateExpressions.map {
        case Alias(agg: Aggregation, name, _) => agg.toAggCall(name)(relBuilder)
        case _ => throw new RuntimeException("This should never happen.")
      }.asJava)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    implicit val relBuilder: RelBuilder = tableEnv.getRelBuilder
    val groupingExprs = groupingExpressions
    val aggregateExprs = aggregateExpressions
    aggregateExprs.foreach(validateAggregateExpression)
    groupingExprs.foreach(validateGroupingExpression)

    def validateAggregateExpression(expr: PlannerExpression): Unit = expr match {
      case distinctExpr: DistinctAgg =>
        distinctExpr.child match {
          case _: DistinctAgg => failValidation(
            "Chained distinct operators are not supported!")
          case aggExpr: Aggregation => validateAggregateExpression(aggExpr)
          case _ => failValidation(
            "Distinct operator can only be applied to aggregation expressions!")
        }
      // check aggregate function
      case aggExpr: Aggregation
        if aggExpr.getSqlAggFunction.requiresOver =>
        failValidation(s"OVER clause is necessary for window functions: [${aggExpr.getClass}].")
      // check no nested aggregation exists.
      case aggExpr: Aggregation =>
        aggExpr.children.foreach { child =>
          child.preOrderVisit {
            case agg: Aggregation =>
              failValidation(
                "It's not allowed to use an aggregate function as " +
                  "input of another aggregate function")
            case _ => // OK
          }
        }
      case a: Attribute if !groupingExprs.exists(_.checkEquals(a)) =>
        failValidation(
          s"expression '$a' is invalid because it is neither" +
            " present in group by nor an aggregate function")
      case e if groupingExprs.exists(_.checkEquals(e)) => // OK
      case e => e.children.foreach(validateAggregateExpression)
    }

    def validateGroupingExpression(expr: PlannerExpression): Unit = {
      if (!expr.resultType.isKeyType) {
        failValidation(
          s"expression $expr cannot be used as a grouping expression " +
            "because it's not a valid key type which must be hashable and comparable")
      }
    }
    this
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
    if (correlated && right.isInstanceOf[LogicalTableFunctionCall] && joinType != JoinType.INNER ) {
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
    groupingExpressions: Seq[PlannerExpression],
    window: LogicalWindow,
    propertyExpressions: Seq[PlannerExpression],
    aggregateExpressions: Seq[PlannerExpression],
    child: LogicalNode)
  extends UnaryNode {

  override def output: Seq[Attribute] = {
    (groupingExpressions ++ aggregateExpressions ++ propertyExpressions) map {
      case ne: NamedExpression => ne.toAttribute
      case e => Alias(e, e.toString).toAttribute
    }
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    val flinkRelBuilder = relBuilder.asInstanceOf[FlinkRelBuilder]
    child.construct(flinkRelBuilder)
    flinkRelBuilder.aggregate(
      window,
      relBuilder.groupKey(groupingExpressions.map(_.toRexNode(relBuilder)).asJava),
      propertyExpressions.map {
        case Alias(prop: WindowProperty, name, _) => prop.toNamedWindowProperty(name)
        case _ => throw new RuntimeException("This should never happen.")
      },
      aggregateExpressions.map {
        case Alias(agg: Aggregation, name, _) => agg.toAggCall(name)(relBuilder)
        case _ => throw new RuntimeException("This should never happen.")
      }.asJava)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    implicit val relBuilder: RelBuilder = tableEnv.getRelBuilder
    val groupingExprs = groupingExpressions
    val aggregateExprs = aggregateExpressions
    aggregateExprs.foreach(validateAggregateExpression)
    groupingExprs.foreach(validateGroupingExpression)

    def validateAggregateExpression(expr: PlannerExpression): Unit = expr match {
      // check aggregate function
      case aggExpr: Aggregation
        if aggExpr.getSqlAggFunction.requiresOver =>
        failValidation(s"OVER clause is necessary for window functions: [${aggExpr.getClass}].")
      case aggExpr: DistinctAgg =>
        validateAggregateExpression(aggExpr.child)
      // check no nested aggregation exists.
      case aggExpr: Aggregation =>PlannerExpressionConverter
        aggExpr.children.foreach { child =>
          child.preOrderVisit {
            case agg: Aggregation =>
              failValidation(
                "It's not allowed to use an aggregate function as " +
                  "input of another aggregate function")
            case _ => // ok
          }
        }
      case a: Attribute if !groupingExprs.exists(_.checkEquals(a)) =>
        failValidation(
          s"Expression '$a' is invalid because it is neither" +
            " present in group by nor an aggregate function")
      case e if groupingExprs.exists(_.checkEquals(e)) => // ok
      case e => e.children.foreach(validateAggregateExpression)
    }

    def validateGroupingExpression(expr: PlannerExpression): Unit = {
      if (!expr.resultType.isKeyType) {
        failValidation(
          s"Expression $expr cannot be used as a grouping expression " +
            "because it's not a valid key type which must be hashable and comparable")
      }
    }

    // validate window
    window.validate(tableEnv) match {
      case ValidationFailure(msg) =>
        failValidation(s"$window is invalid: $msg")
      case ValidationSuccess => // ok
    }

    // validate property
    if (propertyExpressions.nonEmpty) {
      window match {
        case TumblingGroupWindow(_, _, size) if isRowCountLiteral(size) =>
          failValidation("Window start and Window end cannot be selected " +
                           "for a row-count Tumbling window.")

        case SlidingGroupWindow(_, _, size, _) if isRowCountLiteral(size) =>
          failValidation("Window start and Window end cannot be selected " +
                           "for a row-count Sliding window.")

        case _ => // ok
      }
    }

    this
  }
}

/**
  * LogicalNode for calling a user-defined table functions.
  *
  * @param functionName function name
  * @param tableFunction table function to be called (might be overloaded)
  * @param parameters actual parameters
  * @param fieldNames output field names
  * @param child child logical node
  */
case class LogicalTableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[PlannerExpression],
    resultType: TypeInformation[_],
    fieldNames: Array[String],
    child: LogicalNode)
  extends UnaryNode {

  private val (generatedNames, fieldIndexes, fieldTypes) = getFieldInfo(resultType)
  private var evalMethod: Method = _

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

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    // check if not Scala object
    checkNotSingleton(tableFunction.getClass)
    // check if class could be instantiated
    checkForInstantiation(tableFunction.getClass)
    // look for a signature that matches the input types
    val signature = parameters.map(_.resultType)
    val foundMethod = getUserDefinedMethod(tableFunction, "eval", typeInfoToClass(signature))
    if (foundMethod.isEmpty) {
      failValidation(
        s"Given parameters of function '$functionName' do not match any signature. \n" +
          s"Actual: ${signatureToString(signature)} \n" +
          s"Expected: ${signaturesToString(tableFunction, "eval")}")
    } else {
      evalMethod = foundMethod.get
    }
    this
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
      relBuilder.call(sqlFunction, parameters.map(_.toRexNode(relBuilder)).asJava),
      function.getElementType(null),
      function.getRowType(relBuilder.getTypeFactory, null),
      null)

    relBuilder.push(scan)
  }
}
