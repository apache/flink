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
import java.util

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{CorrelationId, JoinRelType}
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.table.api.{StreamTableEnvironment, TableEnvironment, UnresolvedException}
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.expressions.ExpressionUtils.isRowCountLiteral
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.table.validate.{ValidationFailure, ValidationSuccess}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class Project(projectList: Seq[NamedExpression], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def resolveExpressions(tableEnv: TableEnvironment): LogicalNode = {
    val afterResolve = super.resolveExpressions(tableEnv).asInstanceOf[Project]
    val newProjectList =
      afterResolve.projectList.zipWithIndex.map { case (e, i) =>
        e match {
          case u @ UnresolvedAlias(c) => c match {
            case ne: NamedExpression => ne
            case expr if !expr.valid => u
            case c @ Cast(ne: NamedExpression, tp) => Alias(c, s"${ne.name}-$tp")
            case gcf: GetCompositeField => Alias(gcf, gcf.aliasName().getOrElse(s"_c$i"))
            case other => Alias(other, s"_c$i")
          }
          case _ =>
            throw new RuntimeException("This should never be called and probably points to a bug.")
        }
    }
    Project(newProjectList, child)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    val resolvedProject = super.validate(tableEnv).asInstanceOf[Project]
    val names: mutable.Set[String] = mutable.Set()

    def checkName(name: String): Unit = {
      if (names.contains(name)) {
        failValidation(s"Duplicate field name $name.")
      } else {
        names.add(name)
      }
    }

    resolvedProject.projectList.foreach {
      case n: Alias =>
        // explicit name
        checkName(n.name)
      case r: ResolvedFieldReference =>
        // simple field forwarding
        checkName(r.name)
      case _ => // Do nothing
    }
    resolvedProject
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.project(
      projectList.map(_.toRexNode(relBuilder)).asJava,
      projectList.map(_.name).asJava,
      true)
  }
}

case class AliasNode(aliasList: Seq[Expression], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] =
    throw UnresolvedException("Invalid call to output on AliasNode")

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder =
    throw UnresolvedException("Invalid call to toRelNode on AliasNode")

  override def resolveExpressions(tableEnv: TableEnvironment): LogicalNode = {
    if (aliasList.length > child.output.length) {
      failValidation("Aliasing more fields than we actually have")
    } else if (!aliasList.forall(_.isInstanceOf[UnresolvedFieldReference])) {
      failValidation("Alias only accept name expressions as arguments")
    } else if (!aliasList.forall(_.asInstanceOf[UnresolvedFieldReference].name != "*")) {
      failValidation("Alias can not accept '*' as name")
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

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.distinct()
  }
}

case class Sort(order: Seq[Ordering], child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.sort(order.map(_.toRexNode(relBuilder)).asJava)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      failValidation(s"Sort on stream tables is currently not supported.")
    }
    super.validate(tableEnv)
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

case class Filter(condition: Expression, child: LogicalNode) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    child.construct(relBuilder)
    relBuilder.filter(condition.toRexNode(relBuilder))
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    val resolvedFilter = super.validate(tableEnv).asInstanceOf[Filter]
    if (resolvedFilter.condition.resultType != BOOLEAN_TYPE_INFO) {
      failValidation(s"Filter operator requires a boolean expression as input," +
        s" but ${resolvedFilter.condition} is of type ${resolvedFilter.condition.resultType}")
    }
    resolvedFilter
  }
}

case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
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
    val resolvedAggregate = super.validate(tableEnv).asInstanceOf[Aggregate]
    val groupingExprs = resolvedAggregate.groupingExpressions
    val aggregateExprs = resolvedAggregate.aggregateExpressions
    aggregateExprs.foreach(validateAggregateExpression)
    groupingExprs.foreach(validateGroupingExpression)

    def validateAggregateExpression(expr: Expression): Unit = expr match {
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

    def validateGroupingExpression(expr: Expression): Unit = {
      if (!expr.resultType.isKeyType) {
        failValidation(
          s"expression $expr cannot be used as a grouping expression " +
            "because it's not a valid key type which must be hashable and comparable")
      }
    }
    resolvedAggregate
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
    condition: Option[Expression],
    correlated: Boolean) extends BinaryNode {

  override def output: Seq[Attribute] = {
    left.output ++ right.output
  }

  private case class JoinFieldReference(
    name: String,
    resultType: TypeInformation[_],
    left: LogicalNode,
    right: LogicalNode) extends Attribute {

    val isFromLeftInput = left.output.map(_.name).contains(name)

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

  override def resolveExpressions(tableEnv: TableEnvironment): LogicalNode = {
    val node = super.resolveExpressions(tableEnv).asInstanceOf[Join]
    val partialFunction: PartialFunction[Expression, Expression] = {
      case field: ResolvedFieldReference => JoinFieldReference(
        field.name,
        field.resultType,
        left,
        right)
    }
    val resolvedCondition = node.condition.map(_.postOrderTransform(partialFunction))
    Join(node.left, node.right, node.joinType, resolvedCondition, correlated)
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
    if (tableEnv.isInstanceOf[StreamTableEnvironment]
      && !right.isInstanceOf[LogicalTableFunctionCall]) {
      failValidation(s"Join on stream tables is currently not supported.")
    }

    val resolvedJoin = super.validate(tableEnv).asInstanceOf[Join]
    if (!resolvedJoin.condition.forall(_.resultType == BOOLEAN_TYPE_INFO)) {
      failValidation(s"Filter operator requires a boolean expression as input, " +
        s"but ${resolvedJoin.condition} is of type ${resolvedJoin.joinType}")
    } else if (ambiguousName.nonEmpty) {
      failValidation(s"join relations with ambiguous names: ${ambiguousName.mkString(", ")}")
    }

    resolvedJoin.condition.foreach(testJoinCondition)
    resolvedJoin
  }

  private def testJoinCondition(expression: Expression): Unit = {

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
    var nonEquiJoinPredicateFound = false
    var localPredicateFound = false

    def validateConditions(exp: Expression, isAndBranch: Boolean): Unit = exp match {
      case x: And => x.children.foreach(validateConditions(_, isAndBranch))
      case x: Or => x.children.foreach(validateConditions(_, isAndBranch = false))
      case x: EqualTo =>
        if (isAndBranch && checkIfJoinCondition(x)) {
          equiJoinPredicateFound = true
        }
        if (checkIfFilterCondition(x)) {
          localPredicateFound = true
        }
      case x: BinaryComparison =>
        if (checkIfFilterCondition(x)) {
          localPredicateFound = true
        } else {
          nonEquiJoinPredicateFound = true
        }
      case x => failValidation(
        s"Unsupported condition type: ${x.getClass.getSimpleName}. Condition: $x")
    }

    validateConditions(expression, isAndBranch = true)
    if (!equiJoinPredicateFound) {
      failValidation(
        s"Invalid join condition: $expression. At least one equi-join predicate is " +
          s"required.")
    }
    if (joinType != JoinType.INNER && (nonEquiJoinPredicateFound || localPredicateFound)) {
      failValidation(
        s"Invalid join condition: $expression. Non-equality join predicates or local" +
          s" predicates are not supported in outer joins.")
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
    groupingExpressions: Seq[Expression],
    window: LogicalWindow,
    propertyExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalNode)
  extends UnaryNode {

  override def output: Seq[Attribute] = {
    (groupingExpressions ++ aggregateExpressions ++ propertyExpressions) map {
      case ne: NamedExpression => ne.toAttribute
      case e => Alias(e, e.toString).toAttribute
    }
  }

  // resolve references of this operator's parameters
  override def resolveReference(
      tableEnv: TableEnvironment,
      name: String)
    : Option[NamedExpression] = {

    def resolveAlias(alias: String) = {
      // check if reference can already be resolved by input fields
      val found = super.resolveReference(tableEnv, name)
      if (found.isDefined) {
        failValidation(s"Reference $name is ambiguous.")
      } else {
        // resolve type of window reference
        val resolvedType = window.timeAttribute match {
          case UnresolvedFieldReference(n) =>
            super.resolveReference(tableEnv, n) match {
              case Some(ResolvedFieldReference(_, tpe)) => Some(tpe)
              case _ => None
            }
          case _ => None
        }
        // let validation phase throw an error if type could not be resolved
        Some(WindowReference(name, resolvedType))
      }
    }

    window.aliasAttribute match {
      // resolve reference to this window's name
      case UnresolvedFieldReference(alias) if name == alias =>
        resolveAlias(alias)

      case _ =>
        // resolve references as usual
        super.resolveReference(tableEnv, name)
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
    val resolvedWindowAggregate = super.validate(tableEnv).asInstanceOf[WindowAggregate]
    val groupingExprs = resolvedWindowAggregate.groupingExpressions
    val aggregateExprs = resolvedWindowAggregate.aggregateExpressions
    aggregateExprs.foreach(validateAggregateExpression)
    groupingExprs.foreach(validateGroupingExpression)

    def validateAggregateExpression(expr: Expression): Unit = expr match {
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

    def validateGroupingExpression(expr: Expression): Unit = {
      if (!expr.resultType.isKeyType) {
        failValidation(
          s"Expression $expr cannot be used as a grouping expression " +
            "because it's not a valid key type which must be hashable and comparable")
      }
    }

    // validate window
    resolvedWindowAggregate.window.validate(tableEnv) match {
      case ValidationFailure(msg) =>
        failValidation(s"$window is invalid: $msg")
      case ValidationSuccess => // ok
    }

    // validate property
    if (propertyExpressions.nonEmpty) {
      resolvedWindowAggregate.window match {
        case TumblingGroupWindow(_, _, size) if isRowCountLiteral(size) =>
          failValidation("Window start and Window end cannot be selected " +
                           "for a row-count Tumbling window.")

        case SlidingGroupWindow(_, _, size, _) if isRowCountLiteral(size) =>
          failValidation("Window start and Window end cannot be selected " +
                           "for a row-count Sliding window.")

        case _ => // ok
      }
    }

    resolvedWindowAggregate
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
    parameters: Seq[Expression],
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
    val node = super.validate(tableEnv).asInstanceOf[LogicalTableFunctionCall]
    // check if not Scala object
    checkNotSingleton(tableFunction.getClass)
    // check if class could be instantiated
    checkForInstantiation(tableFunction.getClass)
    // look for a signature that matches the input types
    val signature = node.parameters.map(_.resultType)
    val foundMethod = getUserDefinedMethod(tableFunction, "eval", typeInfoToClass(signature))
    if (foundMethod.isEmpty) {
      failValidation(
        s"Given parameters of function '$functionName' do not match any signature. \n" +
          s"Actual: ${signatureToString(signature)} \n" +
          s"Expected: ${signaturesToString(tableFunction, "eval")}")
    } else {
      node.evalMethod = foundMethod.get
    }
    node
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    val fieldIndexes = getFieldInfo(resultType)._2
    val function = new FlinkTableFunctionImpl(
      resultType,
      fieldIndexes,
      if (fieldNames.isEmpty) generatedNames else fieldNames, evalMethod
    )
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val sqlFunction = TableSqlFunction(
      tableFunction.functionIdentifier,
      tableFunction,
      resultType,
      typeFactory,
      function)

    val scan = LogicalTableFunctionScan.create(
      relBuilder.peek().getCluster,
      new util.ArrayList[RelNode](),
      relBuilder.call(sqlFunction, parameters.map(_.toRexNode(relBuilder)).asJava),
      function.getElementType(null),
      function.getRowType(relBuilder.getTypeFactory, null),
      null)

    relBuilder.push(scan)
  }
}
