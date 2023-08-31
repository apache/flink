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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.annotation.Experimental
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.table.planner.functions.sql.SqlTryCastFunction
import org.apache.flink.table.planner.plan.utils.ExpressionDetail.ExpressionDetail
import org.apache.flink.table.planner.plan.utils.ExpressionFormat.ExpressionFormat
import org.apache.flink.table.planner.utils.{ShortcutUtils, TableConfigUtils}

import com.google.common.base.Function
import com.google.common.collect.{ImmutableList, Lists}
import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.plan.{RelOptPredicateList, RelOptUtil}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlAsOperator, SqlKind, SqlOperator}
import org.apache.calcite.sql.fun.{SqlCastFunction, SqlStdOperatorTable}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util._

import java.lang.{Iterable => JIterable}
import java.math.BigDecimal
import java.util
import java.util.{Optional, TimeZone}
import java.util.function.Predicate

import scala.collection.JavaConversions._
import scala.collection.mutable

/** Utility methods concerning [[RexNode]]. */
object FlinkRexUtil {

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_CNF_NODES_LIMIT: ConfigOption[Integer] =
    key("table.optimizer.cnf-nodes-limit")
      .intType()
      .defaultValue(Integer.valueOf(-1))
      .withDescription(
        "When converting to conjunctive normal form (CNF, like '(a AND b) OR" +
          " c' will be converted to '(a OR c) AND (b OR c)'), fail if the expression  exceeds " +
          "this threshold; (e.g. predicate in TPC-DS q41.sql will be converted to hundreds of " +
          "thousands of CNF nodes.) the threshold is expressed in terms of number of nodes " +
          "(only count RexCall node, including leaves and interior nodes). " +
          "Negative number to use the default threshold: double of number of nodes.")

  /**
   * Similar to [[RexUtil#toCnf(RexBuilder, Int, RexNode)]]; it lets you specify a threshold in the
   * number of nodes that can be created out of the conversion. however, if the threshold is a
   * negative number, this method will give a default threshold value that is double of the number
   * of RexCall in the given node.
   *
   * <p>If the number of resulting RexCalls exceeds that threshold, stops conversion and returns the
   * original expression.
   *
   * <p>Leaf nodes(e.g. RexInputRef) in the expression do not count towards the threshold.
   *
   * <p>We strongly discourage use the [[RexUtil#toCnf(RexBuilder, RexNode)]] and
   * [[RexUtil#toCnf(RexBuilder, Int, RexNode)]], because there are many bad case when using
   * [[RexUtil#toCnf(RexBuilder, RexNode)]], such as predicate in TPC-DS q41.sql will be converted
   * to extremely complex expression (including 736450 RexCalls); and we can not give an appropriate
   * value for `maxCnfNodeCount` when using [[RexUtil#toCnf(RexBuilder, Int, RexNode)]].
   */
  def toCnf(rexBuilder: RexBuilder, maxCnfNodeCount: Int, rex: RexNode): RexNode = {
    val maxCnfNodeCnt = if (maxCnfNodeCount < 0) {
      getNumberOfRexCall(rex) * 2
    } else {
      maxCnfNodeCount
    }
    new CnfHelper(rexBuilder, maxCnfNodeCnt).toCnf(rex)
  }

  /** Returns true if the RexNode contains any node in the given expected [[RexInputRef]] nodes. */
  def containsExpectedInputRef(rex: RexNode, expectedInputRefs: ImmutableBitSet): Boolean = {
    val visitor = new InputRefVisitor
    rex.accept(visitor)
    val inputRefs = ImmutableBitSet.of(visitor.getFields: _*)
    !inputRefs.intersect(expectedInputRefs).isEmpty
  }

  /** Get the number of RexCall in the given node. */
  private def getNumberOfRexCall(rex: RexNode): Int = {
    var numberOfNodes = 0
    rex.accept(new RexVisitorImpl[Unit](true) {
      override def visitCall(call: RexCall): Unit = {
        numberOfNodes += 1
        super.visitCall(call)
      }
    })
    numberOfNodes
  }

  /** Helps [[toCnf]] */
  private class CnfHelper(rexBuilder: RexBuilder, maxNodeCount: Int) {

    /** Exception to catch when we pass the limit. */
    @SuppressWarnings(Array("serial"))
    private class OverflowError extends ControlFlowException {}

    @SuppressWarnings(Array("ThrowableInstanceNeverThrown"))
    private val INSTANCE = new OverflowError

    private val ADD_NOT = new Function[RexNode, RexNode]() {
      override def apply(input: RexNode): RexNode =
        rexBuilder.makeCall(input.getType, SqlStdOperatorTable.NOT, ImmutableList.of(input))
    }

    def toCnf(rex: RexNode): RexNode = try {
      toCnf2(rex)
    } catch {
      case e: OverflowError =>
        Util.swallow(e, null)
        rex
    }

    private def toCnf2(rex: RexNode): RexNode = {
      rex.getKind match {
        case SqlKind.AND =>
          val cnfOperands: util.List[RexNode] = Lists.newArrayList()
          val operands = RexUtil.flattenAnd(rex.asInstanceOf[RexCall].operands)
          operands.foreach {
            node =>
              val cnf = toCnf2(node)
              cnf.getKind match {
                case SqlKind.AND =>
                  cnfOperands.addAll(cnf.asInstanceOf[RexCall].operands)
                case _ =>
                  cnfOperands.add(cnf)
              }
          }
          val node = and(cnfOperands)
          checkCnfRexCallCount(node)
          node
        case SqlKind.OR =>
          val operands = RexUtil.flattenOr(rex.asInstanceOf[RexCall].operands)
          val head = operands.head
          val headCnf = toCnf2(head)
          val headCnfs: util.List[RexNode] = RelOptUtil.conjunctions(headCnf)
          val tail = or(Util.skip(operands))
          val tailCnf: RexNode = toCnf2(tail)
          val tailCnfs: util.List[RexNode] = RelOptUtil.conjunctions(tailCnf)
          val list: util.List[RexNode] = Lists.newArrayList()
          headCnfs.foreach(h => tailCnfs.foreach(t => list.add(or(ImmutableList.of(h, t)))))
          val node = and(list)
          checkCnfRexCallCount(node)
          node
        case SqlKind.NOT =>
          val arg = rex.asInstanceOf[RexCall].operands.head
          arg.getKind match {
            case SqlKind.NOT =>
              toCnf2(arg.asInstanceOf[RexCall].operands.head)
            case SqlKind.OR =>
              val operands = arg.asInstanceOf[RexCall].operands
              toCnf2(and(Lists.transform(RexUtil.flattenOr(operands), ADD_NOT)))
            case SqlKind.AND =>
              val operands = arg.asInstanceOf[RexCall].operands
              toCnf2(or(Lists.transform(RexUtil.flattenAnd(operands), ADD_NOT)))
            case _ => rex
          }
        case _ => rex
      }
    }

    private def checkCnfRexCallCount(node: RexNode): Unit = {
      // TODO use more efficient solution to get number of RexCall in CNF node
      if (maxNodeCount >= 0 && getNumberOfRexCall(node) > maxNodeCount) {
        throw INSTANCE
      }
    }

    private def and(nodes: JIterable[_ <: RexNode]): RexNode =
      RexUtil.composeConjunction(rexBuilder, nodes, false)

    private def or(nodes: JIterable[_ <: RexNode]): RexNode =
      RexUtil.composeDisjunction(rexBuilder, nodes)
  }

  /**
   * Merges same expressions and then simplifies the result expression by [[RexSimplify]].
   *
   * Examples for merging same expressions:
   *   1. a = b AND b = a -> a = b 2. a = b OR b = a -> a = b 3. (a > b AND c < 10) AND b < a -> a >
   *      b AND c < 10 4. (a > b OR c < 10) OR b < a -> a > b OR c < 10 5. a = a, a >= a, a <= a ->
   *      true 6. a <> a, a > a, a < a -> false
   */
  def simplify(rexBuilder: RexBuilder, expr: RexNode, executor: RexExecutor): RexNode = {
    if (expr.isAlwaysTrue || expr.isAlwaysFalse) {
      return expr
    }

    val exprShuttle = new EquivalentExprShuttle(rexBuilder)
    val equiExpr = expr.accept(exprShuttle)
    val exprMerger = new SameExprMerger(rexBuilder)
    val sameExprMerged = exprMerger.mergeSameExpr(equiExpr)
    val binaryComparisonExprReduced =
      sameExprMerged.accept(new BinaryComparisonExprReducer(rexBuilder))

    val rexSimplify = new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor)
    rexSimplify.simplifyUnknownAs(binaryComparisonExprReduced, RexUnknownAs.falseIf(true))
  }

  val BINARY_COMPARISON: util.Set[SqlKind] = util.EnumSet.of(
    SqlKind.EQUALS,
    SqlKind.NOT_EQUALS,
    SqlKind.GREATER_THAN,
    SqlKind.GREATER_THAN_OR_EQUAL,
    SqlKind.LESS_THAN,
    SqlKind.LESS_THAN_OR_EQUAL)

  private class BinaryComparisonExprReducer(rexBuilder: RexBuilder) extends RexShuttle {
    override def visitCall(call: RexCall): RexNode = {
      val kind = call.getOperator.getKind
      if (!kind.belongsTo(BINARY_COMPARISON)) {
        super.visitCall(call)
      } else {
        val operand0 = call.getOperands.get(0)
        val operand1 = call.getOperands.get(1)
        (operand0, operand1) match {
          case (op0: RexInputRef, op1: RexInputRef) if op0.getIndex == op1.getIndex =>
            kind match {
              case SqlKind.EQUALS | SqlKind.LESS_THAN_OR_EQUAL | SqlKind.GREATER_THAN_OR_EQUAL =>
                rexBuilder.makeLiteral(true)
              case SqlKind.NOT_EQUALS | SqlKind.LESS_THAN | SqlKind.GREATER_THAN =>
                rexBuilder.makeLiteral(false)
              case _ => super.visitCall(call)
            }
          case _ => super.visitCall(call)
        }
      }
    }
  }

  private class SameExprMerger(rexBuilder: RexBuilder) extends RexShuttle {
    private val sameExprMap = mutable.HashMap[String, RexNode]()

    private def mergeSameExpr(expr: RexNode, equiExpr: RexLiteral): RexNode = {
      if (sameExprMap.contains(expr.toString)) {
        equiExpr
      } else {
        sameExprMap.put(expr.toString, expr)
        expr
      }
    }

    def mergeSameExpr(expr: RexNode): RexNode = {
      // merges same expressions in the operands of AND and OR
      // e.g. a = b AND a = b -> a = b AND true
      //      a = b OR a = b -> a = b OR false
      val newExpr1 = expr.accept(this)

      // merges same expressions in conjunctions
      // e.g. (a > b AND c < 10) AND a > b -> a > b AND c < 10 AND true
      sameExprMap.clear()
      val newConjunctions = RelOptUtil.conjunctions(newExpr1).map {
        ex => mergeSameExpr(ex, rexBuilder.makeLiteral(true))
      }
      val newExpr2 = newConjunctions.size match {
        case 0 => newExpr1 // true AND true
        case 1 => newConjunctions.head
        case _ => rexBuilder.makeCall(AND, newConjunctions: _*)
      }

      // merges same expressions in disjunctions
      // e.g. (a > b OR c < 10) OR a > b -> a > b OR c < 10 OR false
      sameExprMap.clear()
      val newDisjunctions = RelOptUtil.disjunctions(newExpr2).map {
        ex => mergeSameExpr(ex, rexBuilder.makeLiteral(false))
      }
      val newExpr3 = newDisjunctions.size match {
        case 0 => newExpr2 // false OR false
        case 1 => newDisjunctions.head
        case _ => rexBuilder.makeCall(OR, newDisjunctions: _*)
      }
      newExpr3
    }

    override def visitCall(call: RexCall): RexNode = {
      val newCall = call.getOperator match {
        case AND | OR =>
          sameExprMap.clear()
          val newOperands = call.getOperands.map {
            op =>
              val value = if (call.getOperator == AND) true else false
              mergeSameExpr(op, rexBuilder.makeLiteral(value))
          }
          call.clone(call.getType, newOperands)
        case _ => call
      }
      super.visitCall(newCall)
    }
  }

  /**
   * Find all inputRefs.
   *
   * @return
   *   InputRef HashSet.
   */
  def findAllInputRefs(node: RexNode): util.HashSet[RexInputRef] = {
    val set = new util.HashSet[RexInputRef]
    node.accept(new RexVisitorImpl[Void](true) {
      override def visitInputRef(inputRef: RexInputRef): Void = {
        set.add(inputRef)
        null
      }
    })
    set
  }

  /**
   * Adjust the expression's field indices according to fieldsOldToNewIndexMapping.
   *
   * @param expr
   *   The expression to be adjusted.
   * @param fieldsOldToNewIndexMapping
   *   A map containing the mapping the old field indices to new field indices.
   * @param rowType
   *   The row type of the new output.
   * @return
   *   Return new expression with new field indices.
   */
  private[flink] def adjustInputRef(
      expr: RexNode,
      fieldsOldToNewIndexMapping: Map[Int, Int],
      rowType: RelDataType): RexNode = expr.accept(new RexShuttle() {

    override def visitInputRef(inputRef: RexInputRef): RexNode = {
      require(fieldsOldToNewIndexMapping.containsKey(inputRef.getIndex))
      val newIndex = fieldsOldToNewIndexMapping(inputRef.getIndex)
      val ref = RexInputRef.of(newIndex, rowType)
      if (ref.getIndex == inputRef.getIndex && (ref.getType eq inputRef.getType)) {
        inputRef
      } else {
        // re-use old object, to prevent needless expr cloning
        ref
      }
    }
  })

  /** Expands the RexProgram to projection list and condition. */
  def expandRexProgram(program: RexProgram): (Seq[RexNode], Option[RexNode]) = {
    val projection = program.getProjectList.map(program.expandLocalRef)
    val filter = if (program.getCondition != null) {
      Some(program.expandLocalRef(program.getCondition))
    } else {
      None
    }
    (projection, filter)
  }

  /** Expands the SEARCH into normal disjunctions recursively. */
  def expandSearch(rexBuilder: RexBuilder, rex: RexNode): RexNode = {
    expandSearch(rexBuilder, rex, _ => true)
  }

  /** Expands the SEARCH into normal disjunctions recursively. */
  def expandSearch(rexBuilder: RexBuilder, rex: RexNode, tester: RexCall => Boolean): RexNode = {
    val shuttle = new RexShuttle() {
      override def visitCall(call: RexCall): RexNode = {
        if (call.getKind == SqlKind.SEARCH && tester(call)) {
          RexUtil.expandSearch(rexBuilder, null, call)
        } else {
          super.visitCall(call)
        }
      }
    }
    rex.accept(shuttle)
  }

  /**
   * Adjust the expression's field indices according to fieldsOldToNewIndexMapping.
   *
   * @param expr
   *   The expression to be adjusted.
   * @param fieldsOldToNewIndexMapping
   *   A map containing the mapping the old field indices to new field indices.
   * @return
   *   Return new expression with new field indices.
   */
  private[flink] def adjustInputRef(
      expr: RexNode,
      fieldsOldToNewIndexMapping: Map[Int, Int]): RexNode = expr.accept(new RexShuttle() {
    override def visitInputRef(inputRef: RexInputRef): RexNode = {
      require(fieldsOldToNewIndexMapping.containsKey(inputRef.getIndex))
      val newIndex = fieldsOldToNewIndexMapping(inputRef.getIndex)
      new RexInputRef(newIndex, inputRef.getType)
    }
  })

  private class EquivalentExprShuttle(rexBuilder: RexBuilder) extends RexShuttle {
    private val equiExprMap = mutable.HashMap[String, RexNode]()

    override def visitCall(call: RexCall): RexNode = {
      call.getOperator match {
        case EQUALS | NOT_EQUALS | GREATER_THAN | LESS_THAN | GREATER_THAN_OR_EQUAL |
            LESS_THAN_OR_EQUAL =>
          val swapped = swapOperands(call)
          if (equiExprMap.contains(swapped.toString)) {
            swapped
          } else {
            equiExprMap.put(call.toString, call)
            call
          }
        case _ => super.visitCall(call)
      }
    }

    private def swapOperands(call: RexCall): RexCall = {
      val newOp = call.getOperator match {
        case EQUALS | NOT_EQUALS => call.getOperator
        case GREATER_THAN => LESS_THAN
        case GREATER_THAN_OR_EQUAL => LESS_THAN_OR_EQUAL
        case LESS_THAN => GREATER_THAN
        case LESS_THAN_OR_EQUAL => GREATER_THAN_OR_EQUAL
        case _ => throw new IllegalArgumentException(s"Unsupported operator: ${call.getOperator}")
      }
      val operands = call.getOperands
      rexBuilder.makeCall(newOp, operands.last, operands.head).asInstanceOf[RexCall]
    }
  }

  def getExpressionString(expr: RexNode, inFields: Seq[String]): String = {
    getExpressionString(expr, inFields, ExpressionDetail.Digest)
  }

  def getExpressionString(
      expr: RexNode,
      inFields: Seq[String],
      expressionDetail: ExpressionDetail): String = {
    getExpressionString(
      expr,
      inFields,
      Option.empty[List[RexNode]],
      ExpressionFormat.Prefix,
      expressionDetail)
  }

  def getExpressionString(
      expr: RexNode,
      inFields: Seq[String],
      localExprsTable: Option[List[RexNode]],
      expressionFormat: ExpressionFormat,
      expressionDetail: ExpressionDetail): String = {
    expr match {
      case pr: RexPatternFieldRef =>
        val alpha = pr.getAlpha
        val field = inFields(pr.getIndex)
        s"$alpha.$field"

      case i: RexInputRef =>
        inFields(i.getIndex)

      case l: RexLiteral =>
        expressionDetail match {
          case ExpressionDetail.Digest =>
            // the digest for the literal
            l.toString
          case ExpressionDetail.Explain =>
            val value = l.getValue
            if (value == null) {
              // return null with type
              "null:" + l.getType
            } else {
              l.getTypeName match {
                case SqlTypeName.DOUBLE => Util.toScientificNotation(value.asInstanceOf[BigDecimal])
                case SqlTypeName.BIGINT => s"${value.asInstanceOf[BigDecimal].longValue()}L"
                case SqlTypeName.BINARY => s"X'${value.asInstanceOf[ByteString].toString(16)}'"
                case SqlTypeName.VARCHAR | SqlTypeName.CHAR =>
                  s"'${value.asInstanceOf[NlsString].getValue}'"
                case SqlTypeName.TIME | SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE =>
                  l.getValueAs(classOf[TimeString]).toString
                case SqlTypeName.DATE =>
                  l.getValueAs(classOf[DateString]).toString
                case SqlTypeName.TIMESTAMP | SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
                  l.getValueAs(classOf[TimestampString]).toString
                case typ if SqlTypeName.INTERVAL_TYPES.contains(typ) => l.toString
                case _ => value.toString
              }
            }
        }

      case _: RexLocalRef if localExprsTable.isEmpty =>
        throw new IllegalArgumentException(
          "Encountered RexLocalRef without " +
            "local expression table")

      case l: RexLocalRef =>
        val lExpr = localExprsTable.get(l.getIndex)
        getExpressionString(lExpr, inFields, localExprsTable, expressionFormat, expressionDetail)

      case c: RexCall =>
        val op = c.getOperator.toString
        val ops = c.getOperands.map(
          getExpressionString(_, inFields, localExprsTable, expressionFormat, expressionDetail))
        c.getOperator match {
          case _: SqlAsOperator => ops.head
          case _: SqlCastFunction | _: SqlTryCastFunction =>
            val typeStr = expressionDetail match {
              case ExpressionDetail.Digest => c.getType.getFullTypeString
              case ExpressionDetail.Explain => c.getType.toString
            }
            s"$op(${ops.head} AS $typeStr)"
          case _ =>
            if (ops.size() == 1) {
              val operand = ops.head
              expressionFormat match {
                case ExpressionFormat.Infix =>
                  c.getKind match {
                    case SqlKind.IS_FALSE | SqlKind.IS_NOT_FALSE | SqlKind.IS_TRUE |
                        SqlKind.IS_NOT_TRUE | SqlKind.IS_UNKNOWN | SqlKind.IS_NULL |
                        SqlKind.IS_NOT_NULL =>
                      s"$operand $op"
                    case _ => s"$op($operand)"
                  }
                case ExpressionFormat.PostFix => s"$operand $op"
                case ExpressionFormat.Prefix => s"$op($operand)"
              }
            } else {
              c.getKind match {
                case SqlKind.TIMES | SqlKind.DIVIDE | SqlKind.PLUS | SqlKind.MINUS |
                    SqlKind.LESS_THAN | SqlKind.LESS_THAN_OR_EQUAL | SqlKind.GREATER_THAN |
                    SqlKind.GREATER_THAN_OR_EQUAL | SqlKind.EQUALS | SqlKind.NOT_EQUALS |
                    SqlKind.OR | SqlKind.AND =>
                  expressionFormat match {
                    case ExpressionFormat.Infix => s"(${ops.mkString(s" $op ")})"
                    case ExpressionFormat.PostFix => s"(${ops.mkString(", ")})$op"
                    case ExpressionFormat.Prefix => s"$op(${ops.mkString(", ")})"
                  }
                case _ => s"$op(${ops.mkString(", ")})"
              }
            }
        }

      case fa: RexFieldAccess =>
        val referenceExpr = getExpressionString(
          fa.getReferenceExpr,
          inFields,
          localExprsTable,
          expressionFormat,
          expressionDetail)
        val field = fa.getField.getName
        s"$referenceExpr.$field"
      case cv: RexCorrelVariable =>
        cv.toString
      case _ =>
        throw new IllegalArgumentException(s"Unknown expression type '${expr.getClass}': $expr")
    }
  }

  /**
   * Similar to [[RexUtil#findOperatorCall(SqlOperator, RexNode)]], but with a broader predicate
   * support and returning a boolean.
   */
  def hasOperatorCallMatching(expr: RexNode, predicate: Predicate[SqlOperator]): Boolean = {
    try {
      val visitor = new RexVisitorImpl[Void](true) {
        override def visitCall(call: RexCall): Void = {
          if (predicate.test(call.getOperator)) {
            throw new Util.FoundOne(call)
          }
          super.visitCall(call)
        }
      }
      expr.accept(visitor)
    } catch {
      case e: Util.FoundOne =>
        Util.swallow(e, null)
        return true
    }
    false
  }

  /**
   * Returns the non-deterministic call name for a given expression. Use java [[Optional]] for
   * scala-free goal.
   */
  def getNonDeterministicCallName(e: RexNode): Optional[String] = try {
    val visitor = new RexVisitorImpl[Void](true) {
      override def visitCall(call: RexCall): Void = {
        if (!call.getOperator.isDeterministic) {
          throw new Util.FoundOne(call.getOperator.getName)
        }
        super.visitCall(call)
      }
    }
    e.accept(visitor)
    Optional.empty[String]
  } catch {
    case ex: Util.FoundOne =>
      Util.swallow(ex, null)
      Optional.ofNullable(ex.getNode.toString)
  }

  /**
   * Returns whether a given [[RexProgram]] is deterministic.
   *
   * @return
   *   false if any expression of the program is not deterministic
   */
  def isDeterministic(rexProgram: RexProgram): Boolean = try {
    if (null != rexProgram.getCondition) {
      val rexCondi = rexProgram.expandLocalRef(rexProgram.getCondition)
      if (!RexUtil.isDeterministic(rexCondi)) {
        return false
      }
    }
    val projects = rexProgram.getProjectList.map(rexProgram.expandLocalRef)
    projects.forall(RexUtil.isDeterministic)
  }

  /**
   * Return convertible rex nodes and unconverted rex nodes extracted from the filter expression.
   */
  def extractPredicates(
      inputNames: Array[String],
      filterExpression: RexNode,
      rel: RelNode,
      rexBuilder: RexBuilder): (Array[RexNode], Array[RexNode]) = {
    val context = ShortcutUtils.unwrapContext(rel)
    val maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(rel);
    val converter =
      new RexNodeToExpressionConverter(
        rexBuilder,
        inputNames,
        context.getFunctionCatalog,
        context.getCatalogManager,
        TimeZone.getTimeZone(TableConfigUtils.getLocalTimeZone(context.getTableConfig)),
        Some(rel.getRowType));

    RexNodeExtractor.extractConjunctiveConditions(
      filterExpression,
      maxCnfNodeCount,
      rexBuilder,
      converter);
  }
}

/**
 * Infix, Postfix and Prefix notations are three different but equivalent ways of writing
 * expressions. It is easiest to demonstrate the differences by looking at examples of operators
 * that take two operands. Infix notation: (X + Y) Postfix notation: (X Y) + Prefix notation: + (X
 * Y)
 */
object ExpressionFormat extends Enumeration {
  type ExpressionFormat = Value
  val Infix, PostFix, Prefix = Value
}

/** ExpressionDetail defines the types of details for expression string result. */
object ExpressionDetail extends Enumeration {
  type ExpressionDetail = Value
  val Explain, Digest = Value
}
