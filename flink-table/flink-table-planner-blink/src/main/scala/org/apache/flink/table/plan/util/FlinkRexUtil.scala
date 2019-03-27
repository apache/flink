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

import com.google.common.base.Function
import com.google.common.collect.{ImmutableList, Lists}
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.{SqlKind, SqlOperator}
import org.apache.calcite.util.{ControlFlowException, Util}

import java.lang.Iterable
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Utility methods concerning [[RexNode]].
  */
object FlinkRexUtil {

  /**
    * Similar to [[RexUtil#toCnf(RexBuilder, Int, RexNode)]]; it lets you
    * specify a threshold in the number of nodes that can be created out of
    * the conversion. however, if the threshold is a negative number,
    * this method will give a default threshold value that is double of
    * the number of RexCall in the given node.
    *
    * <p>If the number of resulting RexCalls exceeds that threshold,
    * stops conversion and returns the original expression.
    *
    * <p>Leaf nodes(e.g. RexInputRef) in the expression do not count towards the threshold.
    *
    * <p>We strongly discourage use the [[RexUtil#toCnf(RexBuilder, RexNode)]] and
    * [[RexUtil#toCnf(RexBuilder, Int, RexNode)]], because there are many bad case when using
    * [[RexUtil#toCnf(RexBuilder, RexNode)]], such as predicate in TPC-DS q41.sql will be
    * converted to extremely complex expression (including 736450 RexCalls); and we can not give
    * an appropriate value for `maxCnfNodeCount` when using
    * [[RexUtil#toCnf(RexBuilder, Int, RexNode)]].
    */
  def toCnf(rexBuilder: RexBuilder, maxCnfNodeCount: Int, rex: RexNode): RexNode = {
    val maxCnfNodeCnt = if (maxCnfNodeCount < 0) {
      getNumberOfRexCall(rex) * 2
    } else {
      maxCnfNodeCount
    }
    new CnfHelper(rexBuilder, maxCnfNodeCnt).toCnf(rex)
  }

  /**
    * Get the number of RexCall in the given node.
    */
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
    private class OverflowError extends ControlFlowException {
    }

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
          operands.foreach { node =>
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
          headCnfs.foreach { h =>
            tailCnfs.foreach {
              t => list.add(or(ImmutableList.of(h, t)))
            }
          }
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

    private def and(nodes: Iterable[_ <: RexNode]): RexNode =
      RexUtil.composeConjunction(rexBuilder, nodes, false)

    private def or(nodes: Iterable[_ <: RexNode]): RexNode =
      RexUtil.composeDisjunction(rexBuilder, nodes)
  }

  /**
    * Adjust the condition's field indices according to mapOldToNewIndex.
    *
    * @param c The condition to be adjusted.
    * @param fieldsOldToNewIndexMapping A map containing the mapping the old field indices to new
    *   field indices.
    * @param rowType The row type of the new output.
    * @return Return new condition with new field indices.
    */
  private[flink] def adjustInputRefs(
      c: RexNode,
      fieldsOldToNewIndexMapping: Map[Int, Int],
      rowType: RelDataType) = c.accept(
    new RexShuttle() {

      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        assert(fieldsOldToNewIndexMapping.containsKey(inputRef.getIndex))
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

  private class EquivalentExprShuttle(rexBuilder: RexBuilder) extends RexShuttle {
    private val equiExprMap = mutable.HashMap[String, RexNode]()

    override def visitCall(call: RexCall): RexNode = {
      call.getOperator match {
        case EQUALS | NOT_EQUALS | GREATER_THAN | LESS_THAN |
             GREATER_THAN_OR_EQUAL | LESS_THAN_OR_EQUAL =>
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

  /**
    * Returns whether a given expression has dynamic function.
    *
    * @param e Expression
    * @return true if tree has dynamic function, false otherwise
    */
  def hasDynamicFunction(e: RexNode): Boolean = try {
    val visitor = new RexVisitorImpl[Void](true) {
      override def visitCall(call: RexCall): Void = {
        if (call.getOperator.isDynamicFunction) {
          throw Util.FoundOne.NULL
        }
        super.visitCall(call)
      }
    }
    e.accept(visitor)
    false
  } catch {
    case ex: Util.FoundOne =>
      Util.swallow(ex, null)
      true
  }

  /**
    * Return true if the given RexNode is null or does not have
    * non-deterministic `SqlOperator` and dynamic function `SqlOperator`.
    */
  def isDeterministicOperator(rex: RexNode): Boolean = {
    if (rex == null) {
      true
    } else {
      RexUtil.isDeterministic(rex) && !FlinkRexUtil.hasDynamicFunction(rex)
    }
  }

  /**
    * Return true if the given operator is null or is deterministic and none dynamic function.
    */
  def isDeterministicOperator(op: SqlOperator): Boolean = {
    if (op == null) {
      true
    } else {
      op.isDeterministic && !op.isDynamicFunction
    }
  }
}
