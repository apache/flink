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

import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.table.expressions.{Attribute, Expression, NamedExpression}
import org.apache.flink.api.table.trees.TreeNode
import org.apache.flink.api.table.validate.ValidationException

abstract class LogicalNode extends TreeNode[LogicalNode] {
  def output: Seq[Attribute]
  def toRelNode(relBuilder: RelBuilder): RelBuilder

  lazy val resolved: Boolean = childrenResolved && expressions.forall(_.valid)

  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
    * Resolves the given strings to a [[NamedExpression]] using the input from all child
    * nodes of this LogicalPlan.
    */
  def resolveChildren(name: String): Option[NamedExpression] =
    resolve(name, children.flatMap(_.output))

  /**
    *  Performs attribute resolution given a name and a sequence of possible attributes.
    */
  def resolve(name: String, input: Seq[Attribute]): Option[NamedExpression] = {
    // find all matches in input
    val candidates = input.filter(_.name.equalsIgnoreCase(name))
    if (candidates.length > 1) {
      throw new ValidationException(s"Reference $name is ambiguous")
    } else if (candidates.length == 0) {
      None
    } else {
      Some(candidates.head.withName(name))
    }
  }

  def expressions: Seq[Expression] = {
    // Recursively find all expressions from a traversable.
    def seqToExpressions(seq: Traversable[Any]): Traversable[Expression] = seq.flatMap {
      case e: Expression => e :: Nil
      case s: Traversable[_] => seqToExpressions(s)
      case other => Nil
    }

    productIterator.flatMap {
      case e: Expression => e :: Nil
      case Some(e: Expression) => e :: Nil
      case seq: Traversable[_] => seqToExpressions(seq)
      case other => Nil
    }.toSeq
  }

  /**
    * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
    * @param rule the rule to be applied to every expression in this operator.
    */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): LogicalNode = {
    var changed = false

    @inline def transformExpressionDown(e: Expression): Expression = {
      val newE = e.transformDown(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    val newArgs = productIterator.map {
      case e: Expression => transformExpressionDown(e)
      case Some(e: Expression) => Some(transformExpressionDown(e))
      case seq: Traversable[_] => seq.map {
        case e: Expression => transformExpressionDown(e)
        case other => other
      }
      case other: AnyRef => other
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /**
    * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
    * @param rule the rule to be applied to every expression in this operator.
    * @return
    */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): LogicalNode = {
    var changed = false

    @inline def transformExpressionUp(e: Expression): Expression = {
      val newE = e.transformUp(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    val newArgs = productIterator.map {
      case e: Expression => transformExpressionUp(e)
      case Some(e: Expression) => Some(transformExpressionUp(e))
      case seq: Traversable[_] => seq.map {
        case e: Expression => transformExpressionUp(e)
        case other => other
      }
      case other: AnyRef => other
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }
}

abstract class LeafNode extends LogicalNode {
  override def children: Seq[LogicalNode] = Nil
}

abstract class UnaryNode extends LogicalNode {
  def child: LogicalNode

  override def children: Seq[LogicalNode] = child :: Nil
}

abstract class BinaryNode extends LogicalNode {
  def left: LogicalNode
  def right: LogicalNode

  override def children: Seq[LogicalNode] = left :: right :: Nil
}
