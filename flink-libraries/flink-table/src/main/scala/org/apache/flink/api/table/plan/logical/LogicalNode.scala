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

import org.apache.flink.api.table.TableEnvironment
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.trees.TreeNode
import org.apache.flink.api.table.validate._

/**
  * LogicalNode is created and validated as we construct query plan using Table API.<p>
  *
  * The main validation procedure is separated into two phases:<p>
  * Expressions' resolution and transformation (#resolveExpressions(TableEnvironment)):
  * <ul>
  *   <li>translate UnresolvedFieldReference into ResolvedFieldReference
  *     using child operator's output</li>
  *   <li>translate Call(UnresolvedFunction) into solid Expression</li>
  *   <li>generate alias names for query output</li>
  *   <li>....</li>
  * </ul>
  *
  * LogicalNode validation (#validate(TableEnvironment)):
  * <ul>
  *   <li>check no UnresolvedFieldReference exists any more</li>
  *   <li>check if all expressions have children of needed type</li>
  *   <li>check each logical operator have desired input</li>
  * </ul>
  * Once we pass the validation phase, we can safely convert LogicalNode into Calcite's RelNode.
  *
  * Note: this is adapted from Apache Spark's LogicalPlan.
  */
abstract class LogicalNode extends TreeNode[LogicalNode] {
  def output: Seq[Attribute]

  def resolveExpressions(tableEnv: TableEnvironment): LogicalNode = {
    // resolve references and function calls
    transformExpressionsUp {
      case u @ UnresolvedFieldReference(name) =>
        resolveChildren(name).getOrElse(u)
      case c @ Call(name, children) if c.childrenValid =>
        tableEnv.getFunctionCatalog.lookupFunction(name, children)
    }
  }

  def toRelNode(relBuilder: RelBuilder): RelBuilder

  def validate(tableEnv: TableEnvironment): LogicalNode = {
    val resolvedNode = resolveExpressions(tableEnv)
    resolvedNode.transformExpressionsUp {
      case a: Attribute if !a.valid =>
        val from = children.flatMap(_.output).map(_.name).mkString(", ")
        failValidation(s"cannot resolve [${a.name}] given input [$from]")

      case e: Expression if e.validateInput().isFailure =>
        e.validateInput() match {
          case ExprValidationResult.ValidationFailure(message) =>
            failValidation(s"Expression $e failed on input check: $message")
        }
    }
  }

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
      failValidation(s"Reference $name is ambiguous")
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

  protected def failValidation(msg: String): Nothing = {
    throw new ValidationException(msg)
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
