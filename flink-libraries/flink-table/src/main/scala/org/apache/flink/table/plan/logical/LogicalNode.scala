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

import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.plan.TreeNode
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.expressions._
import org.apache.flink.table.typeutils.TypeCoercion
import org.apache.flink.table.validate._

/**
  * LogicalNode is created and validated as we construct query plan using Table API.
  *
  * The main validation procedure is separated into two phases:
  *
  * Expressions' resolution and transformation ([[resolveExpressions]]):
  *
  * - translate [[UnresolvedFieldReference]] into [[ResolvedFieldReference]]
  *     using child operator's output
  * - translate [[Call]](UnresolvedFunction) into solid Expression
  * - generate alias names for query output
  * - ....
  *
  * LogicalNode validation ([[validate]]):
  *
  * - check no [[UnresolvedFieldReference]] exists any more
  * - check if all expressions have children of needed type
  * - check each logical operator have desired input
  *
  * Once we pass the validation phase, we can safely convert LogicalNode into Calcite's RelNode.
  */
abstract class LogicalNode extends TreeNode[LogicalNode] {
  def output: Seq[Attribute]

  def resolveExpressions(tableEnv: TableEnvironment): LogicalNode = {
    // resolve references and function calls
    val exprResolved = expressionPostOrderTransform {
      case u @ UnresolvedFieldReference(name) =>
        // try resolve a field
        resolveReference(tableEnv, name).getOrElse(u)
      case c @ Call(name, children) if c.childrenValid =>
        tableEnv.getFunctionCatalog.lookupFunction(name, children)
    }

    exprResolved.expressionPostOrderTransform {
      case ips: InputTypeSpec if ips.childrenValid =>
        var changed: Boolean = false
        val newChildren = ips.expectedTypes.zip(ips.children).map { case (tpe, child) =>
          val childType = child.resultType
          if (childType != tpe && TypeCoercion.canSafelyCast(childType, tpe)) {
            changed = true
            Cast(child, tpe)
          } else {
            child
          }
        }.toArray[AnyRef]
        if (changed) ips.makeCopy(newChildren) else ips
    }
  }

  final def toRelNode(relBuilder: RelBuilder): RelNode = construct(relBuilder).build()

  protected[logical] def construct(relBuilder: RelBuilder): RelBuilder

  def validate(tableEnv: TableEnvironment): LogicalNode = {
    val resolvedNode = resolveExpressions(tableEnv)
    resolvedNode.expressionPostOrderTransform {
      case a: Attribute if !a.valid =>
        val from = children.flatMap(_.output).map(_.name).mkString(", ")
        // give helpful error message for null literals
        if (a.name == "null") {
          failValidation(s"Cannot resolve field [${a.name}] given input [$from]. If you want to " +
            s"express a null literal, use 'Null(TYPE)' for typed null expressions. " +
            s"For example: Null(INT)")
        } else {
          failValidation(s"Cannot resolve field [${a.name}] given input [$from].")
        }

      case e: Expression if e.validateInput().isFailure =>
        failValidation(s"Expression $e failed on input check: " +
          s"${e.validateInput().asInstanceOf[ValidationFailure].message}")
    }
  }

  /**
    * Resolves the given strings to a [[NamedExpression]] using the input from all child
    * nodes of this LogicalPlan.
    */
  def resolveReference(tableEnv: TableEnvironment, name: String): Option[NamedExpression] = {
    // try to resolve a field
    val childrenOutput = children.flatMap(_.output)
    val fieldCandidates = childrenOutput.filter(_.name.equalsIgnoreCase(name))
    if (fieldCandidates.length > 1) {
      failValidation(s"Reference $name is ambiguous.")
    } else if (fieldCandidates.nonEmpty) {
      return Some(fieldCandidates.head.withName(name))
    }

    // try to resolve a table
    tableEnv.scanInternal(Array(name)) match {
      case Some(table) => Some(TableReference(name, table))
      case None => None
    }
  }

  /**
    * Runs [[postOrderTransform]] with `rule` on all expressions present in this logical node.
    *
    * @param rule the rule to be applied to every expression in this logical node.
    */
  def expressionPostOrderTransform(rule: PartialFunction[Expression, Expression]): LogicalNode = {
    var changed = false

    def expressionPostOrderTransform(e: Expression): Expression = {
      val newExpr = e.postOrderTransform(rule)
      if (newExpr.fastEquals(e)) {
        e
      } else {
        changed = true
        newExpr
      }
    }

    val newArgs = productIterator.map {
      case e: Expression => expressionPostOrderTransform(e)
      case Some(e: Expression) => Some(expressionPostOrderTransform(e))
      case seq: Traversable[_] => seq.map {
        case e: Expression => expressionPostOrderTransform(e)
        case other => other
      }
      case r: Resolvable[_] => r.resolveExpressions(e => expressionPostOrderTransform(e))
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
