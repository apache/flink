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
package org.apache.flink.table.expressions

import org.apache.calcite.rel.RelFieldCollation.{Direction, NullDirection}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.plan.logical.LogicalExprVisitor
import org.apache.flink.table.runtime.aggregate.RelFieldCollations
import org.apache.flink.table.validate._

abstract class Ordering extends UnaryExpression {
  override private[flink] def validateInput(): ValidationResult = {
    if (!child.isInstanceOf[NamedExpression]) {
      ValidationFailure(s"Sort should only based on field reference")
    } else {
      ValidationSuccess
    }
  }
}

case class Asc(child: Expression) extends Ordering {
  override def toString: String = s"($child).asc"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    toRexNode(relBuilder, useDefaultNullCollation = true)
  }

  private[flink] def toRexNode(
    implicit relBuilder: RelBuilder, useDefaultNullCollation: Boolean): RexNode = {
    val node = child.toRexNode
    if (useDefaultNullCollation) {
      if (RelFieldCollations.defaultNullDirection(Direction.ASCENDING) == NullDirection.FIRST) {
        relBuilder.nullsFirst(node)
      } else {
        relBuilder.nullsLast(node)
      }
    } else {
      node
    }
  }

  override private[flink] def resultType: InternalType = child.resultType

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Desc(child: Expression) extends Ordering {
  override def toString: String = s"($child).desc"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    toRexNode(relBuilder, useDefaultNullCollation = true)
  }

  private[flink] def toRexNode(
    implicit relBuilder: RelBuilder, useDefaultNullCollation: Boolean): RexNode = {
    val node = relBuilder.desc(child.toRexNode)
    if (useDefaultNullCollation) {
      if (RelFieldCollations.defaultNullDirection(Direction.DESCENDING) == NullDirection.FIRST) {
        relBuilder.nullsFirst(node)
      } else {
        relBuilder.nullsLast(node)
      }
    } else {
      node
    }
  }

  override private[flink] def resultType: InternalType = child.resultType

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

abstract class NullsOrdering extends UnaryExpression {
  override private[flink] def validateInput(): ValidationResult = {
    if (child.isInstanceOf[NamedExpression] ||
      child.isInstanceOf[Asc] || child.isInstanceOf[Desc]) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Nulls first/last should only based on field reference, asc or desc")
    }
  }
}

case class NullsFirst(child: Expression) extends NullsOrdering {
  override def toString: String = s"($child).nulls_first"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    child match {
      case c: Asc =>
        relBuilder.nullsFirst(
          c.toRexNode(relBuilder, useDefaultNullCollation = false))
      case c: Desc =>
        relBuilder.nullsFirst(
          c.toRexNode(relBuilder, useDefaultNullCollation = false))
      case _ =>
        relBuilder.nullsFirst(child.toRexNode(relBuilder))
    }
  }

  override private[flink] def resultType: InternalType = child.resultType

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class NullsLast(child: Expression) extends Ordering {
  override def toString: String = s"($child).nulls_last"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    child match {
      case c: Asc =>
        relBuilder.nullsLast(
          c.toRexNode(relBuilder, useDefaultNullCollation = false))
      case c: Desc =>
        relBuilder.nullsLast(
          c.toRexNode(relBuilder, useDefaultNullCollation = false))
      case _ =>
        relBuilder.nullsLast(child.toRexNode(relBuilder))
    }
  }

  override private[flink] def resultType: InternalType = child.resultType

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}
