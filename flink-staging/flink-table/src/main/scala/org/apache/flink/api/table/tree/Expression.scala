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
package org.apache.flink.api.table.tree

import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.api.common.typeinfo.{NothingTypeInfo, TypeInformation}

import scala.language.postfixOps


abstract class Expression extends Product {
  def children: Seq[Expression]
  def name: String = Expression.freshName("expression")
  def typeInfo: TypeInformation[_]

  /**
   * Tests for equality by first testing for reference equality.
   */
  def fastEquals(other: Expression): Boolean = this.eq(other) || this == other

  def transformPre(rule: PartialFunction[Expression, Expression]): Expression = {
    val afterTransform = rule.applyOrElse(this, identity[Expression])

    if (afterTransform fastEquals this) {
      this.transformChildrenPre(rule)
    } else {
      afterTransform.transformChildrenPre(rule)
    }
  }

  def transformChildrenPre(rule: PartialFunction[Expression, Expression]): Expression = {
    var changed = false
    val newArgs = productIterator map {
      case child: Expression if children.contains(child) =>
        val newChild = child.transformPre(rule)
        if (newChild fastEquals child) {
          child
        } else {
          changed = true
          newChild
        }
      case other: AnyRef => other
      case null => null
    } toArray

    if (changed) makeCopy(newArgs) else this
  }

  def transformPost(rule: PartialFunction[Expression, Expression]): Expression = {
    val afterChildren = transformChildrenPost(rule)
    if (afterChildren fastEquals this) {
      rule.applyOrElse(this, identity[Expression])
    } else {
      rule.applyOrElse(afterChildren, identity[Expression])
    }
  }

  def transformChildrenPost(rule: PartialFunction[Expression, Expression]): Expression = {
    var changed = false
    val newArgs = productIterator map {
      case child: Expression if children.contains(child) =>
        val newChild = child.transformPost(rule)
        if (newChild fastEquals child) {
          child
        } else {
          changed = true
          newChild
        }
      case other: AnyRef => other
      case null => null
    } toArray
    // toArray forces evaluation, toSeq does not seem to work here

    if (changed) makeCopy(newArgs) else this
  }

  def exists(predicate: Expression => Boolean): Boolean = {
    var exists = false
    this.transformPre {
      case e: Expression => if (predicate(e)) {
        exists = true
      }
        e
    }
    exists
  }

  /**
   * Creates a new copy of this expression with new children. This is used during transformation
   * if children change. This must be overridden by Expressions that don't have the Constructor
   * arguments in the same order as the `children`.
   */
  def makeCopy(newArgs: Seq[AnyRef]): this.type = {
    val defaultCtor =
      this.getClass.getConstructors.find { _.getParameterTypes.size > 0}.head
    try {
      defaultCtor.newInstance(newArgs.toArray: _*).asInstanceOf[this.type]
    } catch {
      case iae: IllegalArgumentException =>
        println("IAE " + this)
        throw new RuntimeException("Should never happen.")
    }
  }
}

abstract class BinaryExpression() extends Expression {
  def left: Expression
  def right: Expression
  def children = Seq(left, right)
}

abstract class UnaryExpression() extends Expression {
  def child: Expression
  def children = Seq(child)
}

abstract class LeafExpression() extends Expression {
  val children = Nil
}

case class NopExpression() extends LeafExpression {
  val typeInfo = new NothingTypeInfo()
  override val name = Expression.freshName("nop")

}

object Expression {
  def freshName(prefix: String): String = {
    s"$prefix-${freshNameCounter.getAndIncrement}"
  }

  val freshNameCounter = new AtomicInteger
}
