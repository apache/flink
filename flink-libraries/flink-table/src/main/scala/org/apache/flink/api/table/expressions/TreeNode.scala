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
package org.apache.flink.api.table.expressions

/**
 * Generic base class for trees that can be transformed and traversed.
 */
abstract class TreeNode[A <: TreeNode[A]] { self: A with Product =>

  /**
   * List of child nodes that should be considered when doing transformations. Other values
   * in the Product will not be transformed, only handed through.
   */
  def children: Seq[A]

  /**
   * Tests for equality by first testing for reference equality.
   */
  def fastEquals(other: TreeNode[_]): Boolean = this.eq(other) || this == other

  def transformPre(rule: PartialFunction[A, A]): A = {
    val afterTransform = rule.applyOrElse(this, identity[A])

    if (afterTransform fastEquals this) {
      this.transformChildrenPre(rule)
    } else {
      afterTransform.transformChildrenPre(rule)
    }
  }

  def transformChildrenPre(rule: PartialFunction[A, A]): A = {
    var changed = false
    val newArgs = productIterator map {
      case child: A if children.contains(child) =>
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

  def transformPost(rule: PartialFunction[A, A]): A = {
    val afterChildren = transformChildrenPost(rule)
    if (afterChildren fastEquals this) {
      rule.applyOrElse(this, identity[A])
    } else {
      rule.applyOrElse(afterChildren, identity[A])
    }
  }

  def transformChildrenPost(rule: PartialFunction[A, A]): A = {
    var changed = false
    val newArgs = productIterator map {
      case child: A if children.contains(child) =>
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

  def exists(predicate: A => Boolean): Boolean = {
    var exists = false
    this.transformPre {
      case e: A => if (predicate(e)) {
        exists = true
      }
        e
    }
    exists
  }

  /**
   * Creates a new copy of this expression with new children. This is used during transformation
   * if children change. This must be overridden by tree nodes that don't have the Constructor
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

