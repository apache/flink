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
package org.apache.flink.table.planner.plan

import org.apache.flink.table.planner.typeutils.TypeInfoCheckUtils

/**
 * Generic base class for trees that can be transformed and traversed.
 */
abstract class TreeNode[A <: TreeNode[A]] extends Product { self: A =>

  /**
   * List of child nodes that should be considered when doing transformations. Other values
   * in the Product will not be transformed, only handed through.
   */
  private[flink] def children: Seq[A]

  /**
   * Tests for equality by first testing for reference equality.
   */
  private[flink] def fastEquals(other: TreeNode[_]): Boolean = this.eq(other) || this == other

  /**
    * Do tree transformation in post order.
    */
  private[flink] def postOrderTransform(rule: PartialFunction[A, A]): A = {
    def childrenTransform(rule: PartialFunction[A, A]): A = {
      var changed = false
      val newArgs = productIterator.map {
        case arg: TreeNode[_] if children.contains(arg) =>
          val newChild = arg.asInstanceOf[A].postOrderTransform(rule)
          if (!(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case args: Traversable[_] => args.map {
          case arg: TreeNode[_] if children.contains(arg) =>
            val newChild = arg.asInstanceOf[A].postOrderTransform(rule)
            if (!(newChild fastEquals arg)) {
              changed = true
              newChild
            } else {
              arg
            }
          case other => other
        }
        case nonChild: AnyRef => nonChild
        case null => null
      }.toArray
      if (changed) makeCopy(newArgs) else this
    }

    val afterChildren = childrenTransform(rule)
    if (afterChildren fastEquals this) {
      rule.applyOrElse(this, identity[A])
    } else {
      rule.applyOrElse(afterChildren, identity[A])
    }
  }

  /**
    * Runs the given function first on the node and then recursively on all its children.
    */
  private[flink] def preOrderVisit(f: A => Unit): Unit = {
    f(this)
    children.foreach(_.preOrderVisit(f))
  }

  /**
   * Creates a new copy of this expression with new children. This is used during transformation
   * if children change.
   */
  private[flink] def makeCopy(newArgs: Array[AnyRef]): A = {
    val ctors = getClass.getConstructors.filter(_.getParameterTypes.length > 0)
    if (ctors.isEmpty) {
      throw new RuntimeException(s"No valid constructor for ${getClass.getSimpleName}")
    }

    val defaultCtor = ctors.find { ctor =>
      if (ctor.getParameterTypes.length != newArgs.length) {
        false
      } else if (newArgs.contains(null)) {
        false
      } else {
        val argsClasses: Array[Class[_]] = newArgs.map(_.getClass)
        TypeInfoCheckUtils.isAssignable(argsClasses, ctor.getParameterTypes)
      }
    }.getOrElse(ctors.maxBy(_.getParameterTypes.length))

    try {
      defaultCtor.newInstance(newArgs: _*).asInstanceOf[A]
    } catch {
      case e: Throwable =>
        throw new RuntimeException(
          s"Fail to copy tree node ${getClass.getName}.", e)
    }
  }
}
