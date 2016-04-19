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
package org.apache.flink.api.table.trees

import org.apache.commons.lang.ClassUtils

/**
 * Generic base class for trees that can be transformed and traversed.
 */
abstract class TreeNode[A <: TreeNode[A]] extends Product { self: A =>

  /**
   * List of child nodes that should be considered when doing transformations. Other values
   * in the Product will not be transformed, only handed through.
   */
  def children: Seq[A]

  /**
   * Tests for equality by first testing for reference equality.
   */
  def fastEquals(other: TreeNode[_]): Boolean = this.eq(other) || this == other

  def transformDown(rule: PartialFunction[A, A]): A = {
    val afterTransform = rule.applyOrElse(this, identity[A])

    if (afterTransform fastEquals this) {
      transformChildren(rule, (t, r) => t.transformDown(r))
    } else {
      afterTransform.transformChildren(rule, (t, r) => t.transformDown(r))
    }
  }

  def transformUp(rule: PartialFunction[A, A]): A = {
    val afterChildren = transformChildren(rule, (t, r) => t.transformUp(r))
    if (afterChildren fastEquals this) {
      rule.applyOrElse(this, identity[A])
    } else {
      rule.applyOrElse(afterChildren, identity[A])
    }
  }

  /**
    * Returns a copy of this node where `rule` has been recursively applied to all the children of
    * this node.  When `rule` does not apply to a given node it is left unchanged.
    * @param rule the function used to transform this nodes children
    */
  protected def transformChildren(
      rule: PartialFunction[A, A],
      nextOperation: (A, PartialFunction[A, A]) => A): A = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = nextOperation(arg.asInstanceOf[A], rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case args: Traversable[_] => args.map {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = nextOperation(arg.asInstanceOf[A], rule)
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

  def exists(predicate: A => Boolean): Boolean = {
    var exists = false
    this.transformDown {
      case e: TreeNode[_] => if (predicate(e.asInstanceOf[A])) {
        exists = true
      }
      e.asInstanceOf[A]
    }
    exists
  }

  /**
    * Runs the given function recursively on [[children]] then on this node.
    * @param f the function to be applied to each node in the tree.
    */
  def foreachUp(f: A => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  /**
   * Creates a new copy of this expression with new children. This is used during transformation
   * if children change. This must be overridden by tree nodes that don't have the Constructor
   * arguments in the same order as the `children`.
   */
  def makeCopy(newArgs: Array[AnyRef]): A = {
    val ctors = getClass.getConstructors.filter(_.getParameterCount != 0)
    if (ctors.isEmpty) {
      sys.error(s"No valid constructor for ${getClass.getSimpleName}")
    }

    val defaultCtor = ctors.find { ctor =>
      if (ctor.getParameterCount != newArgs.length) {
        false
      } else if (newArgs.contains(null)) {
        // if there is a `null`, we can't figure out the class, therefore we should just fallback
        // to older heuristic
        false
      } else {
        val argsArray: Array[Class[_]] = newArgs.map(_.getClass)
        ClassUtils.isAssignable(argsArray, ctor.getParameterTypes)
      }
    }.getOrElse(ctors.maxBy(_.getParameterCount))

    try {
      defaultCtor.newInstance(newArgs.toArray: _*).asInstanceOf[A]
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"""
             |Failed to copy node.
             |Exception message: ${e.getMessage}
             |ctor: $defaultCtor
             |types: ${newArgs.map(_.getClass).mkString(", ")}
             |args: ${newArgs.mkString(", ")}
           """.stripMargin)
    }
  }

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  /** Returns a string representing the arguments to this node, minus any children */
  def argString: String = productIterator.flatMap {
    case tn: TreeNode[_] if containsChild(tn) => Nil
    case tn: TreeNode[_] => s"${tn.simpleString}" :: Nil
    case seq: Seq[A] if seq.toSet.subsetOf(children.toSet) => Nil
    case seq: Seq[_] => seq.mkString("[", ",", "]") :: Nil
    case set: Set[_] => set.mkString("{", ",", "}") :: Nil
    case other => other :: Nil
  }.mkString(", ")

  /** Returns the name of this type of TreeNode.  Defaults to the class name. */
  def nodeName: String = getClass.getSimpleName

  /** String representation of this node without any children */
  def simpleString: String = s"$nodeName $argString".trim

  override def toString: String = treeString

  /** Returns a string representation of the nodes in this tree */
  def treeString: String = generateTreeString(0, Nil, new StringBuilder).toString

  /**
    * Appends the string represent of this node and its children to the given StringBuilder.
    *
    * The `i`-th element in `lastChildren` indicates whether the ancestor of the current node at
    * depth `i + 1` is the last child of its own parent node.  The depth of the root node is 0, and
    * `lastChildren` for the root node should be empty.
    */
  protected def generateTreeString(
    depth: Int, lastChildren: Seq[Boolean], builder: StringBuilder): StringBuilder = {
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        val prefixFragment = if (isLast) "   " else ":  "
        builder.append(prefixFragment)
      }

      val branch = if (lastChildren.last) "+- " else ":- "
      builder.append(branch)
    }

    builder.append(simpleString)
    builder.append("\n")

    if (children.nonEmpty) {
      children.init.foreach(_.generateTreeString(depth + 1, lastChildren :+ false, builder))
      children.last.generateTreeString(depth + 1, lastChildren :+ true, builder)
    }

    builder
  }
}
