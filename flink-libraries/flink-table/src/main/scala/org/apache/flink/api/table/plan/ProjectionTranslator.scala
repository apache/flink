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

package org.apache.flink.api.table.plan

import org.apache.flink.api.table.TableEnvironment
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.plan.logical.LogicalNode

import scala.collection.mutable.ListBuffer

object ProjectionTranslator {

  /**
    * Extracts all aggregation and property expressions (zero, one, or more) from an expression,
    * and replaces the original expressions by field accesses expressions.
    */
  def extractAggregationsAndProperties(
      exp: Expression,
      tableEnv: TableEnvironment)
    : (Expression, List[NamedExpression], List[NamedExpression]) = {

    exp match {
      case agg: Aggregation =>
        val name = tableEnv.createUniqueAttributeName()
        val aggCall = Alias(agg, name)
        val fieldExp = UnresolvedFieldReference(name)
        (fieldExp, List(aggCall), Nil)
      case prop: WindowProperty =>
        val name = tableEnv.createUniqueAttributeName()
        val propCall = Alias(prop, name)
        val fieldExp = UnresolvedFieldReference(name)
        (fieldExp, Nil, List(propCall))
      case n @ Alias(agg: Aggregation, name) =>
        val fieldExp = UnresolvedFieldReference(name)
        (fieldExp, List(n), Nil)
      case n @ Alias(prop: WindowProperty, name) =>
        val fieldExp = UnresolvedFieldReference(name)
        (fieldExp, Nil, List(n))
      case l: LeafExpression =>
        (l, Nil, Nil)
      case u: UnaryExpression =>
        val c = extractAggregationsAndProperties(u.child, tableEnv)
        (u.makeCopy(Array(c._1)), c._2, c._3)
      case b: BinaryExpression =>
        val l = extractAggregationsAndProperties(b.left, tableEnv)
        val r = extractAggregationsAndProperties(b.right, tableEnv)
        (b.makeCopy(Array(l._1, r._1)),
          l._2 ::: r._2,
          l._3 ::: r._3)

      // Functions calls
      case c @ Call(name, args) =>
        val newArgs = args.map(extractAggregationsAndProperties(_, tableEnv))
        (c.makeCopy((name :: newArgs.map(_._1) :: Nil).toArray),
          newArgs.flatMap(_._2).toList,
          newArgs.flatMap(_._3).toList)

      case sfc @ ScalarFunctionCall(clazz, args) =>
        val newArgs = args.map(extractAggregationsAndProperties(_, tableEnv))
        (sfc.makeCopy((clazz :: newArgs.map(_._1) :: Nil).toArray),
          newArgs.flatMap(_._2).toList,
          newArgs.flatMap(_._3).toList)

      // General expression
      case e: Expression =>
        val newArgs = e.productIterator.map {
          case arg: Expression =>
            extractAggregationsAndProperties(arg, tableEnv)
        }
        (e.makeCopy(newArgs.map(_._1).toArray),
          newArgs.flatMap(_._2).toList,
          newArgs.flatMap(_._3).toList)
    }
  }

  /**
    * Parses all input expressions to [[UnresolvedAlias]].
    * And expands star to parent's full project list.
    */
  def expandProjectList(exprs: Seq[Expression], parent: LogicalNode): Seq[NamedExpression] = {
    val projectList = new ListBuffer[NamedExpression]
    exprs.foreach {
      case n: UnresolvedFieldReference if n.name == "*" =>
        projectList ++= parent.output.map(UnresolvedAlias(_))
      case e: Expression => projectList += UnresolvedAlias(e)
    }
    projectList
  }
}
