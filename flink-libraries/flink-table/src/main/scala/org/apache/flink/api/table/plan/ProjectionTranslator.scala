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
    * Extracts and deduplicates all aggregation and window property expressions (zero, one, or more)
    * from all expressions and replaces the original expressions by field accesses expressions.
    *
    * @param exprs a list of expressions to convert
    * @param tableEnv the TableEnvironment
    * @return a Tuple3, the first field contains the converted expressions, the second field the
    *         extracted and deduplicated aggregations, and the third field the extracted and
    *         deduplicated window properties.
    */
  def extractAggregationsAndProperties(
      exprs: Seq[Expression],
      tableEnv: TableEnvironment)
  : (Seq[NamedExpression], Seq[NamedExpression], Seq[NamedExpression]) = {

    val (aggNames, propNames) =
      exprs.foldLeft( (Map[Expression, String](), Map[Expression, String]()) ) {
        (x, y) => identifyAggregationsAndProperties(y, tableEnv, x._1, x._2)
      }

    val replaced = exprs
      .map(replaceAggregationsAndProperties(_, tableEnv, aggNames, propNames))
      .map {
        case e: Expression => UnresolvedAlias(e)
      }
    val aggs = aggNames.map( a => Alias(a._1, a._2)).toSeq
    val props = propNames.map( p => Alias(p._1, p._2)).toSeq

    (replaced, aggs, props)
  }

  /** Identifies and deduplicates aggregation functions and window properties. */
  private def identifyAggregationsAndProperties(
      exp: Expression,
      tableEnv: TableEnvironment,
      aggNames: Map[Expression, String],
      propNames: Map[Expression, String]) : (Map[Expression, String], Map[Expression, String]) = {

    exp match {
      case agg: Aggregation =>
        if (aggNames contains agg) {
          (aggNames, propNames)
        } else {
          (aggNames + (agg -> tableEnv.createUniqueAttributeName()), propNames)
        }
      case prop: WindowProperty =>
        if (propNames contains prop) {
          (aggNames, propNames)
        } else {
          (aggNames, propNames + (prop -> tableEnv.createUniqueAttributeName()))
        }
      case l: LeafExpression =>
        (aggNames, propNames)
      case u: UnaryExpression =>
        identifyAggregationsAndProperties(u.child, tableEnv, aggNames, propNames)
      case b: BinaryExpression =>
        val l = identifyAggregationsAndProperties(b.left, tableEnv, aggNames, propNames)
        identifyAggregationsAndProperties(b.right, tableEnv, l._1, l._2)

      // Functions calls
      case c @ Call(name, args) =>
        args.foldLeft((aggNames, propNames)){
          (x, y) => identifyAggregationsAndProperties(y, tableEnv, x._1, x._2)
        }

      case sfc @ ScalarFunctionCall(clazz, args) =>
        args.foldLeft((aggNames, propNames)){
          (x, y) => identifyAggregationsAndProperties(y, tableEnv, x._1, x._2)
        }

      // General expression
      case e: Expression =>
        e.productIterator.foldLeft((aggNames, propNames)){
          (x, y) => y match {
            case e: Expression => identifyAggregationsAndProperties(e, tableEnv, x._1, x._2)
            case _ => (x._1, x._2)
          }
        }
    }
  }

  /** Replaces aggregations and projections by named field references. */
  private def replaceAggregationsAndProperties(
      exp: Expression,
      tableEnv: TableEnvironment,
      aggNames: Map[Expression, String],
      propNames: Map[Expression, String]) : Expression = {

    exp match {
      case agg: Aggregation =>
        val name = aggNames(agg)
        Alias(UnresolvedFieldReference(name), tableEnv.createUniqueAttributeName())
      case prop: WindowProperty =>
        val name = propNames(prop)
        Alias(UnresolvedFieldReference(name), tableEnv.createUniqueAttributeName())
      case n @ Alias(agg: Aggregation, name) =>
        val aName = aggNames(agg)
        Alias(UnresolvedFieldReference(aName), name)
      case n @ Alias(prop: WindowProperty, name) =>
        val pName = propNames(prop)
        Alias(UnresolvedFieldReference(pName), name)
      case l: LeafExpression => l
      case u: UnaryExpression =>
        val c = replaceAggregationsAndProperties(u.child, tableEnv, aggNames, propNames)
        u.makeCopy(Array(c))
      case b: BinaryExpression =>
        val l = replaceAggregationsAndProperties(b.left, tableEnv, aggNames, propNames)
        val r = replaceAggregationsAndProperties(b.right, tableEnv, aggNames, propNames)
        b.makeCopy(Array(l, r))

      // Functions calls
      case c @ Call(name, args) =>
        val newArgs = args.map(replaceAggregationsAndProperties(_, tableEnv, aggNames, propNames))
        c.makeCopy(Array(name, newArgs))

      case sfc @ ScalarFunctionCall(clazz, args) =>
        val newArgs: Seq[Expression] = args
          .map(replaceAggregationsAndProperties(_, tableEnv, aggNames, propNames))
        sfc.makeCopy(Array(clazz,newArgs))

      // General expression
      case e: Expression =>
        val newArgs = e.productIterator.map {
          case arg: Expression =>
            replaceAggregationsAndProperties(arg, tableEnv, aggNames, propNames)
        }
        e.makeCopy(newArgs.toArray)
    }
  }

  /**
    * Expands an UnresolvedFieldReference("*") to parent's full project list.
    */
  def expandProjectList(exprs: Seq[Expression], parent: LogicalNode): Seq[Expression] = {
    val projectList = new ListBuffer[Expression]
    exprs.foreach {
      case n: UnresolvedFieldReference if n.name == "*" =>
        projectList ++= parent.output.map(a => UnresolvedFieldReference(a.name))
      case e: Expression => projectList += e
    }
    projectList
  }
}
