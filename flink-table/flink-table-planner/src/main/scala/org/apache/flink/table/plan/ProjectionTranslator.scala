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

package org.apache.flink.table.plan

import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical.{LogicalNode, LogicalOverWindow, Project}
import org.apache.flink.table.typeutils.RowIntervalTypeInfo

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ProjectionTranslator {

  /**
    * Extracts and deduplicates all aggregation and window property expressions (zero, one, or more)
    * from the given expressions.
    *
    * @param exprs    a list of expressions to extract
    * @param tableEnv the TableEnvironment
    * @return a Tuple2, the first field contains the extracted and deduplicated aggregations,
    *         and the second field contains the extracted and deduplicated window properties.
    */
  def extractAggregationsAndProperties(
      exprs: Seq[PlannerExpression],
      tableEnv: TableEnvironment)
    : (Map[PlannerExpression, String], Map[PlannerExpression, String]) = {
    exprs.foldLeft((Map[PlannerExpression, String](), Map[PlannerExpression, String]())) {
      (x, y) => identifyAggregationsAndProperties(y, tableEnv, x._1, x._2)
    }
  }

  /** Identifies and deduplicates aggregation functions and window properties. */
  private def identifyAggregationsAndProperties(
      exp: PlannerExpression,
      tableEnv: TableEnvironment,
      aggNames: Map[PlannerExpression, String],
      propNames: Map[PlannerExpression, String])
    : (Map[PlannerExpression, String], Map[PlannerExpression, String]) = {

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

      case sfc @ PlannerScalarFunctionCall(clazz, args) =>
        args.foldLeft((aggNames, propNames)){
          (x, y) => identifyAggregationsAndProperties(y, tableEnv, x._1, x._2)
        }

      // General expression
      case e: PlannerExpression =>
        e.productIterator.foldLeft((aggNames, propNames)){
          (x, y) => y match {
            case e: PlannerExpression => identifyAggregationsAndProperties(e, tableEnv, x._1, x._2)
            case _ => (x._1, x._2)
          }
        }

      // Expression is null
      case null =>
        throw new ValidationException("Scala 'null' is not a valid expression. " +
          "Use 'Null(TYPE)' to specify typed null expressions. For example: Null(Types.INT)")
    }
  }

  /**
    * Replaces expressions with deduplicated aggregations and properties.
    *
    * @param exprs     a list of expressions to replace
    * @param tableEnv  the TableEnvironment
    * @param aggNames  the deduplicated aggregations
    * @param propNames the deduplicated properties
    * @return a list of replaced expressions
    */
  def replaceAggregationsAndProperties(
      exprs: Seq[PlannerExpression],
      tableEnv: TableEnvironment,
      aggNames: Map[PlannerExpression, String],
      propNames: Map[PlannerExpression, String]): Seq[NamedExpression] = {
    val projectedNames = new mutable.HashSet[String]
    exprs.map((exp: PlannerExpression) => replaceAggregationsAndProperties(exp, tableEnv,
      aggNames, propNames, projectedNames))
        .map(UnresolvedAlias)
  }

  private def replaceAggregationsAndProperties(
      exp: PlannerExpression,
      tableEnv: TableEnvironment,
      aggNames: Map[PlannerExpression, String],
      propNames: Map[PlannerExpression, String],
      projectedNames: mutable.HashSet[String])
    : PlannerExpression = {

    exp match {
      case agg: Aggregation =>
        val name = aggNames(agg)
        if (projectedNames.add(name)) {
          UnresolvedFieldReference(name)
        } else {
          Alias(UnresolvedFieldReference(name), tableEnv.createUniqueAttributeName())
        }
      case prop: WindowProperty =>
        val name = propNames(prop)
        if (projectedNames.add(name)) {
          UnresolvedFieldReference(name)
        } else {
          Alias(UnresolvedFieldReference(name), tableEnv.createUniqueAttributeName())
        }
      case n @ Alias(agg: Aggregation, name, _) =>
        val aName = aggNames(agg)
        Alias(UnresolvedFieldReference(aName), name)
      case n @ Alias(prop: WindowProperty, name, _) =>
        val pName = propNames(prop)
        Alias(UnresolvedFieldReference(pName), name)
      case l: LeafExpression => l
      case u: UnaryExpression =>
        val c = replaceAggregationsAndProperties(u.child, tableEnv,
          aggNames, propNames, projectedNames)
        u.makeCopy(Array(c))
      case b: BinaryExpression =>
        val l = replaceAggregationsAndProperties(b.left, tableEnv,
          aggNames, propNames, projectedNames)
        val r = replaceAggregationsAndProperties(b.right, tableEnv,
          aggNames, propNames, projectedNames)
        b.makeCopy(Array(l, r))

      case sfc @ PlannerScalarFunctionCall(clazz, args) =>
        val newArgs: Seq[PlannerExpression] = args
          .map((exp: PlannerExpression) =>
            replaceAggregationsAndProperties(exp, tableEnv, aggNames, propNames, projectedNames))
        sfc.makeCopy(Array(clazz, newArgs))

      // array constructor
      case c @ ArrayConstructor(args) =>
        val newArgs = c.elements
          .map((exp: PlannerExpression) =>
            replaceAggregationsAndProperties(exp, tableEnv, aggNames, propNames, projectedNames))
        c.makeCopy(Array(newArgs))

      // map constructor
      case c @ MapConstructor(args) =>
        val newArgs = c.elements
          .map((exp: PlannerExpression) =>
            replaceAggregationsAndProperties(exp, tableEnv, aggNames, propNames, projectedNames))
        c.makeCopy(Array(newArgs))

      // General expression
      case e: PlannerExpression =>
        val newArgs = e.productIterator.map {
          case arg: PlannerExpression =>
            replaceAggregationsAndProperties(arg, tableEnv, aggNames, propNames, projectedNames)
        }
        e.makeCopy(newArgs.toArray)
    }
  }

  /**
    * Expands an UnresolvedFieldReference("*") to parent's full project list.
    */
  def expandProjectList(
      exprs: Seq[PlannerExpression],
      parent: LogicalNode,
      tableEnv: TableEnvironment)
    : Seq[PlannerExpression] = {

    val projectList = new ListBuffer[PlannerExpression]

    exprs.foreach {
      case n: UnresolvedFieldReference if n.name == "*" =>
        projectList ++= parent.output.map(a => UnresolvedFieldReference(a.name))

      case Flattening(unresolved) =>
        // simulate a simple project to resolve fields using current parent
        val project = Project(Seq(UnresolvedAlias(unresolved)), parent).validate(tableEnv)
        val resolvedExpr = project
          .output
          .headOption
          .getOrElse(throw new RuntimeException("Could not find resolved composite."))
        resolvedExpr.validateInput()
        val newProjects = resolvedExpr.resultType match {
          case ct: CompositeType[_] =>
            (0 until ct.getArity).map { idx =>
              projectList += GetCompositeField(unresolved, ct.getFieldNames()(idx))
            }
          case _ =>
            projectList += unresolved
        }

      case e: PlannerExpression => projectList += e
    }
    projectList
  }

  def resolveOverWindows(
      exprs: Seq[PlannerExpression],
      overWindows: Seq[LogicalOverWindow],
      tEnv: TableEnvironment): Seq[PlannerExpression] = {

    exprs.map(e => replaceOverCall(e, overWindows, tEnv))
  }

  /**
    * Find and replace UnresolvedOverCall with OverCall
    *
    * @param expr    the expression to check
    * @return an expression with correct resolved OverCall
    */
  private def replaceOverCall(
      expr: PlannerExpression,
      overWindows: Seq[LogicalOverWindow],
      tableEnv: TableEnvironment)
    : PlannerExpression = {

    expr match {
      case u: UnresolvedOverCall =>
        overWindows.find(_.alias.equals(u.alias)) match {
          case Some(overWindow) =>
            OverCall(
              u.agg,
              overWindow.partitionBy,
              overWindow.orderBy,
              overWindow.preceding,
              overWindow.following.getOrElse {
                // set following to CURRENT_ROW / CURRENT_RANGE if not defined
                if (overWindow.preceding.resultType.isInstanceOf[RowIntervalTypeInfo]) {
                  CurrentRow()
                } else {
                  CurrentRange()
                }
              })

          case None => u
        }

      case u: UnaryExpression =>
        val c = replaceOverCall(u.child, overWindows, tableEnv)
        u.makeCopy(Array(c))

      case b: BinaryExpression =>
        val l = replaceOverCall(b.left, overWindows, tableEnv)
        val r = replaceOverCall(b.right, overWindows, tableEnv)
        b.makeCopy(Array(l, r))

      // Scala functions
      case sfc @ PlannerScalarFunctionCall(clazz, args: Seq[PlannerExpression]) =>
        val newArgs: Seq[PlannerExpression] =
          args.map(
            (exp: PlannerExpression) =>
              replaceOverCall(exp, overWindows, tableEnv))
        sfc.makeCopy(Array(clazz, newArgs))

      // Array constructor
      case c @ ArrayConstructor(args) =>
        val newArgs =
          c.elements
            .map((exp: PlannerExpression) => replaceOverCall(exp, overWindows, tableEnv))
        c.makeCopy(Array(newArgs))

      // Other expressions
      case e: PlannerExpression => e
    }
  }


  /**
    * Extract all field references from the given expressions.
    *
    * @param exprs a list of expressions to extract
    * @return a list of field references extracted from the given expressions
    */
  def extractFieldReferences(exprs: Seq[PlannerExpression]): Seq[NamedExpression] = {
    exprs.foldLeft(Set[NamedExpression]()) {
      (fieldReferences, expr) => identifyFieldReferences(expr, fieldReferences)
    }.toSeq
  }

  private def identifyFieldReferences(
      expr: PlannerExpression,
      fieldReferences: Set[NamedExpression]): Set[NamedExpression] = expr match {

    case f: UnresolvedFieldReference =>
      fieldReferences + UnresolvedAlias(f)

    case b: BinaryExpression =>
      val l = identifyFieldReferences(b.left, fieldReferences)
      identifyFieldReferences(b.right, l)

    case PlannerScalarFunctionCall(_, args: Seq[PlannerExpression]) =>
      args.foldLeft(fieldReferences) {
        (fieldReferences, expr) => identifyFieldReferences(expr, fieldReferences)
      }

    case AggFunctionCall(_, _, _, args) =>
      args.foldLeft(fieldReferences) {
        (fieldReferences, expr) => identifyFieldReferences(expr, fieldReferences)
      }

    // array constructor
    case ArrayConstructor(args) =>
      args.foldLeft(fieldReferences) {
        (fieldReferences, expr) => identifyFieldReferences(expr, fieldReferences)
      }

    // ignore fields from window property
    case _: WindowProperty =>
      fieldReferences

    // keep this case after all unwanted unary expressions
    case u: UnaryExpression =>
      identifyFieldReferences(u.child, fieldReferences)

    // General expression
    case e: PlannerExpression =>
      e.productIterator.foldLeft(fieldReferences) {
        (fieldReferences, expr) => expr match {
          case e: PlannerExpression => identifyFieldReferences(e, fieldReferences)
          case _ => fieldReferences
        }
      }
  }
}
