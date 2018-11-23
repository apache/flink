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
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical.{LogicalNode, LogicalOverWindow, Project}
import org.apache.flink.table.typeutils.RowIntervalTypeInfo

object ProjectionTranslator {

  /**
    * Expands an UnresolvedFieldReference("*") to parent's full project list.
    */
  def expandProjectList(
      exprs: Seq[PlannerExpression],
      parent: LogicalNode,
      tableEnv: TableEnvironment)
    : Seq[PlannerExpression] = {
    exprs.flatMap(expr => flattenExpression(expr, parent, tableEnv))
  }

  def flattenExpression(
      expr: PlannerExpression,
      parent: LogicalNode,
      tableEnv: TableEnvironment)
    : Seq[PlannerExpression] = {
    expr match {
      case n: UnresolvedFieldReference if n.name == "*" =>
        parent.output.map(a => UnresolvedFieldReference(a.name))

      case Flattening(unresolved) =>
        // simulate a simple project to resolve fields using current parent
        val project = Project(Seq(UnresolvedAlias(unresolved)), parent).validate(tableEnv)
        val resolvedExpr = project
          .output
          .headOption
          .getOrElse(throw new RuntimeException("Could not find resolved composite."))
        resolvedExpr.validateInput()
        resolvedExpr.resultType match {
          case ct: CompositeType[_] =>
            (0 until ct.getArity).map { idx =>
              GetCompositeField(unresolved, ct.getFieldNames()(idx))
            }
          case _ =>
            Seq(unresolved)
        }

      case e: Expression => Seq(e)
    }
  }

  /**
    * Find and replace UnresolvedOverCall with OverCall
    *
    * @param exprs    the expressions to check
    * @param overWindows windows to use to resolve expressions
    * @return an expression with correct resolved OverCall
    */
  def resolveOverWindows(
      exprs: Seq[PlannerExpression],
      overWindows: Seq[LogicalOverWindow])
    : Seq[PlannerExpression] = {

    exprs.map(e => replaceOverCall(e, overWindows))
  }

  private def replaceOverCall(
      expr: PlannerExpression,
      overWindows: Seq[LogicalOverWindow])
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
        val c = replaceOverCall(u.child, overWindows)
        u.makeCopy(Array(c))

      case b: BinaryExpression =>
        val l = replaceOverCall(b.left, overWindows)
        val r = replaceOverCall(b.right, overWindows)
        b.makeCopy(Array(l, r))

      // Scala functions
      case sfc @ PlannerScalarFunctionCall(clazz, args: Seq[PlannerExpression]) =>
        val newArgs: Seq[PlannerExpression] =
          args.map(
            (exp: PlannerExpression) =>
              replaceOverCall(exp, overWindows))
        sfc.makeCopy(Array(clazz, newArgs))

      // Array constructor
      case c @ ArrayConstructor(args) =>
        val newArgs =
          c.elements
            .map((exp: PlannerExpression) => replaceOverCall(exp, overWindows))
        c.makeCopy(Array(newArgs))

      // Other expressions
      case e: PlannerExpression => e
    }
  }


  /**
    * Extracts the leading non-alias expression.
    *
    * @param expr the expression to extract
    * @return the top non-alias expression
    */
  def getLeadingNonAliasExpr(expr: Expression): Expression = {
    expr match {
      case callExpr: CallExpression if callExpr.getFunctionDefinition.equals(
          BuiltInFunctionDefinitions.AS) =>
        getLeadingNonAliasExpr(callExpr.getChildren.get(0))
      case _ => expr
    }
  }
}
