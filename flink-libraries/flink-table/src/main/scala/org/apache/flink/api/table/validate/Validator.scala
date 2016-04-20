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
package org.apache.flink.api.table.validate

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.plan.logical._

/**
  * Entry point for validating logical plan constructed from Table API.
  * The main validation procedure is separated into two phases:
  * - Resolve and Transformation:
  *   translate [[UnresolvedFieldReference]] into [[ResolvedFieldReference]]
  *     using child operator's output
  *   translate [[Call]](UnresolvedFunction) into solid Expression
  *   generate alias names for query output
  *   ....
  * - One pass validation of the resolved logical plan
  *   check no [[UnresolvedFieldReference]] exists any more
  *   check if all expressions have children of needed type
  *   check each logical operator have desired input
  *
  * Once we pass the validation phase, we can safely convert
  * logical operator into Calcite's RelNode.
  *
  * Note: the main idea of validation is adapted from Spark's Analyzer.
  */
class Validator(functionCatalog: FunctionCatalog) extends RuleExecutor[LogicalNode] {

  val fixedPoint = FixedPoint(100)

  lazy val batches: Seq[Batch] = Seq(
    Batch("Resolution", fixedPoint,
      ResolveReferences ::
      ResolveFunctions ::
      ResolveAliases ::
      AliasNodeTransformation :: Nil : _*)
  )

  /**
    * Resolve [[UnresolvedFieldReference]] using children's output.
    */
  object ResolveReferences extends Rule[LogicalNode] {
    def apply(plan: LogicalNode): LogicalNode = plan transformUp {
      case p: LogicalNode if !p.childrenResolved => p
      case q: LogicalNode =>
        q transformExpressionsUp {
          case u @ UnresolvedFieldReference(name) =>
            // if we failed to find a match this round,
            // leave it unchanged and hope we can do resolution next round.
            q.resolveChildren(name).getOrElse(u)
        }
    }
  }

  /**
    * Look up [[Call]] (Unresolved Functions) in function catalog and
    * transform them into concrete expressions.
    */
  object ResolveFunctions extends Rule[LogicalNode] {
    def apply(plan: LogicalNode): LogicalNode = plan transformUp {
      case p: LogicalNode =>
        p transformExpressionsUp {
          case c @ Call(name, children) if c.childrenValid =>
            functionCatalog.lookupFunction(name, children)
        }
    }
  }

  /**
    * Replace AliasNode (generated from `as("a, b, c")`) into Project operator
    */
  object AliasNodeTransformation extends Rule[LogicalNode] {
    def apply(plan: LogicalNode): LogicalNode = plan transformUp {
      case l: LogicalNode if !l.childrenResolved => l
      case a @ AliasNode(aliases, child) =>
        if (aliases.length > child.output.length) {
          failValidation("Aliasing more fields than we actually have")
        } else if (!aliases.forall(_.isInstanceOf[UnresolvedFieldReference])) {
          failValidation("`as` only allow string arguments")
        } else {
          val names = aliases.map(_.asInstanceOf[UnresolvedFieldReference].name)
          val input = child.output
          Project(
            names.zip(input).map { case (name, attr) =>
              Alias(attr, name)} ++ input.drop(names.length), child)
        }
    }
  }

  /**
    * Replace unnamed alias into concrete ones.
    */
  object ResolveAliases extends Rule[LogicalNode] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.zipWithIndex.map {
        case (expr, i) =>
          expr transformUp {
            case u @ UnresolvedAlias(child, optionalAliasName) => child match {
              case ne: NamedExpression => ne
              case e if !e.valid => u
              case c @ Cast(ne: NamedExpression, _) => Alias(c, ne.name)
              case other => Alias(other, optionalAliasName.getOrElse(s"_c$i"))
            }
          }
      }.asInstanceOf[Seq[NamedExpression]]
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]): Boolean =
      exprs.exists(_.isInstanceOf[UnresolvedAlias])

    def apply(plan: LogicalNode): LogicalNode = plan transformUp {
      case Project(projectList, child) if child.resolved && hasUnresolvedAlias(projectList) =>
        Project(assignAliases(projectList), child)
    }
  }

  def resolve(logical: LogicalNode): LogicalNode = execute(logical)

  /**
    * This would throw ValidationException on failure
    */
  def validate(resolved: LogicalNode): Unit = {
    resolved.foreachUp {
      case p: LogicalNode =>
        p transformExpressionsUp {
          case a: Attribute if !a.valid =>
            val from = p.children.flatMap(_.output).map(_.name).mkString(", ")
            failValidation(s"cannot resolve [${a.name}] given input [$from]")

          case e: Expression if e.validateInput().isFailure =>
            e.validateInput() match {
              case ExprValidationResult.ValidationFailure(message) =>
                failValidation(s"Expression $e failed on input check: $message")
            }
        }

        p match {
          case f: Filter if f.condition.dataType != BasicTypeInfo.BOOLEAN_TYPE_INFO =>
            failValidation(
              s"filter expression ${f.condition} of ${f.condition.dataType} is not a boolean")

          case j @ Join(_, _, _, Some(condition)) =>
            if (condition.dataType != BasicTypeInfo.BOOLEAN_TYPE_INFO) {
              failValidation(
                s"filter expression ${condition} of ${condition.dataType} is not a boolean")
            }

          case Union(left, right) =>
            if (left.output.length != right.output.length) {
              failValidation(s"Union two table of different column sizes:" +
                s" ${left.output.size} and ${right.output.size}")
            }
            val sameSchema = left.output.zip(right.output).forall { case (l, r) =>
              l.dataType == r.dataType && l.name == r.name }
            if (!sameSchema) {
              failValidation(s"Union two table of different schema:" +
                s" [${left.output.map(a => (a.name, a.dataType)).mkString(", ")}] and" +
                s" [${right.output.map(a => (a.name, a.dataType)).mkString(", ")}]")
            }

          case Aggregate(groupingExprs, aggregateExprs, child) =>
            def validateAggregateExpression(expr: Expression): Unit = expr match {
              // check no nested aggregation exists.
              case aggExpr: Aggregation =>
                aggExpr.children.foreach { child =>
                  child.foreach {
                    case agg: Aggregation =>
                      failValidation(
                        "It's not allowed to use an aggregate function as " +
                          "input of another aggregate function")
                    case _ => // OK
                  }
                }
              case a: Attribute if !groupingExprs.exists(_.semanticEquals(a)) =>
                failValidation(
                  s"expression '$a' is invalid because it is neither" +
                    " present in group by nor an aggregate function")
              case e if groupingExprs.exists(_.semanticEquals(e)) => // OK
              case e => e.children.foreach(validateAggregateExpression)
            }

            def validateGroupingExpression(expr: Expression): Unit = {
              if (!expr.dataType.isKeyType) {
                failValidation(
                  s"expression $expr cannot be used as a grouping expression " +
                    "because it's not a valid key type")
              }
            }

            aggregateExprs.foreach(validateAggregateExpression)
            groupingExprs.foreach(validateGroupingExpression)

          case _ => // fall back to following checks
        }

        p match {
          case o if !o.resolved =>
            failValidation(s"unresolved operator ${o.simpleString}")

          case _ => // Validation successful
        }
    }
  }

  protected def failValidation(msg: String): Nothing = {
    throw new ValidationException(msg)
  }
}
