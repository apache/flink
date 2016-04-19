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
import org.apache.flink.api.table.plan.logical.{Filter, Join, LogicalNode, Project}

class Validator(functionCatalog: FunctionCatalog) extends RuleExecutor[LogicalNode] {

  val fixedPoint = FixedPoint(100)

  lazy val batches: Seq[Batch] = Seq(
    Batch("Resolution", fixedPoint,
      ResolveReferences ::
      ResolveFunctions ::
      ResolveAliases :: Nil : _*)
  )

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

  object ResolveFunctions extends Rule[LogicalNode] {
    def apply(plan: LogicalNode): LogicalNode = plan transformUp {
      case p: LogicalNode =>
        p transformExpressionsUp {
          case c @ Call(name, children) if c.childrenValid =>
            functionCatalog.lookupFunction(name, children)
        }
    }
  }

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

          case c: Cast if !c.valid =>
            failValidation(s"invalid cast from ${c.child.dataType} to ${c.dataType}")
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
          case _ =>
        }

        p match {
          case o if !o.resolved =>
            failValidation(s"unresolved operator ${o.simpleString}")

          case _ =>
        }
    }
  }

  protected def failValidation(msg: String): Nothing = {
    throw new ValidationException(msg)
  }

}
