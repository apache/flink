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

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.UnresolvedException
import org.apache.flink.api.table.validate.{ExprValidationResult, ValidationFailure}

trait NamedExpression extends Expression {
  def name: String
  def toAttribute: Attribute
}

abstract class Attribute extends LeafExpression with NamedExpression {
  override def toAttribute: Attribute = this

  def withName(newName: String): Attribute
}

case class UnresolvedFieldReference(name: String) extends Attribute {

  override def toString = "\"" + name

  override def withName(newName: String): Attribute = UnresolvedFieldReference(newName)

  override def resultType: TypeInformation[_] =
    throw new UnresolvedException(s"calling resultType on ${this.getClass}")

  override def validateInput(): ExprValidationResult =
    ValidationFailure(s"Unresolved reference $name")
}

case class ResolvedFieldReference(
    name: String,
    resultType: TypeInformation[_]) extends Attribute {

  override def toString = s"'$name"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.field(name)
  }

  override def withName(newName: String): Attribute = {
    if (newName == name) {
      this
    } else {
      ResolvedFieldReference(newName, resultType)
    }
  }
}

case class Alias(child: Expression, name: String)
    extends UnaryExpression with NamedExpression {

  override def toString = s"$child as '$name"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.alias(child.toRexNode, name)
  }

  override def resultType: TypeInformation[_] = child.resultType

  override def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: Expression = anyRefs.head.asInstanceOf[Expression]
    copy(child, name).asInstanceOf[this.type]
  }

  override def toAttribute: Attribute = {
    if (valid) {
      ResolvedFieldReference(name, child.resultType)
    } else {
      UnresolvedFieldReference(name)
    }
  }
}

case class UnresolvedAlias(child: Expression) extends UnaryExpression with NamedExpression {

  override def name: String =
    throw new UnresolvedException("Invalid call to name on UnresolvedAlias")

  override def toAttribute: Attribute =
    throw new UnresolvedException("Invalid call to toAttribute on UnresolvedAlias")

  override def resultType: TypeInformation[_] =
    throw new UnresolvedException("Invalid call to resultType on UnresolvedAlias")

  override lazy val valid = false
}
