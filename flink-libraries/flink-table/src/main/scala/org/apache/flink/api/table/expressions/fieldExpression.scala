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
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.table.{UnresolvedException, ValidationException}
import org.apache.flink.api.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

trait NamedExpression extends Expression {
  private[flink] def name: String
  private[flink] def toAttribute: Attribute
}

abstract class Attribute extends LeafExpression with NamedExpression {
  override private[flink] def toAttribute: Attribute = this

  private[flink] def withName(newName: String): Attribute
}

case class UnresolvedFieldReference(name: String) extends Attribute {

  override def toString = s"'$name"

  override private[flink] def withName(newName: String): Attribute =
    UnresolvedFieldReference(newName)

  override private[flink] def resultType: TypeInformation[_] =
    throw UnresolvedException(s"Calling resultType on ${this.getClass}.")

  override private[flink] def validateInput(): ValidationResult =
    ValidationFailure(s"Unresolved reference $name.")
}

case class ResolvedFieldReference(
    name: String,
    resultType: TypeInformation[_]) extends Attribute {

  override def toString = s"'$name"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.field(name)
  }

  override private[flink] def withName(newName: String): Attribute = {
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

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.alias(child.toRexNode, name)
  }

  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: Expression = anyRefs.head.asInstanceOf[Expression]
    copy(child, name).asInstanceOf[this.type]
  }

  override private[flink] def toAttribute: Attribute = {
    if (valid) {
      ResolvedFieldReference(name, child.resultType)
    } else {
      UnresolvedFieldReference(name)
    }
  }

  override private[flink] def validateInput(): ValidationResult = {
    if (name == "*") {
      ValidationFailure("Alias can not accept '*' as name.")
    } else {
      ValidationSuccess
    }
  }
}

case class UnresolvedAlias(child: Expression) extends UnaryExpression with NamedExpression {

  override private[flink] def name: String =
    throw UnresolvedException("Invalid call to name on UnresolvedAlias")

  override private[flink] def toAttribute: Attribute =
    throw UnresolvedException("Invalid call to toAttribute on UnresolvedAlias")

  override private[flink] def resultType: TypeInformation[_] =
    throw UnresolvedException("Invalid call to resultType on UnresolvedAlias")

  override private[flink] lazy val valid = false
}

case class RowtimeAttribute() extends Attribute {
  override private[flink] def withName(newName: String): Attribute = {
    if (newName == "rowtime") {
      this
    } else {
      throw new ValidationException("Cannot rename streaming rowtime attribute.")
    }
  }

  override private[flink] def name: String = "rowtime"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    throw new UnsupportedOperationException("A rowtime attribute can not be used solely.")
  }

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.LONG_TYPE_INFO
}

case class WindowReference(name: String) extends Attribute {

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException("A window reference can not be used solely.")

  override private[flink] def resultType: TypeInformation[_] =
    throw new UnsupportedOperationException("A window reference has no result type.")

  override private[flink] def withName(newName: String): Attribute = {
    if (newName == name) {
      this
    } else {
      throw new ValidationException("Cannot rename window reference.")
    }
  }
}
