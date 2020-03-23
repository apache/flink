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

package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.UnresolvedException
import org.apache.flink.table.planner.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

/**
  * Flattening of composite types. All flattenings are resolved into
  * `GetCompositeField` expressions.
  */
case class Flattening(child: PlannerExpression) extends UnaryExpression {

  override def toString = s"$child.flatten()"

  override private[flink] def resultType: TypeInformation[_] =
    throw new UnresolvedException(s"Invalcall to on ${this.getClass}.")

  override private[flink] def validateInput(): ValidationResult =
    ValidationFailure(s"Unresolved flattening of $child")
}

case class GetCompositeField(child: PlannerExpression, key: Any) extends UnaryExpression {

  private var fieldIndex: Option[Int] = None

  override def toString = s"$child.get($key)"

  override private[flink] def validateInput(): ValidationResult = {
    // check for composite type
    if (!child.resultType.isInstanceOf[CompositeType[_]]) {
      return ValidationFailure(s"Cannot access field of non-composite type '${child.resultType}'.")
    }
    val compositeType = child.resultType.asInstanceOf[CompositeType[_]]

    // check key
    key match {
      case name: String =>
        val index = compositeType.getFieldIndex(name)
        if (index < 0) {
          ValidationFailure(s"Field name '$name' could not be found.")
        } else {
          fieldIndex = Some(index)
          ValidationSuccess
        }
      case index: Int =>
        if (index >= compositeType.getArity) {
          ValidationFailure(s"Field index '$index' exceeds arity.")
        } else {
          fieldIndex = Some(index)
          ValidationSuccess
        }
      case _ =>
        ValidationFailure(s"Invalid key '$key'.")
    }
  }

  override private[flink] def resultType: TypeInformation[_] =
    child.resultType.asInstanceOf[CompositeType[_]].getTypeAt(fieldIndex.get)

  override private[flink] def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: PlannerExpression = anyRefs.head.asInstanceOf[PlannerExpression]
    copy(child, key).asInstanceOf[this.type]
  }

  /**
    * Gives a meaningful alias if possible (e.g. a$mypojo$field).
    */
  private[flink] def aliasName(): Option[String] = child match {
    case gcf: GetCompositeField =>
      val alias = gcf.aliasName()
      if (alias.isDefined) {
        Some(s"${alias.get}$$$key")
      } else {
        None
      }
    case c: PlannerResolvedFieldReference =>
      val keySuffix = if (key.isInstanceOf[Int]) s"_$key" else key
      Some(s"${c.name}$$$keySuffix")
    case _ => None
  }
}
