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
import org.apache.flink.table.api._
import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory._
import org.apache.flink.table.planner.validate.{ValidationFailure, ValidationResult, ValidationSuccess}
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import org.apache.calcite.rex.RexNode

trait NamedExpression extends PlannerExpression {
  private[flink] def name: String
  private[flink] def toAttribute: Attribute
}

abstract class Attribute extends LeafExpression with NamedExpression {
  override private[flink] def toAttribute: Attribute = this

  private[flink] def withName(newName: String): Attribute
}

/**
  * Dummy wrapper for expressions that were converted to RexNode in a different way.
  */
case class RexPlannerExpression(
    private[flink] val rexNode: RexNode)
  extends LeafExpression {

  override private[flink] def resultType: TypeInformation[_] =
    fromLogicalTypeToTypeInfo(FlinkTypeFactory.toLogicalType(rexNode.getType))
}

case class UnresolvedFieldReference(name: String) extends Attribute {

  override def toString = s"'$name"

  override private[flink] def withName(newName: String): Attribute =
    UnresolvedFieldReference(newName)

  override private[flink] def resultType: TypeInformation[_] =
    throw new UnresolvedException(s"Calling resultType on ${this.getClass}.")

  override private[flink] def validateInput(): ValidationResult =
    ValidationFailure(s"Unresolved reference $name.")
}

case class PlannerResolvedFieldReference(
    name: String,
    resultType: TypeInformation[_]) extends Attribute {

  override def toString = s"'$name"

  override private[flink] def withName(newName: String): Attribute = {
    if (newName == name) {
      this
    } else {
      PlannerResolvedFieldReference(newName, resultType)
    }
  }
}

case class Alias(child: PlannerExpression, name: String, extraNames: Seq[String] = Seq())
    extends UnaryExpression with NamedExpression {

  override def toString = s"$child as '$name"

  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: PlannerExpression = anyRefs.head.asInstanceOf[PlannerExpression]
    copy(child, name, extraNames).asInstanceOf[this.type]
  }

  override private[flink] def toAttribute: Attribute = {
    if (valid) {
      PlannerResolvedFieldReference(name, child.resultType)
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

case class UnresolvedAlias(child: PlannerExpression) extends UnaryExpression with NamedExpression {

  override private[flink] def name: String =
    throw new UnresolvedException("Invalid call to name on UnresolvedAlias")

  override private[flink] def toAttribute: Attribute =
    throw new UnresolvedException("Invalid call to toAttribute on UnresolvedAlias")

  override private[flink] def resultType: TypeInformation[_] =
    throw new UnresolvedException("Invalid call to resultType on UnresolvedAlias")

  override private[flink] lazy val valid = false
}

case class WindowReference(name: String, tpe: Option[TypeInformation[_]] = None) extends Attribute {

  override private[flink] def resultType: TypeInformation[_] =
    tpe.getOrElse(throw new UnresolvedException("Could not resolve type of referenced window."))

  override private[flink] def withName(newName: String): Attribute = {
    if (newName == name) {
      this
    } else {
      throw new ValidationException("Cannot rename window reference.")
    }
  }

  override def toString: String = s"'$name"
}

case class TableReference(name: String, tableOperation: QueryOperation)
  extends LeafExpression
  with NamedExpression {

  override private[flink] def resultType: TypeInformation[_] =
    throw new UnresolvedException(s"Table reference '$name' has no result type.")

  override private[flink] def toAttribute =
    throw new UnsupportedOperationException(s"A table reference '$name' can not be an attribute.")

  override def toString: String = s"$name"
}

abstract class TimeAttribute(val expression: PlannerExpression)
  extends UnaryExpression
  with WindowProperty {

  override private[flink] def child: PlannerExpression = expression
}

case class RowtimeAttribute(expr: PlannerExpression) extends TimeAttribute(expr) {

  override private[flink] def validateInput(): ValidationResult = {
    child match {
      case WindowReference(_, Some(tpe: TypeInformation[_])) if isProctimeIndicatorType(tpe) =>
        ValidationFailure("A proctime window cannot provide a rowtime attribute.")
      case WindowReference(_, Some(tpe: TypeInformation[_])) if isRowtimeIndicatorType(tpe) =>
        // rowtime window
        ValidationSuccess
      case WindowReference(_, Some(tpe)) if tpe == Types.LONG || tpe == Types.SQL_TIMESTAMP =>
        // batch time window
        ValidationSuccess
      case WindowReference(_, _) =>
        ValidationFailure("Reference to a rowtime or proctime window required.")
      case any =>
        ValidationFailure(
          s"The '.rowtime' expression can only be used for table definitions and windows, " +
            s"while [$any] was found.")
    }
  }

  override def resultType: TypeInformation[_] = {
    child match {
      case WindowReference(_, Some(tpe: TypeInformation[_])) if isRowtimeIndicatorType(tpe) =>
        // rowtime window
        TimeIndicatorTypeInfo.ROWTIME_INDICATOR
      case WindowReference(_, Some(tpe)) if tpe == Types.LONG || tpe == Types.SQL_TIMESTAMP =>
        // batch time window
        Types.SQL_TIMESTAMP
      case _ =>
        throw new TableException("RowtimeAttribute has invalid type. Please report this bug.")
    }
  }

  override def toNamedWindowProperty(name: String): NamedWindowProperty =
    NamedWindowProperty(name, this)

  override def toString: String = s"rowtime($child)"
}

case class ProctimeAttribute(expr: PlannerExpression) extends TimeAttribute(expr) {

  override private[flink] def validateInput(): ValidationResult = {
    child match {
      case WindowReference(_, Some(tpe: TypeInformation[_])) if isTimeIndicatorType(tpe) =>
        ValidationSuccess
      case WindowReference(_, _) =>
        ValidationFailure("Reference to a rowtime or proctime window required.")
      case any =>
        ValidationFailure(
          "The '.proctime' expression can only be used for table definitions and windows, " +
            s"while [$any] was found.")
    }
  }

  override def resultType: TypeInformation[_] =
    TimeIndicatorTypeInfo.PROCTIME_INDICATOR

  override def toNamedWindowProperty(name: String): NamedWindowProperty =
    NamedWindowProperty(name, this)

  override def toString: String = s"proctime($child)"
}

/** Expression to access the timestamp of a StreamRecord. */
case class StreamRecordTimestamp() extends LeafExpression {

  override private[flink] def resultType = Types.LONG
}
