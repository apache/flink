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
package org.apache.flink.table.expressions

import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api._
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory.isTimeIndicatorType
import org.apache.flink.table.calcite.{FlinkTypeFactory, RexAggBufferVariable, RexAggLocalVariable}
import org.apache.flink.table.functions.sql.StreamRecordTimestampSqlFunction
import org.apache.flink.table.calcite._
import org.apache.flink.table.plan.logical.LogicalExprVisitor
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

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

  override private[flink] def resultType: InternalType =
    throw UnresolvedException(s"Calling resultType on ${this.getClass}.")

  override private[flink] def validateInput(): ValidationResult =
    ValidationFailure(s"Unresolved reference $name.")

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class ResolvedFieldReference(
    name: String,
    resultType: InternalType) extends Attribute {

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

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class UnresolvedAggBufferReference(name: String, returnType: InternalType) extends Attribute {

  override def toString = s"'$name"

  override def resultType: InternalType = returnType

  override private[flink] def withName(newName: String): Attribute =
    UnresolvedAggBufferReference(newName, returnType)

  override private[flink] def validateInput(): ValidationResult =
    ValidationFailure(s"Unresolved reference $name.")

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
 * Normally we should use [[ResolvedFieldReference]] to represent an input field.
 * [[ResolvedFieldReference]] uses name to locate the field, in aggregate case, we want to use
 * field index.
 */
case class ResolvedAggInputReference(
    name: String,
    index: Int,
    resultType: InternalType) extends Attribute {

  override def toString = s"'$name"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    // using index to resolve field directly, name used in toString only
    val typeFactory = relBuilder.getRexBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    new RexInputRef(
      index,
      typeFactory.createTypeFromInternalType(resultType, isNullable = true))
  }

  override private[flink] def withName(newName: String): Attribute = {
    if (newName == name) this
    else ResolvedAggInputReference(newName, index, resultType)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
 * Special reference which represent a filed from aggregate buffer. We may store the aggregate
 * buffer as rows, or directly as class members. We should use an unique name to locate the field.
 *
 * See [[org.apache.flink.table.codegen.ExprCodeGenerator.visitLocalRef()]]
 */
case class ResolvedAggBufferReference(name: String, resultType: InternalType)
  extends Attribute {

  override def toString = s"'$name"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.getRexBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    RexAggBufferVariable(
      name,
      typeFactory.createTypeFromInternalType(resultType, isNullable = true),
      resultType)
  }

  override private[flink] def withName(newName: String): Attribute = {
    if (newName == name) this
    else ResolvedAggBufferReference(newName, resultType)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Special reference which represent a local filed, such as aggregate buffers or constants.
  * We are stored as class members, so the field can be referenced directly.
  * We should use an unique name to locate the field.
  *
  * See [[org.apache.flink.table.codegen.ExprCodeGenerator.visitLocalRef()]]
  */
case class ResolvedAggLocalReference(
    name: String,
    nullTerm: String,
    resultType: InternalType)
  extends Attribute {

  override def toString = s"'$name"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.getRexBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    RexAggLocalVariable(
      name,
      nullTerm,
      typeFactory.createTypeFromInternalType(resultType, isNullable = true),
      resultType)
  }

  override private[flink] def withName(newName: String): Attribute = {
    if (newName == name) this
    else ResolvedAggLocalReference(newName, nullTerm, resultType)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Special reference which represent a distinct key input filed,
  * [[ResolvedDistinctKeyReference]] uses name to locate the field.
  */
case class ResolvedDistinctKeyReference(
    name: String,
    resultType: InternalType)
  extends Attribute {

  override def toString = s"'$name"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.getRexBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    RexDistinctKeyVariable(
      name,
      typeFactory.createTypeFromInternalType(resultType, isNullable = true),
      resultType)
  }

  override private[flink] def withName(newName: String): Attribute = {
    if (newName == name) this
    else ResolvedDistinctKeyReference(newName, resultType)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Alias(child: Expression, name: String, extraNames: Seq[String] = Seq())
    extends UnaryExpression with NamedExpression {

  override def toString = s"$child as '$name"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.alias(child.toRexNode, name)
  }

  override private[flink] def resultType: InternalType = child.resultType

  override private[flink] def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: Expression = anyRefs.head.asInstanceOf[Expression]
    copy(child, name, extraNames).asInstanceOf[this.type]
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
    } else if (extraNames.nonEmpty) {
      ValidationFailure("Invalid call to Alias with multiple names.")
    } else {
      ValidationSuccess
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class UnresolvedAlias(child: Expression) extends UnaryExpression with NamedExpression {

  override private[flink] def name: String =
    throw UnresolvedException("Invalid call to name on UnresolvedAlias")

  override private[flink] def toAttribute: Attribute =
    throw UnresolvedException("Invalid call to toAttribute on UnresolvedAlias")

  override private[flink] def resultType: InternalType =
    throw UnresolvedException("Invalid call to resultType on UnresolvedAlias")

  override private[flink] lazy val valid = false

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class WindowReference(name: String, tpe: Option[InternalType] = None) extends Attribute {

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException("A window reference can not be used solely.")

  override private[flink] def resultType: InternalType =
    tpe.getOrElse(throw UnresolvedException("Could not resolve type of referenced window."))

  override private[flink] def withName(newName: String): Attribute = {
    if (newName == name) {
      this
    } else {
      throw new ValidationException("Cannot rename window reference.")
    }
  }

  override def toString: String = s"'$name"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class TableReference(name: String, table: Table) extends LeafExpression with NamedExpression {

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException(s"Table reference '$name' can not be used solely.")

  override private[flink] def resultType: InternalType =
    throw UnresolvedException(s"Table reference '$name' has no result type.")

  override private[flink] def toAttribute =
    throw new UnsupportedOperationException(s"A table reference '$name' can not be an attribute.")

  override def toString: String = s"$name"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

abstract class TimeAttribute(val expression: Expression)
  extends UnaryExpression
  with WindowProperty {

  override private[flink] def child: Expression = expression
}

case class RowtimeAttribute(expr: Expression) extends TimeAttribute(expr) {

  override private[flink] def validateInput(): ValidationResult = {
    child match {
      case WindowReference(_, Some(tpe)) if tpe == DataTypes.PROCTIME_INDICATOR =>
        ValidationFailure("A proctime window cannot provide a rowtime attribute.")
      case WindowReference(_, Some(tpe)) if tpe == DataTypes.ROWTIME_INDICATOR =>
        // rowtime window
        ValidationSuccess
      case WindowReference(_, Some(tpe)) if tpe == DataTypes.LONG || tpe == DataTypes.TIMESTAMP =>
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

  override def resultType: InternalType = {
    child match {
      case WindowReference(_, Some(tpe)) if tpe == DataTypes.ROWTIME_INDICATOR =>
        // rowtime window
        DataTypes.ROWTIME_INDICATOR
      case WindowReference(_, Some(tpe)) if tpe == DataTypes.LONG || tpe == DataTypes.TIMESTAMP =>
        // batch time window
        DataTypes.TIMESTAMP
      case _ =>
        throw new TableException("WindowReference of RowtimeAttribute has invalid type. " +
          "Please report this bug.")
    }
  }

  override def toNamedWindowProperty(name: String): NamedWindowProperty =
    NamedWindowProperty(name, this)

  override def toString: String = s"rowtime($child)"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class ProctimeAttribute(expr: Expression) extends TimeAttribute(expr) {

  override private[flink] def validateInput(): ValidationResult = {
    child match {
      case WindowReference(_, Some(tpe)) if isTimeIndicatorType(tpe) =>
        ValidationSuccess
      case WindowReference(_, _) =>
        ValidationFailure("Reference to a rowtime or proctime window required.")
      case any =>
        ValidationFailure(
          "The '.proctime' expression can only be used for table definitions and windows, " +
            s"while [$any] was found.")
    }
  }

  override def resultType: InternalType = DataTypes.PROCTIME_INDICATOR

  override def toNamedWindowProperty(name: String): NamedWindowProperty =
    NamedWindowProperty(name, this)

  override def toString: String = s"proctime($child)"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/** Expression to access the timestamp of a StreamRecord. */
case class StreamRecordTimestamp() extends LeafExpression {

  override private[flink] def resultType = DataTypes.LONG

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(StreamRecordTimestampSqlFunction)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}
