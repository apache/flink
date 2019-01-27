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

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.types.{ArrayType, DataType, DataTypes, InternalType, MapType}
import org.apache.flink.table.calcite.FlinkRelBuilder
import org.apache.flink.table.plan.logical.LogicalExprVisitor
import org.apache.flink.table.typeutils.TypeCheckUtils.{isArray, isMap}
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

import scala.collection.JavaConverters._

case class RowConstructor(elements: Seq[Expression]) extends Expression {

  override private[flink] def children: Seq[Expression] = elements

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val relDataType = relBuilder
      .asInstanceOf[FlinkRelBuilder]
      .getTypeFactory
      .createTypeFromInternalType(resultType, isNullable = false)
    val values = elements.map(_.toRexNode).toList.asJava
    relBuilder
      .getRexBuilder
      .makeCall(relDataType, SqlStdOperatorTable.ROW, values)
  }

  override def toString = s"row(${elements.mkString(", ")})"

  override private[flink] def resultType: InternalType = DataTypes.createRowType(
    elements.map(e => e.resultType).toArray[DataType],
    Array.range(0, elements.length).map(e => s"f$e"))

  override private[flink] def validateInput(): ValidationResult = {
    if (elements.isEmpty) {
      return ValidationFailure("Empty rows are not supported yet.")
    }
    ValidationSuccess
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class ArrayConstructor(elements: Seq[Expression]) extends Expression {
  override private[flink] def children: Seq[Expression] = elements

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val relDataType = relBuilder
      .asInstanceOf[FlinkRelBuilder]
      .getTypeFactory
      .createTypeFromInternalType(resultType, isNullable = false)
    val values = elements.map(_.toRexNode).toList.asJava
    relBuilder
      .getRexBuilder
      .makeCall(relDataType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, values)
  }

  override def toString = s"array(${elements.mkString(", ")})"

  override private[flink] def resultType: InternalType =
    DataTypes.createArrayType(elements.head.resultType)

  override private[flink] def validateInput(): ValidationResult = {
    if (elements.isEmpty) {
      return ValidationFailure("Empty arrays are not supported yet.")
    }
    val elementType = elements.head.resultType
    if (!elements.forall(_.resultType == elementType)) {
      ValidationFailure("Not all elements of the array have the same type.")
    } else {
      ValidationSuccess
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class MapConstructor(elements: Seq[Expression]) extends Expression {
  override private[flink] def children: Seq[Expression] = elements

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.asInstanceOf[FlinkRelBuilder].getTypeFactory
    val relDataType = typeFactory.createMapType(
      typeFactory.createTypeFromInternalType(elements.head.resultType, isNullable = true),
      typeFactory.createTypeFromInternalType(elements.last.resultType, isNullable = true)
    )
    val values = elements.map(_.toRexNode).toList.asJava
    relBuilder
      .getRexBuilder
      .makeCall(relDataType, SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, values)
  }

  override def toString = s"map(${elements
    .grouped(2)
    .map(x => s"[${x.mkString(": ")}]").mkString(", ")})"

  override private[flink] def resultType: InternalType = new MapType(
    elements.head.resultType,
    elements.last.resultType
  )

  override private[flink] def validateInput(): ValidationResult = {
    if (elements.isEmpty) {
      return ValidationFailure("Empty maps are not supported yet.")
    }
    if (elements.size % 2 != 0) {
      return ValidationFailure("Maps must have an even number of elements to form key-value pairs.")
    }
    if (!elements.grouped(2).forall(_.head.resultType == elements.head.resultType)) {
      return ValidationFailure("Not all key elements of the map literal have the same type.")
    }
    if (!elements.grouped(2).forall(_.last.resultType == elements.last.resultType)) {
      return ValidationFailure("Not all value elements of the map literal have the same type.")
    }
    ValidationSuccess
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class ArrayElement(array: Expression) extends Expression {
  override private[flink] def children: Seq[Expression] = Seq(array)

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder
      .getRexBuilder
      .makeCall(SqlStdOperatorTable.ELEMENT, array.toRexNode)
  }

  override def toString = s"($array).element()"

  override private[flink] def resultType =
    array.resultType.asInstanceOf[ArrayType].getElementInternalType

  override private[flink] def validateInput(): ValidationResult = {
    array.resultType match {
      case ati: InternalType if isArray(ati) => ValidationSuccess
      case other@_ => ValidationFailure(s"Array expected but was '$other'.")
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Cardinality(container: Expression) extends Expression {
  override private[flink] def children: Seq[Expression] = Seq(container)

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder
      .getRexBuilder
      .makeCall(SqlStdOperatorTable.CARDINALITY, container.toRexNode)
  }

  override def toString = s"($container).cardinality()"

  override private[flink] def resultType = DataTypes.INT

  override private[flink] def validateInput(): ValidationResult = {
    container.resultType match {
      case at: InternalType if isArray(at) => ValidationSuccess
      case mt: InternalType if isMap(mt) => ValidationSuccess
      case other@_ => ValidationFailure(s"Array or map expected but was '$other'.")
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class ItemAt(container: Expression, index: Expression) extends Expression {
  override private[flink] def children: Seq[Expression] = Seq(container, index)

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder
      .getRexBuilder
      .makeCall(SqlStdOperatorTable.ITEM, container.toRexNode, index.toRexNode)
  }

  override def toString = s"($container).at($index)"

  override private[flink] def resultType = container.resultType match {
    case mt: MapType => mt.getValueInternalType
    case at: ArrayType => at.getElementInternalType
  }

  override private[flink] def validateInput(): ValidationResult = {
    container.resultType match {

      case _: ArrayType =>
        if (index.resultType == DataTypes.INT) {
          // check for common user mistake
          index match {
            case Literal(value: Int, DataTypes.INT) if value < 1 =>
              ValidationFailure(
                s"Array element access needs an index starting at 1 but was $value.")
            case _ => ValidationSuccess
          }
        } else {
          ValidationFailure(
            s"Array element access needs an integer index but was '${index.resultType}'.")
        }

      case mt: MapType  =>
        if (index.resultType == mt.getKeyInternalType) {
          ValidationSuccess
        } else {
          ValidationFailure(
            s"Map entry access needs a valid key of type " +
              s"'${mt.getKeyInternalType}', found '${index.resultType}'.")
        }

      case other@_ => ValidationFailure(s"Array or map expected but was '$other'.")
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}
