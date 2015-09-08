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

package org.apache.flink.api.java.table

import java.lang.reflect.Modifier
import org.apache.flink.api.common.operators.base.AbstractJoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.plan._
import org.apache.flink.api.table.runtime.{ExpressionFilterFunction, ExpressionSelectFunction}
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.api.table.{ExpressionException, Row, Table}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.operators.StreamMap

/**
 * [[PlanTranslator]] for creating [[Table]]s from Java [[DataStream]]s and
 * translating them back to Java [[DataStream]]s.
 *
 * This is very limited right now. Only select and filter are implemented. Also, the expression
 * operations must be extended to allow windowing operations.
 */

class JavaStreamingTranslator extends PlanTranslator {

  type Representation[A] = DataStream[A]

  override def createTable[A](
      repr: Representation[A],
      inputType: CompositeType[A],
      expressions: Array[Expression],
      resultFields: Seq[(String, TypeInformation[_])]): Table = {

    val rowDataStream = createSelect(expressions, repr, inputType)

    new Table(Root(rowDataStream, resultFields))
  }

  override def translate[A](op: PlanNode)(implicit tpe: TypeInformation[A]): DataStream[A] = {

    if (tpe.getTypeClass == classOf[Row]) {
      // shortcut for DataSet[Row]
      return translateInternal(op).asInstanceOf[DataStream[A]]
    }

    val clazz = tpe.getTypeClass
    if (clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) {
      throw new ExpressionException("Cannot create DataStream of type " +
        clazz.getName + ". Only top-level classes or static member classes are supported.")
    }

    if (!implicitly[TypeInformation[A]].isInstanceOf[CompositeType[A]]) {
      throw new ExpressionException(
        "A Table can only be converted to composite types, type is: " +
          implicitly[TypeInformation[A]] +
          ". Composite types would be tuples, case classes and POJOs.")

    }

    val resultSet = translateInternal(op)

    val resultType = resultSet.getType.asInstanceOf[RowTypeInfo]

    val outputType = implicitly[TypeInformation[A]].asInstanceOf[CompositeType[A]]

    val resultNames = resultType.getFieldNames
    val outputNames = outputType.getFieldNames.toSeq

    if (resultNames.toSet != outputNames.toSet) {
      throw new ExpressionException(s"Expression result type $resultType does not have the same" +
        s"fields as output type $outputType")
    }

    for (f <- outputNames) {
      val in = resultType.getTypeAt(resultType.getFieldIndex(f))
      val out = outputType.getTypeAt(outputType.getFieldIndex(f))
      if (!in.equals(out)) {
        throw new ExpressionException(s"Types for field $f differ on input $resultType and " +
          s"output $outputType.")
      }
    }

    val outputFields = outputNames map {
      f => ResolvedFieldReference(f, resultType.getTypeAt(f))
    }

    val function = new ExpressionSelectFunction(
      resultSet.getType.asInstanceOf[RowTypeInfo],
      outputType,
      outputFields)

    val opName = s"select(${outputFields.mkString(",")})"

    resultSet.transform(opName, outputType, new StreamMap[Row, A](function))
  }

  private def translateInternal(op: PlanNode): DataStream[Row] = {
    op match {
      case Root(dataSet: DataStream[Row], resultFields) =>
        dataSet

      case Root(_, _) =>
        throw new ExpressionException("Invalid Root for JavaStreamingTranslator: " + op + ". " +
          "Did you try converting a Table based on a DataSet to a DataStream or vice-versa?")

      case GroupBy(_, fields) =>
        throw new ExpressionException("Dangling GroupBy operation. Did you forget a " +
          "SELECT statement?")

      case As(input, newNames) =>
        throw new ExpressionException("As operation for Streams not yet implemented.")

      case sel@Select(Filter(Join(leftInput, rightInput), predicate), selection) =>

        val expandedInput = ExpandAggregations(sel)

        if (expandedInput.eq(sel)) {
          val translatedLeftInput = translateInternal(leftInput)
          val translatedRightInput = translateInternal(rightInput)
          val leftInType = translatedLeftInput.getType.asInstanceOf[CompositeType[Row]]
          val rightInType = translatedRightInput.getType.asInstanceOf[CompositeType[Row]]

          createJoin(
            predicate,
            selection,
            translatedLeftInput,
            translatedRightInput,
            leftInType,
            rightInType,
            JoinHint.OPTIMIZER_CHOOSES)
        } else {
          translateInternal(expandedInput)
        }

      case Filter(Join(leftInput, rightInput), predicate) =>
        val translatedLeftInput = translateInternal(leftInput)
        val translatedRightInput = translateInternal(rightInput)
        val leftInType = translatedLeftInput.getType.asInstanceOf[CompositeType[Row]]
        val rightInType = translatedRightInput.getType.asInstanceOf[CompositeType[Row]]

        createJoin(
          predicate,
          leftInput.outputFields.map( f => ResolvedFieldReference(f._1, f._2)) ++
            rightInput.outputFields.map( f => ResolvedFieldReference(f._1, f._2)),
          translatedLeftInput,
          translatedRightInput,
          leftInType,
          rightInType,
          JoinHint.OPTIMIZER_CHOOSES)

      case Join(leftInput, rightInput) =>
        throw new ExpressionException("Join without filter condition encountered. " +
          "Did you forget to add .where(...) ?")

      case sel@Select(input, selection) =>

        val expandedInput = ExpandAggregations(sel)

        if (expandedInput.eq(sel)) {
          // no expansions took place
          val translatedInput = translateInternal(input)
          val inType = translatedInput.getType.asInstanceOf[CompositeType[Row]]
          val inputFields = inType.getFieldNames
          createSelect(
            selection,
            translatedInput,
            inType)
        } else {
          translateInternal(expandedInput)
        }

      case agg@Aggregate(GroupBy(input, groupExpressions), aggregations) =>
        throw new ExpressionException("Aggregate operation for Streams not yet implemented.")

      case agg@Aggregate(input, aggregations) =>
        throw new ExpressionException("Aggregate operation for Streams not yet implemented.")

      case Filter(input, predicate) =>
        val translatedInput = translateInternal(input)
        val inType = translatedInput.getType.asInstanceOf[CompositeType[Row]]
        val filter = new ExpressionFilterFunction[Row](predicate, inType)
        translatedInput.filter(filter)
    }
  }

  private def createSelect[I](
      fields: Seq[Expression],
      input: DataStream[I],
      inputType: CompositeType[I]): DataStream[Row] = {

    fields foreach {
      f =>
        if (f.exists(_.isInstanceOf[Aggregation])) {
          throw new ExpressionException("Found aggregate in " + fields.mkString(", ") + ".")
        }

    }

    val resultType = new RowTypeInfo(fields)

    val function = new ExpressionSelectFunction(inputType, resultType, fields)

    val opName = s"select(${fields.mkString(",")})"

    input.transform(opName, resultType, new StreamMap[I, Row](function))
  }

  private def createJoin[L, R](
      predicate: Expression,
      fields: Seq[Expression],
      leftInput: DataStream[L],
      rightInput: DataStream[R],
      leftType: CompositeType[L],
      rightType: CompositeType[R],
      joinHint: JoinHint): DataStream[Row] = {

    throw new ExpressionException("Join operation for Streams not yet implemented.")
  }
}
