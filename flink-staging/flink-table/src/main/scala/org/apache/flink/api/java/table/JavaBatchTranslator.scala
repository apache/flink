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

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.aggregation.AggregationFunction
import org.apache.flink.api.java.operators.JoinOperator.EquiJoin
import org.apache.flink.api.java.operators.Keys.ExpressionKeys
import org.apache.flink.api.java.operators.{GroupReduceOperator, Keys, MapOperator, UnsortedGrouping}
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.table.expressions.analysis.ExtractEquiJoinFields
import org.apache.flink.api.table.plan._
import org.apache.flink.api.table.runtime.{ExpressionAggregateFunction, ExpressionFilterFunction, ExpressionJoinFunction, ExpressionSelectFunction}
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.typeinfo.{RenameOperator, RenamingProxyTypeInfo, RowTypeInfo}
import org.apache.flink.api.table.{ExpressionException, Row, Table}

/**
 * [[PlanTranslator]] for creating [[Table]]s from Java [[org.apache.flink.api.java.DataSet]]s and
 * translating them back to Java [[org.apache.flink.api.java.DataSet]]s.
 */
class JavaBatchTranslator extends PlanTranslator {

  type Representation[A] = JavaDataSet[A]

  override def createTable[A](
      repr: Representation[A],
      inputType: CompositeType[A],
      expressions: Array[Expression],
      resultFields: Seq[(String, TypeInformation[_])]): Table = {

    val rowDataSet = createSelect(expressions, repr, inputType)

    Table(Root(rowDataSet, resultFields))
  }

  override def translate[A](op: PlanNode)(implicit tpe: TypeInformation[A]): JavaDataSet[A] = {

    if (tpe.getTypeClass == classOf[Row]) {
      // shortcut for DataSet[Row]
      return translateInternal(op).asInstanceOf[JavaDataSet[A]]
    }

    val clazz = tpe.getTypeClass
    if (clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) {
      throw new ExpressionException("Cannot create DataSet of type " +
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
      throw new ExpressionException(s"Expression result type $resultType does not have the same " +
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
    val operator = new MapOperator(resultSet, outputType, function, opName)

    operator
  }

  private def translateInternal(op: PlanNode): JavaDataSet[Row] = {
    op match {
      case Root(dataSet: JavaDataSet[Row], resultFields) =>
        dataSet

      case Root(_, _) =>
        throw new ExpressionException("Invalid Root for JavaBatchTranslator: " + op + ". " +
          "Did you try converting a Table based on a DataSet to a DataStream or vice-versa?")

      case GroupBy(_, fields) =>
        throw new ExpressionException("Dangling GroupBy operation. Did you forget a " +
          "SELECT statement?")

      case As(input, newNames) =>
        val translatedInput = translateInternal(input)
        val inType = translatedInput.getType.asInstanceOf[CompositeType[Row]]
        val proxyType = new RenamingProxyTypeInfo[Row](inType, newNames.toArray)
        new RenameOperator(translatedInput, proxyType)

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
        val translatedInput = translateInternal(input)
        val inType = translatedInput.getType.asInstanceOf[CompositeType[Row]]

        val keyIndices = groupExpressions map {
          case fe: ResolvedFieldReference => inType.getFieldIndex(fe.name)
          case e => throw new ExpressionException(s"Expression $e is not a valid key expression.")
        }

        val keys = new Keys.ExpressionKeys(keyIndices.toArray, inType, false)

        val grouping = new UnsortedGrouping(translatedInput, keys)

        val aggFunctions: Seq[AggregationFunction[Any]] = aggregations map {
          case (fieldName, fun) =>
            fun.getFactory.createAggregationFunction[Any](
              inType.getTypeAt[Any](inType.getFieldIndex(fieldName)).getTypeClass)
        }

        val aggIndices = aggregations map {
          case (fieldName, _) =>
            inType.getFieldIndex(fieldName)
        }

        val result = new GroupReduceOperator(
          grouping,
          inType,
          new ExpressionAggregateFunction(aggIndices, aggFunctions),
          "Expression Aggregation: " + agg)

        result

      case agg@Aggregate(input, aggregations) =>
        val translatedInput = translateInternal(input)
        val inType = translatedInput.getType.asInstanceOf[CompositeType[Row]]

        val aggFunctions: Seq[AggregationFunction[Any]] = aggregations map {
          case (fieldName, fun) =>
            fun.getFactory.createAggregationFunction[Any](
              inType.getTypeAt[Any](inType.getFieldIndex(fieldName)).getTypeClass)
        }

        val aggIndices = aggregations map {
          case (fieldName, _) =>
            inType.getFieldIndex(fieldName)
        }

        val result = new GroupReduceOperator(
          translatedInput,
          inType,
          new ExpressionAggregateFunction(aggIndices, aggFunctions),
          "Expression Aggregation: " + agg)

        result


      case Filter(input, predicate) =>
        val translatedInput = translateInternal(input)
        val inType = translatedInput.getType.asInstanceOf[CompositeType[Row]]
        val filter = new ExpressionFilterFunction[Row](predicate, inType)
        translatedInput.filter(filter).name(predicate.toString)
    }
  }

  private def createSelect[I](
      fields: Seq[Expression],
      input: JavaDataSet[I],
      inputType: CompositeType[I]): JavaDataSet[Row] = {

    fields foreach {
      f =>
        if (f.exists(_.isInstanceOf[Aggregation])) {
          throw new ExpressionException("Found aggregate in " + fields.mkString(", ") + ".")
        }

    }

    val resultType = new RowTypeInfo(fields)

    val function = new ExpressionSelectFunction(inputType, resultType, fields)

    val opName = s"select(${fields.mkString(",")})"
    val operator = new MapOperator(input, resultType, function, opName)

    operator
  }

  private def createJoin[L, R](
      predicate: Expression,
      fields: Seq[Expression],
      leftInput: JavaDataSet[L],
      rightInput: JavaDataSet[R],
      leftType: CompositeType[L],
      rightType: CompositeType[R],
      joinHint: JoinHint): JavaDataSet[Row] = {

    val resultType = new RowTypeInfo(fields)

    val (reducedPredicate, leftFields, rightFields) =
      ExtractEquiJoinFields(leftType, rightType, predicate)

    if (leftFields.isEmpty || rightFields.isEmpty) {
      throw new ExpressionException("Could not derive equi-join predicates " +
        "for predicate " + predicate + ".")
    }

    val leftKey = new ExpressionKeys[L](leftFields, leftType)
    val rightKey = new ExpressionKeys[R](rightFields, rightType)

    val joiner = new ExpressionJoinFunction[L, R, Row](
      reducedPredicate,
      leftType,
      rightType,
      resultType,
      fields)

    new EquiJoin[L, R, Row](
      leftInput,
      rightInput,
      leftKey,
      rightKey,
      joiner,
      resultType,
      joinHint,
      predicate.toString)
  }
}
