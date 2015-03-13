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
package org.apache.flink.api.scala

import com.google.common.base.Preconditions
import org.apache.flink.api.expressions.{Row, ExpressionOperation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.streaming.api.scala.DataStream

import scala.language.implicitConversions

/**
 * == Language Integrated Queries (aka Expression Operations) ==
 *
 * Importing this package with:
 *
 * {{{
 *   import org.apache.flink.api.scala.expressions._
 * }}}
 *
 * imports implicit conversions for converting a [[DataSet]] or [[DataStream]] to an
 * [[ExpressionOperation]]. This can be used to perform SQL-like queries on data. Please have
 * a look at [[ExpressionOperation]] to see which operations are supported and
 * [[org.apache.flink.api.scala.expressions.ImplicitExpressionOperations]] to see how an
 * expression can be specified.
 *
 * Inside an expression operation you can use Scala Symbols to refer to field names. One would
 * refer to field `a` by writing `'a`. Sometimes it is necessary to manually confert a
 * Scala literal to an Expression Literal, in those cases use `Literal`, as in `Literal(3)`.
 *
 * Example:
 *
 * {{{
 *   import org.apache.flink.api.scala._
 *   import org.apache.flink.api.scala.expressions._
 *
 *   val env = ExecutionEnvironment.getExecutionEnvironment
 *   val input = env.fromElements(("Hello", 2), ("Hello", 5), ("Ciao", 3))
 *   val result = input.as('word, 'count).groupBy('word).select('word, 'count.avg)
 *   result.print()
 *
 *   env.execute()
 * }}}
 *
 * The result of an [[ExpressionOperation]] can be converted back to the underlying API
 * representation using `as`:
 *
 * {{{
 *   case class Word(word: String, count: Int)
 *
 *   val result = in.select(...).as('word, 'count)
 *   val set = result.as[Word]
 * }}}
 */
package object expressions extends ImplicitExpressionConversions {

  implicit def dataSet2DataSetConversions[T](set: DataSet[T]): DataSetConversions[T] = {
    new DataSetConversions[T](set, set.getType.asInstanceOf[CompositeType[T]])
  }

  implicit def expressionOperation2RowDataSet(
      expressionOperation: ExpressionOperation[ScalaBatchTranslator]): DataSet[Row] = {
    expressionOperation.as[Row]
  }

  implicit def rowDataSet2ExpressionOperation(
      rowDataSet: DataSet[Row]): ExpressionOperation[ScalaBatchTranslator] = {
    rowDataSet.toExpression
  }

  implicit def dataStream2DataSetConversions[T](
      stream: DataStream[T]): DataStreamConversions[T] = {
    new DataStreamConversions[T](
      stream,
      stream.getJavaStream.getType.asInstanceOf[CompositeType[T]])
  }

  implicit def expressionOperation2RowDataStream(
      expressionOperation: ExpressionOperation[ScalaStreamingTranslator]): DataStream[Row] = {
    expressionOperation.as[Row]
  }

  implicit def rowDataStream2ExpressionOperation(
      rowDataStream: DataStream[Row]): ExpressionOperation[ScalaStreamingTranslator] = {
    rowDataStream.toExpression
  }
}
