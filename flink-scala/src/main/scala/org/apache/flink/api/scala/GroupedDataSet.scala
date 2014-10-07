/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.scala

import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.java.functions.FirstReducer
import org.apache.flink.api.scala.operators.ScalaAggregateOperator

import scala.collection.JavaConverters._

import org.apache.commons.lang3.Validate
import org.apache.flink.api.common.functions.{GroupReduceFunction, ReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.operators._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * A [[DataSet]] to which a grouping key was added. Operations work on groups of elements with the
 * same key (`aggregate`, `reduce`, and `reduceGroup`).
 *
 * A secondary sort order can be added with sortGroup, but this is only used when using one
 * of the group-at-a-time operations, i.e. `reduceGroup`.
 */
class GroupedDataSet[T: ClassTag](
    private val set: DataSet[T],
    private val keys: Keys[T]) {

  // These are for optional secondary sort. They are only used
  // when using a group-at-a-time reduce function.
  private val groupSortKeyPositions = mutable.MutableList[Either[Int, String]]()
  private val groupSortOrders = mutable.MutableList[Order]()

  /**
   * Adds a secondary sort key to this [[GroupedDataSet]]. This will only have an effect if you
   * use one of the group-at-a-time, i.e. `reduceGroup`.
   *
   * This only works on Tuple DataSets.
   */
  def sortGroup(field: Int, order: Order): GroupedDataSet[T] = {
    if (!set.getType.isTupleType) {
      throw new InvalidProgramException("Specifying order keys via field positions is only valid " +
        "for tuple data types.")
    }
    if (field >= set.getType.getArity) {
      throw new IllegalArgumentException("Order key out of tuple bounds.")
    }
    groupSortKeyPositions += Left(field)
    groupSortOrders += order
    this
  }

  /**
   * Adds a secondary sort key to this [[GroupedDataSet]]. This will only have an effect if you
   * use one of the group-at-a-time, i.e. `reduceGroup`.
   *
   * This only works on CaseClass DataSets.
   */
  def sortGroup(field: String, order: Order): GroupedDataSet[T] = {
    groupSortKeyPositions += Right(field)
    groupSortOrders += order
    this
  }

  /**
   * Creates a [[SortedGrouping]] if group sorting keys were specified.
   */
  private def maybeCreateSortedGrouping(): Grouping[T] = {
    if (groupSortKeyPositions.length > 0) {
      val grouping = groupSortKeyPositions(0) match {
        case Left(pos) =>
          new SortedGrouping[T](
            set.javaSet,
            keys,
            pos,
            groupSortOrders(0))

        case Right(field) =>
          new SortedGrouping[T](
            set.javaSet,
            keys,
            field,
            groupSortOrders(0))

      }
      // now manually add the rest of the keys
      for (i <- 1 until groupSortKeyPositions.length) {
        groupSortKeyPositions(i) match {
          case Left(pos) =>
            grouping.sortGroup(pos, groupSortOrders(i))

          case Right(field) =>
            grouping.sortGroup(field, groupSortOrders(i))

        }
      }
      grouping
    } else {
      new UnsortedGrouping[T](set.javaSet, keys)
    }
  }

  /** Convenience methods for creating the [[UnsortedGrouping]] */
  private def createUnsortedGrouping(): Grouping[T] = new UnsortedGrouping[T](set.javaSet, keys)

  /**
   * Creates a new [[DataSet]] by aggregating the specified tuple field using the given aggregation
   * function. Since this is a keyed DataSet the aggregation will be performed on groups of
   * tuples with the same key.
   *
   * This only works on Tuple DataSets.
   */
  def aggregate(agg: Aggregations, field: String): AggregateDataSet[T] = {
    val fieldIndex = fieldNames2Indices(set.getType, Array(field))(0)

    new AggregateDataSet(new ScalaAggregateOperator[T](createUnsortedGrouping(), agg, fieldIndex))
  }

  /**
   * Creates a new [[DataSet]] by aggregating the specified field using the given aggregation
   * function. Since this is a keyed DataSet the aggregation will be performed on groups of
   * elements with the same key.
   *
   * This only works on CaseClass DataSets.
   */
  def aggregate(agg: Aggregations, field: Int): AggregateDataSet[T] = {
    new AggregateDataSet(new ScalaAggregateOperator[T](createUnsortedGrouping(), agg, field))
  }

  /**
   * Syntactic sugar for [[aggregate]] with `SUM`
   */
  def sum(field: Int) = {
    aggregate(Aggregations.SUM, field)
  }

  /**
   * Syntactic sugar for [[aggregate]] with `MAX`
   */
  def max(field: Int) = {
    aggregate(Aggregations.MAX, field)
  }

  /**
   * Syntactic sugar for [[aggregate]] with `MIN`
   */
  def min(field: Int) = {
    aggregate(Aggregations.MIN, field)
  }

  /**
   * Syntactic sugar for [[aggregate]] with `SUM`
   */
  def sum(field: String) = {
    aggregate(Aggregations.SUM, field)
  }

  /**
   * Syntactic sugar for [[aggregate]] with `MAX`
   */
  def max(field: String) = {
    aggregate(Aggregations.MAX, field)
  }

  /**
   * Syntactic sugar for [[aggregate]] with `MIN`
   */
  def min(field: String) = {
    aggregate(Aggregations.MIN, field)
  }

  /**
   * Creates a new [[DataSet]] by merging the elements of each group (elements with the same key)
   * using an associative reduce function.
   */
  def reduce(fun: (T, T) => T): DataSet[T] = {
    Validate.notNull(fun, "Reduce function must not be null.")
    val reducer = new ReduceFunction[T] {
      def reduce(v1: T, v2: T) = {
        fun(v1, v2)
      }
    }
    wrap(new ReduceOperator[T](createUnsortedGrouping(), reducer))
  }

  /**
   * Creates a new [[DataSet]] by merging the elements of each group (elements with the same key)
   * using an associative reduce function.
   */
  def reduce(reducer: ReduceFunction[T]): DataSet[T] = {
    Validate.notNull(reducer, "Reduce function must not be null.")
    wrap(new ReduceOperator[T](createUnsortedGrouping(), reducer))
  }

  /**
   * Creates a new [[DataSet]] by passing for each group (elements with the same key) the list
   * of elements to the group reduce function. The function must output one element. The
   * concatenation of those will form the resulting [[DataSet]].
   */
  def reduceGroup[R: TypeInformation: ClassTag](
      fun: (TraversableOnce[T]) => R): DataSet[R] = {
    Validate.notNull(fun, "Group reduce function must not be null.")
    val reducer = new GroupReduceFunction[T, R] {
      def reduce(in: java.lang.Iterable[T], out: Collector[R]) {
        out.collect(fun(in.iterator().asScala))
      }
    }
    wrap(
      new GroupReduceOperator[T, R](maybeCreateSortedGrouping(),
        implicitly[TypeInformation[R]], reducer))
  }

  /**
   * Creates a new [[DataSet]] by passing for each group (elements with the same key) the list
   * of elements to the group reduce function. The function can output zero or more elements using
   * the [[Collector]]. The concatenation of the emitted values will form the resulting [[DataSet]].
   */
  def reduceGroup[R: TypeInformation: ClassTag](
      fun: (TraversableOnce[T], Collector[R]) => Unit): DataSet[R] = {
    Validate.notNull(fun, "Group reduce function must not be null.")
    val reducer = new GroupReduceFunction[T, R] {
      def reduce(in: java.lang.Iterable[T], out: Collector[R]) {
        fun(in.iterator().asScala, out)
      }
    }
    wrap(
      new GroupReduceOperator[T, R](maybeCreateSortedGrouping(),
        implicitly[TypeInformation[R]], reducer))
  }

  /**
   * Creates a new [[DataSet]] by passing for each group (elements with the same key) the list
   * of elements to the [[GroupReduceFunction]]. The function can output zero or more elements. The
   * concatenation of the emitted values will form the resulting [[DataSet]].
   */
  def reduceGroup[R: TypeInformation: ClassTag](reducer: GroupReduceFunction[T, R]): DataSet[R] = {
    Validate.notNull(reducer, "GroupReduce function must not be null.")
    wrap(
      new GroupReduceOperator[T, R](maybeCreateSortedGrouping(),
        implicitly[TypeInformation[R]], reducer))
  }

  /**
   * Creates a new DataSet containing the first `n` elements of each group of this DataSet.
   */
  def first(n: Int): DataSet[T] = {
    if (n < 1) {
      throw new InvalidProgramException("Parameter n of first(n) must be at least 1.")
    }
    // Normally reduceGroup expects implicit parameters, supply them manually here.
    reduceGroup(new FirstReducer[T](n))(set.getType, implicitly[ClassTag[T]])
  }
}
