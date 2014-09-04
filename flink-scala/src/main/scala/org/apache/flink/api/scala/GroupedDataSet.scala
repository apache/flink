/**
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
import org.apache.flink.api.scala.operators.ScalaAggregateOperator

import scala.collection.JavaConverters._

import org.apache.commons.lang3.Validate
import org.apache.flink.api.common.functions.{GroupReduceFunction, ReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.operators._
import org.apache.flink.types.TypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.api.java.{DataSet => JavaDataSet}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * A [[DataSet]] to which a grouping key was added. Operations work on groups of elements with the
 * same key (`aggregate`, `reduce`, and `reduceGroup`).
 *
 * A secondary sort order can be added with sortGroup, but this is only used when using one
 * of the group-at-a-time operations, i.e. `reduceGroup`.
 */
trait GroupedDataSet[T] {

  /**
   * Adds a secondary sort key to this [[GroupedDataSet]]. This will only have an effect if you
   * use one of the group-at-a-time, i.e. `reduceGroup`
   */
  def sortGroup(field: Int, order: Order): GroupedDataSet[T]

  /**
   * Creates a new [[DataSet]] by aggregating the specified tuple field using the given aggregation
   * function. Since this is a keyed DataSet the aggregation will be performed on groups of
   * tuples with the same key.
   *
   * This only works on Tuple DataSets.
   */
  def aggregate(agg: Aggregations, field: Int): DataSet[T]

  /**
   * Syntactic sugar for [[aggregate]] with `SUM`
   */
  def sum(field: Int): DataSet[T]

  /**
   * Syntactic sugar for [[aggregate]] with `MAX`
   */
  def max(field: Int): DataSet[T]

  /**
   * Syntactic sugar for [[aggregate]] with `MIN`
   */
  def min(field: Int): DataSet[T]

  /**
   * Creates a new [[DataSet]] by merging the elements of each group (elements with the same key)
   * using an associative reduce function.
   */
  def reduce(fun: (T, T) => T): DataSet[T]

  /**
   * Creates a new [[DataSet]] by merging the elements of each group (elements with the same key)
   * using an associative reduce function.
   */
  def reduce(reducer: ReduceFunction[T]): DataSet[T]

  /**
   * Creates a new [[DataSet]] by passing for each group (elements with the same key) the list
   * of elements to the group reduce function. The function must output one element. The
   * concatenation of those will form the resulting [[DataSet]].
   */
  def reduceGroup[R: TypeInformation: ClassTag](fun: (TraversableOnce[T]) => R): DataSet[R]

  /**
   * Creates a new [[DataSet]] by passing for each group (elements with the same key) the list
   * of elements to the group reduce function. The function can output zero or more elements using
   * the [[Collector]]. The concatenation of the emitted values will form the resulting [[DataSet]].
   */
  def reduceGroup[R: TypeInformation: ClassTag](
      fun: (TraversableOnce[T], Collector[R]) => Unit): DataSet[R]

  /**
   * Creates a new [[DataSet]] by passing for each group (elements with the same key) the list
   * of elements to the [[GroupReduceFunction]]. The function can output zero or more elements. The
   * concatenation of the emitted values will form the resulting [[DataSet]].
   */
  def reduceGroup[R: TypeInformation: ClassTag](reducer: GroupReduceFunction[T, R]): DataSet[R]
}

/**
 * /**
 * Private implementation for [[GroupedDataSet]] to keep the implementation details, i.e. the
 * parameters of the constructor, hidden.
 */
 */
private[flink] class GroupedDataSetImpl[T: ClassTag](
    private val set: JavaDataSet[T],
    private val keys: Keys[T])
  extends GroupedDataSet[T] {

  // These are for optional secondary sort. They are only used
  // when using a group-at-a-time reduce function.
  private val groupSortKeyPositions = mutable.MutableList[Int]()
  private val groupSortOrders = mutable.MutableList[Order]()

  /**
   * Adds a secondary sort key to this [[GroupedDataSet]]. This will only have an effect if you
   * use one of the group-at-a-time, i.e. `reduceGroup`
   */
  def sortGroup(field: Int, order: Order): GroupedDataSet[T] = {
    if (!set.getType.isTupleType) {
      throw new InvalidProgramException("Specifying order keys via field positions is only valid " +
        "for tuple data types")
    }
    if (field >= set.getType.getArity) {
      throw new IllegalArgumentException("Order key out of tuple bounds.")
    }
    groupSortKeyPositions += field
    groupSortOrders += order
    this
  }

  /**
   * Creates a [[SortedGrouping]] if any secondary sort fields were specified. Otherwise, just
   * create an [[UnsortedGrouping]].
   */
  private def maybeCreateSortedGrouping(): Grouping[T] = {
    if (groupSortKeyPositions.length > 0) {
      val grouping = new SortedGrouping[T](set, keys, groupSortKeyPositions(0), groupSortOrders(0))
      // now manually add the rest of the keys
      for (i <- 1 until groupSortKeyPositions.length) {
        grouping.sortGroup(groupSortKeyPositions(i), groupSortOrders(i))
      }
      grouping
    } else {
      new UnsortedGrouping[T](set, keys)
    }
  }

  /** Convenience methods for creating the [[UnsortedGrouping]] */
  private def createUnsortedGrouping(): Grouping[T] = new UnsortedGrouping[T](set, keys)

  /**
   * Creates a new [[DataSet]] by aggregating the specified tuple field using the given aggregation
   * function. Since this is a keyed DataSet the aggregation will be performed on groups of
   * tuples with the same key.
   *
   * This only works on Tuple DataSets.
   */
  def aggregate(agg: Aggregations, field: Int): DataSet[T] = set match {
    case aggregation: ScalaAggregateOperator[T] =>
      aggregation.and(agg, field)
      wrap(aggregation)

    case _ => wrap(new ScalaAggregateOperator[T](createUnsortedGrouping(), agg, field))
  }

  /**
   * Syntactic sugar for [[aggregate]] with `SUM`
   */
  def sum(field: Int): DataSet[T] = {
    aggregate(Aggregations.SUM, field)
  }

  /**
   * Syntactic sugar for [[aggregate]] with `MAX`
   */
  def max(field: Int): DataSet[T] = {
    aggregate(Aggregations.MAX, field)
  }

  /**
   * Syntactic sugar for [[aggregate]] with `MIN`
   */
  def min(field: Int): DataSet[T] = {
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
      new GroupReduceOperator[T, R](createUnsortedGrouping(),
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
      new GroupReduceOperator[T, R](createUnsortedGrouping(),
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
}
