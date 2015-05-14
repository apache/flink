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

import org.apache.flink.api.common.InvalidProgramException
import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction, Partitioner, ReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.functions.{FirstReducer, KeySelector}
import org.apache.flink.api.java.operators._
import org.apache.flink.api.scala.operators.ScalaAggregateOperator
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
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

  private var partitioner : Partitioner[_] = _

  private var groupSortKeySelector: Option[Keys.SelectorFunctionKeys[T, _]] = None

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
    if (keys.isInstanceOf[Keys.SelectorFunctionKeys[_, _]]) {
      throw new InvalidProgramException("KeySelector grouping keys and field index group-sorting " +
        "keys cannot be used together.")
    }
    if (groupSortKeySelector.nonEmpty) {
      throw new InvalidProgramException("Chaining sortGroup with KeySelector sorting is not " +
        "supported.")
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
    if (groupSortKeySelector.nonEmpty) {
      throw new InvalidProgramException("Chaining sortGroup with KeySelector sorting is not" +
        "supported.")
    }
    if (keys.isInstanceOf[Keys.SelectorFunctionKeys[_, _]]) {
      throw new InvalidProgramException("KeySelector grouping keys and field expression " +
        "group-sorting keys cannot be used together.")
    }
    groupSortKeyPositions += Right(field)
    groupSortOrders += order
    this
  }

  /**
   * Adds a secondary sort key to this [[GroupedDataSet]]. This will only have an effect if you
   * use one of the group-at-a-time, i.e. `reduceGroup`.
   *
   * This works on any data type.
   */
  def sortGroup[K: TypeInformation](fun: T => K, order: Order): GroupedDataSet[T] = {
    if (groupSortOrders.length != 0) {
      throw new InvalidProgramException("Chaining sortGroup with KeySelector sorting is not" +
        "supported.")
    }
    if (!keys.isInstanceOf[Keys.SelectorFunctionKeys[_, _]]) {
      throw new InvalidProgramException("Sorting on KeySelector keys only works with KeySelector " +
        "grouping.")
    }

    groupSortOrders += order
    val keyType = implicitly[TypeInformation[K]]
    val keyExtractor = new KeySelector[T, K] {
      def getKey(in: T) = fun(in)
    }
    groupSortKeySelector = Some(new Keys.SelectorFunctionKeys[T, K](
      keyExtractor,
      set.javaSet.getType,
      keyType))
    this
  }
  /**
   * Creates a [[SortedGrouping]] if group sorting keys were specified.
   */
  private def maybeCreateSortedGrouping(): Grouping[T] = {
    groupSortKeySelector match {
      case Some(keySelector) =>
        if (partitioner == null) {
          new SortedGrouping[T](set.javaSet, keys, keySelector, groupSortOrders(0))
        } else {
          new SortedGrouping[T](set.javaSet, keys, keySelector, groupSortOrders(0))
            .withPartitioner(partitioner)
        }
      case None =>
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

          if (partitioner == null) {
            grouping
          } else {
            grouping.withPartitioner(partitioner)
          }

      } else {
        createUnsortedGrouping()
      }
    }
  }


  /** Convenience methods for creating the [[UnsortedGrouping]] */
  private def createUnsortedGrouping(): Grouping[T] = {
    val grp = new UnsortedGrouping[T](set.javaSet, keys)
    if (partitioner == null) {
      grp
    } else {
      grp.withPartitioner(partitioner)
    }
  }

  /**
   * Sets a custom partitioner for the grouping.
   */
  def withPartitioner[K : TypeInformation](partitioner: Partitioner[K]) : GroupedDataSet[T] = {
    require(partitioner != null)
    keys.validateCustomPartitioner(partitioner, implicitly[TypeInformation[K]])
    this.partitioner = partitioner
    this
  }
  
  /**
   * Gets the custom partitioner to be used for this grouping, or null, if
   * none was defined.
   */
  def getCustomPartitioner[K]() : Partitioner[K] = {
    partitioner.asInstanceOf[Partitioner[K]]
  }
  
  // ----------------------------------------------------------------------------------------------
  //  Operations
  // ----------------------------------------------------------------------------------------------
  
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
    require(fun != null, "Reduce function must not be null.")
    val reducer = new ReduceFunction[T] {
      val cleanFun = set.clean(fun)
      def reduce(v1: T, v2: T) = {
        cleanFun(v1, v2)
      }
    }
    wrap(new ReduceOperator[T](createUnsortedGrouping(), reducer, getCallLocationName()))
  }

  /**
   * Creates a new [[DataSet]] by merging the elements of each group (elements with the same key)
   * using an associative reduce function.
   */
  def reduce(reducer: ReduceFunction[T]): DataSet[T] = {
    require(reducer != null, "Reduce function must not be null.")
    wrap(new ReduceOperator[T](createUnsortedGrouping(), reducer, getCallLocationName()))
  }

  /**
   * Creates a new [[DataSet]] by passing for each group (elements with the same key) the list
   * of elements to the group reduce function. The function must output one element. The
   * concatenation of those will form the resulting [[DataSet]].
   */
  def reduceGroup[R: TypeInformation: ClassTag](
      fun: (Iterator[T]) => R): DataSet[R] = {
    require(fun != null, "Group reduce function must not be null.")
    val reducer = new GroupReduceFunction[T, R] {
      val cleanFun = set.clean(fun)
      def reduce(in: java.lang.Iterable[T], out: Collector[R]) {
        out.collect(cleanFun(in.iterator().asScala))
      }
    }
    wrap(
      new GroupReduceOperator[T, R](maybeCreateSortedGrouping(),
        implicitly[TypeInformation[R]], reducer, getCallLocationName()))
  }

  /**
   * Creates a new [[DataSet]] by passing for each group (elements with the same key) the list
   * of elements to the group reduce function. The function can output zero or more elements using
   * the [[Collector]]. The concatenation of the emitted values will form the resulting [[DataSet]].
   */
  def reduceGroup[R: TypeInformation: ClassTag](
      fun: (Iterator[T], Collector[R]) => Unit): DataSet[R] = {
    require(fun != null, "Group reduce function must not be null.")
    val reducer = new GroupReduceFunction[T, R] {
      val cleanFun = set.clean(fun)
      def reduce(in: java.lang.Iterable[T], out: Collector[R]) {
        cleanFun(in.iterator().asScala, out)
      }
    }
    wrap(
      new GroupReduceOperator[T, R](maybeCreateSortedGrouping(),
        implicitly[TypeInformation[R]], reducer, getCallLocationName()))
  }

  /**
   * Creates a new [[DataSet]] by passing for each group (elements with the same key) the list
   * of elements to the [[GroupReduceFunction]]. The function can output zero or more elements. The
   * concatenation of the emitted values will form the resulting [[DataSet]].
   */
  def reduceGroup[R: TypeInformation: ClassTag](reducer: GroupReduceFunction[T, R]): DataSet[R] = {
    require(reducer != null, "GroupReduce function must not be null.")
    wrap(
      new GroupReduceOperator[T, R](maybeCreateSortedGrouping(),
        implicitly[TypeInformation[R]], reducer, getCallLocationName()))
  }

  /**
   *  Applies a CombineFunction on a grouped [[DataSet]].  A
   *  CombineFunction is similar to a GroupReduceFunction but does not
   *  perform a full data exchange. Instead, the CombineFunction calls
   *  the combine method once per partition for combining a group of
   *  results. This operator is suitable for combining values into an
   *  intermediate format before doing a proper groupReduce where the
   *  data is shuffled across the node for further reduction. The
   *  GroupReduce operator can also be supplied with a combiner by
   *  implementing the RichGroupReduce function. The combine method of
   *  the RichGroupReduce function demands input and output type to be
   *  the same. The CombineFunction, on the other side, can have an
   *  arbitrary output type.
   */
  def combineGroup[R: TypeInformation: ClassTag](
                                          fun: (Iterator[T], Collector[R]) => Unit): DataSet[R] = {
    require(fun != null, "GroupCombine function must not be null.")
    val combiner = new GroupCombineFunction[T, R] {
      val cleanFun = set.clean(fun)
      def combine(in: java.lang.Iterable[T], out: Collector[R]) {
        cleanFun(in.iterator().asScala, out)
      }
    }
    wrap(
      new GroupCombineOperator[T, R](maybeCreateSortedGrouping(),
        implicitly[TypeInformation[R]], combiner, getCallLocationName()))
  }

  /**
   *  Applies a CombineFunction on a grouped [[DataSet]].  A
   *  CombineFunction is similar to a GroupReduceFunction but does not
   *  perform a full data exchange. Instead, the CombineFunction calls
   *  the combine method once per partition for combining a group of
   *  results. This operator is suitable for combining values into an
   *  intermediate format before doing a proper groupReduce where the
   *  data is shuffled across the node for further reduction. The
   *  GroupReduce operator can also be supplied with a combiner by
   *  implementing the RichGroupReduce function. The combine method of
   *  the RichGroupReduce function demands input and output type to be
   *  the same. The CombineFunction, on the other side, can have an
   *  arbitrary output type.
   */
  def combineGroup[R: TypeInformation: ClassTag](
      combiner: GroupCombineFunction[T, R]): DataSet[R] = {
    require(combiner != null, "GroupCombine function must not be null.")
    wrap(
      new GroupCombineOperator[T, R](maybeCreateSortedGrouping(),
        implicitly[TypeInformation[R]], combiner, getCallLocationName()))
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
