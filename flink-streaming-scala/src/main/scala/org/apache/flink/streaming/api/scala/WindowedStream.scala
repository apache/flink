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

package org.apache.flink.streaming.api.scala

import org.apache.flink.annotation.{PublicEvolving, Public}
import org.apache.flink.api.common.functions.{FoldFunction, ReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{WindowedStream => JavaWStream}
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType
import org.apache.flink.streaming.api.functions.aggregation.{ComparableAggregator, SumAggregator}
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
 * A [[WindowedStream]] represents a data stream where elements are grouped by
 * key, and for each key, the stream of elements is split into windows based on a
 * [[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner]]. Window emission
 * is triggered based on a [[Trigger]].
 *
 * The windows are conceptually evaluated for each key individually, meaning windows can trigger at
 * different points for each key.
 *
 * If an [[org.apache.flink.streaming.api.windowing.evictors.Evictor]] is specified it will
 * be used to evict elements from the window after evaluation was triggered by the [[Trigger]]
 * but before the actual evaluation of the window. When using an evictor window performance will
 * degrade significantly, since pre-aggregation of window results cannot be used.
 *
 * Note that the [[WindowedStream]] is purely and API construct, during runtime
 * the [[WindowedStream]] will be collapsed together with the
 * [[KeyedStream]] and the operation over the window into one single operation.
 *
 * @tparam T The type of elements in the stream.
 * @tparam K The type of the key by which elements are grouped.
 * @tparam W The type of [[Window]] that the
 *           [[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner]]
 *           assigns the elements to.
 */
@Public
class WindowedStream[T, K, W <: Window](javaStream: JavaWStream[T, K, W]) {

  /**
   * Sets the [[Trigger]] that should be used to trigger window emission.
   */
  @PublicEvolving
  def trigger(trigger: Trigger[_ >: T, _ >: W]): WindowedStream[T, K, W] = {
    javaStream.trigger(trigger)
    this
  }

  /**
   * Sets the [[Evictor]] that should be used to evict elements from a window before emission.
   *
   * Note: When using an evictor window performance will degrade significantly, since
   * pre-aggregation of window results cannot be used.
   */
  @PublicEvolving
  def evictor(evictor: Evictor[_ >: T, _ >: W]): WindowedStream[T, K, W] = {
    javaStream.evictor(evictor)
    this
  }

  // ------------------------------------------------------------------------
  //  Operations on the keyed windows
  // ------------------------------------------------------------------------

  /**
   * Applies a reduce function to the window. The window function is called for each evaluation
   * of the window for each key individually. The output of the reduce function is interpreted
   * as a regular non-windowed stream.
   *
   * This window will try and pre-aggregate data as much as the window policies permit. For example,
   * tumbling time windows can perfectly pre-aggregate the data, meaning that only one element per
   * key is stored. Sliding time windows will pre-aggregate on the granularity of the slide
   * interval, so a few elements are stored per key (one per slide interval).
   * Custom windows may not be able to pre-aggregate, or may need to store extra values in an
   * aggregation tree.
   *
   * @param function The reduce function.
   * @return The data stream that is the result of applying the reduce function to the window.
   */
  def reduce(function: ReduceFunction[T]): DataStream[T] = {
    asScalaStream(javaStream.reduce(clean(function)))
  }

  /**
   * Applies a reduce function to the window. The window function is called for each evaluation
   * of the window for each key individually. The output of the reduce function is interpreted
   * as a regular non-windowed stream.
   *
   * This window will try and pre-aggregate data as much as the window policies permit. For example,
   * tumbling time windows can perfectly pre-aggregate the data, meaning that only one element per
   * key is stored. Sliding time windows will pre-aggregate on the granularity of the slide
   * interval, so a few elements are stored per key (one per slide interval).
   * Custom windows may not be able to pre-aggregate, or may need to store extra values in an
   * aggregation tree.
   *
   * @param function The reduce function.
   * @return The data stream that is the result of applying the reduce function to the window.
   */
  def reduce(function: (T, T) => T): DataStream[T] = {
    if (function == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    val cleanFun = clean(function)
    val reducer = new ReduceFunction[T] {
      def reduce(v1: T, v2: T) = { cleanFun(v1, v2) }
    }
    reduce(reducer)
  }

  /**
   * Applies the given fold function to each window. The window function is called for each
   * evaluation of the window for each key individually. The output of the reduce function is
   * interpreted as a regular non-windowed stream.
   *
   * @param function The fold function.
   * @return The data stream that is the result of applying the fold function to the window.
   */
  def fold[R: TypeInformation](
      initialValue: R,
      function: FoldFunction[T,R]): DataStream[R] = {
    if (function == null) {
      throw new NullPointerException("Fold function must not be null.")
    }

    val resultType : TypeInformation[R] = implicitly[TypeInformation[R]]

    asScalaStream(javaStream.fold(initialValue, function, resultType))
  }

  /**
   * Applies the given fold function to each window. The window function is called for each
   * evaluation of the window for each key individually. The output of the reduce function is
   * interpreted as a regular non-windowed stream.
   *
   * @param function The fold function.
   * @return The data stream that is the result of applying the fold function to the window.
   */
  def fold[R: TypeInformation](initialValue: R, function: (R, T) => R): DataStream[R] = {
    if (function == null) {
      throw new NullPointerException("Fold function must not be null.")
    }
    val cleanFun = clean(function)
    val folder = new FoldFunction[T,R] {
      def fold(acc: R, v: T) = {
        cleanFun(acc, v)
      }
    }
    fold(initialValue, folder)
  }

  /**
   * Applies the given window function to each window. The window function is called for each
   * evaluation of the window for each key individually. The output of the window function is
   * interpreted as a regular non-windowed stream.
   *
   * Not that this function requires that all data in the windows is buffered until the window
   * is evaluated, as the function provides no means of pre-aggregation.
   *
   * @param function The window function.
   * @return The data stream that is the result of applying the window function to the window.
   */
  def apply[R: TypeInformation](
      function: WindowFunction[Iterable[T], R, K, W]): DataStream[R] = {
    
    val cleanFunction = clean(function)
    val javaFunction = new WindowFunction[java.lang.Iterable[T], R, K, W] {
      def apply(key: K, window: W, input: java.lang.Iterable[T], out: Collector[R]) = {
        cleanFunction.apply(key, window, input.asScala, out)
      }
    }

    asScalaStream(javaStream.apply(javaFunction, implicitly[TypeInformation[R]]))
  }

  /**
   * Applies the given window function to each window. The window function is called for each
   * evaluation of the window for each key individually. The output of the window function is
   * interpreted as a regular non-windowed stream.
   *
   * Not that this function requires that all data in the windows is buffered until the window
   * is evaluated, as the function provides no means of pre-aggregation.
   *
   * @param function The window function.
   * @return The data stream that is the result of applying the window function to the window.
   */
  def apply[R: TypeInformation](
      function: (K, W, Iterable[T], Collector[R]) => Unit): DataStream[R] = {
    if (function == null) {
      throw new NullPointerException("WindowApply function must not be null.")
    }

    val cleanedFunction = clean(function)
    val applyFunction = new WindowFunction[java.lang.Iterable[T], R, K, W] {
      def apply(key: K, window: W, elements: java.lang.Iterable[T], out: Collector[R]): Unit = {
        cleanedFunction(key, window, elements.asScala, out)
      }
    }
    asScalaStream(javaStream.apply(applyFunction, implicitly[TypeInformation[R]]))
  }

  /**
   * Applies the given window function to each window. The window function is called for each
   * evaluation of the window for each key individually. The output of the window function is
   * interpreted as a regular non-windowed stream.
   *
   * Arriving data is pre-aggregated using the given pre-aggregation reducer.
   *
   * @param preAggregator The reduce function that is used for pre-aggregation
   * @param function The window function.
   * @return The data stream that is the result of applying the window function to the window.
   */
  def apply[R: TypeInformation](
      preAggregator: ReduceFunction[T],
      function: WindowFunction[T, R, K, W]): DataStream[R] = {

    val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]
    asScalaStream(javaStream.apply(clean(preAggregator), clean(function), resultType))
  }

  /**
   * Applies the given window function to each window. The window function is called for each
   * evaluation of the window for each key individually. The output of the window function is
   * interpreted as a regular non-windowed stream.
   *
   * Arriving data is pre-aggregated using the given pre-aggregation reducer.
   *
   * @param preAggregator The reduce function that is used for pre-aggregation
   * @param function The window function.
   * @return The data stream that is the result of applying the window function to the window.
   */
  def apply[R: TypeInformation](
      preAggregator: (T, T) => T,
      function: (K, W, T, Collector[R]) => Unit): DataStream[R] = {
    
    if (function == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    if (function == null) {
      throw new NullPointerException("WindowApply function must not be null.")
    }

    val cleanReducer = clean(preAggregator)
    val reducer = new ReduceFunction[T] {
      def reduce(v1: T, v2: T) = { cleanReducer(v1, v2) }
    }

    val cleanApply = clean(function)
    val applyFunction = new WindowFunction[T, R, K, W] {
      def apply(key: K, window: W, input: T, out: Collector[R]): Unit = {
        cleanApply(key, window, input, out)
      }
    }
    
    asScalaStream(javaStream.apply(reducer, applyFunction, implicitly[TypeInformation[R]]))
  }

  /**
    * Applies the given window function to each window. The window function is called for each
    * evaluation of the window for each key individually. The output of the window function is
    * interpreted as a regular non-windowed stream.
    *
    * Arriving data is incrementally aggregated using the given fold function.
    *
    * @param initialValue The initial value of the fold
    * @param foldFunction The fold function that is used for incremental aggregation
    * @param function The window function.
    * @return The data stream that is the result of applying the window function to the window.
    */
  def apply[R: TypeInformation](
      initialValue: R,
      foldFunction: FoldFunction[T, R],
      function: WindowFunction[R, R, K, W]): DataStream[R] = {
    
    asScalaStream(javaStream.apply(
      initialValue,
      clean(foldFunction),
      clean(function),
      implicitly[TypeInformation[R]]))
  }

  /**
    * Applies the given window function to each window. The window function is called for each
    * evaluation of the window for each key individually. The output of the window function is
    * interpreted as a regular non-windowed stream.
    *
    * Arriving data is incrementally aggregated using the given fold function.
    *
    * @param foldFunction The fold function that is used for incremental aggregation
    * @param function The window function.
    * @return The data stream that is the result of applying the window function to the window.
    */
  def apply[R: TypeInformation](
      initialValue: R,
      foldFunction: (R, T) => R,
      function: (K, W, R, Collector[R]) => Unit): DataStream[R] = {
    
    if (function == null) {
      throw new NullPointerException("Fold function must not be null.")
    }
    if (function == null) {
      throw new NullPointerException("WindowApply function must not be null.")
    }

    val cleanFolder = clean(foldFunction)
    val folder = new FoldFunction[T, R] {
      def fold(acc: R, v: T) = { cleanFolder(acc, v) }
    }

    val cleanApply = clean(function)
    val applyFunction = new WindowFunction[R, R, K, W] {
      def apply(key: K, window: W, input: R, out: Collector[R]): Unit = {
        cleanApply(key, window, input, out)
      }
    }
    val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]
    asScalaStream(javaStream.apply(initialValue, folder, applyFunction, resultType))
  }

  // ------------------------------------------------------------------------
  //  Aggregations on the keyed windows
  // ------------------------------------------------------------------------

  /**
   * Applies an aggregation that that gives the maximum of the elements in the window at
   * the given position.
   */
  def max(position: Int): DataStream[T] = aggregate(AggregationType.MAX, position)

  /**
   * Applies an aggregation that that gives the maximum of the elements in the window at
   * the given field.
   */
  def max(field: String): DataStream[T] = aggregate(AggregationType.MAX, field)

  /**
   * Applies an aggregation that that gives the minimum of the elements in the window at
   * the given position.
   */
  def min(position: Int): DataStream[T] = aggregate(AggregationType.MIN, position)

  /**
   * Applies an aggregation that that gives the minimum of the elements in the window at
   * the given field.
   */
  def min(field: String): DataStream[T] = aggregate(AggregationType.MIN, field)

  /**
   * Applies an aggregation that sums the elements in the window at the given position.
   */
  def sum(position: Int): DataStream[T] = aggregate(AggregationType.SUM, position)

  /**
   * Applies an aggregation that sums the elements in the window at the given field.
   */
  def sum(field: String): DataStream[T] = aggregate(AggregationType.SUM, field)

  /**
   * Applies an aggregation that that gives the maximum element of the window by
   * the given position. When equality, returns the first.
   */
  def maxBy(position: Int): DataStream[T] = aggregate(AggregationType.MAXBY,
    position)

  /**
   * Applies an aggregation that that gives the maximum element of the window by
   * the given field. When equality, returns the first.
   */
  def maxBy(field: String): DataStream[T] = aggregate(AggregationType.MAXBY,
    field)

  /**
   * Applies an aggregation that that gives the minimum element of the window by
   * the given position. When equality, returns the first.
   */
  def minBy(position: Int): DataStream[T] = aggregate(AggregationType.MINBY,
    position)

  /**
   * Applies an aggregation that that gives the minimum element of the window by
   * the given field. When equality, returns the first.
   */
  def minBy(field: String): DataStream[T] = aggregate(AggregationType.MINBY,
    field)

  private def aggregate(aggregationType: AggregationType, field: String): DataStream[T] = {
    val position = fieldNames2Indices(getInputType(), Array(field))(0)
    aggregate(aggregationType, position)
  }

  def aggregate(aggregationType: AggregationType, position: Int): DataStream[T] = {

    val jStream = javaStream.asInstanceOf[JavaWStream[Product, K, W]]

    val reducer = aggregationType match {
      case AggregationType.SUM =>
        new SumAggregator(position, jStream.getInputType, jStream.getExecutionEnvironment.getConfig)

      case _ =>
        new ComparableAggregator(
          position,
          jStream.getInputType,
          aggregationType,
          true,
          jStream.getExecutionEnvironment.getConfig)
    }

    new DataStream[Product](jStream.reduce(reducer)).asInstanceOf[DataStream[T]]
  }

  // ------------------------------------------------------------------------
  //  Utilities
  // ------------------------------------------------------------------------

  /**
   * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
   * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
   */
  private[flink] def clean[F <: AnyRef](f: F): F = {
    new StreamExecutionEnvironment(javaStream.getExecutionEnvironment).scalaClean(f)
  }

  /**
   * Gets the output type.
   */
  private def getInputType(): TypeInformation[T] = javaStream.getInputType
}
