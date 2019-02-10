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

import java.util.{List => JList}

import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{SlicedStream => JavaSStream}
import org.apache.flink.streaming.api.scala.function.util._
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.slicing.Slice
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window


/**
 * A [[SlicedStream]] represents a [[WindowedStream]] with non-overlapping windows.
 *
 * @tparam T The type of elements in the stream.
 * @tparam K The type of the key by which elements are grouped.
 * @tparam W The type of [[Window]] that the
 *           [[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner]]
 *           assigns the elements to.
 */
@Public
class SlicedStream[T, K, W <: Window](javaStream: JavaSStream[T, K, W]) {

  /**
    * Sets the allowed lateness to a user-specified value.
    * If not explicitly set, the allowed lateness is [[0L]].
    * Setting the allowed lateness is only valid for event-time windows.
    * If a value different than 0 is provided with a processing-time
    * [[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner]],
    * then an exception is thrown.
    */
  @PublicEvolving
  def allowedLateness(lateness: Time): SlicedStream[T, K, W] = {
    javaStream.allowedLateness(lateness)
    this
  }

  /**
   * Send late arriving data to the side output identified by the given [[OutputTag]]. Data
   * is considered late after the watermark has passed the end of the window plus the allowed
   * lateness set using [[allowedLateness(Time)]].
   *
   * You can get the stream of late data using [[DataStream.getSideOutput()]] on the [[DataStream]]
   * resulting from the windowed operation with the same [[OutputTag]].
   */
  @PublicEvolving
  def sideOutputLateData(outputTag: OutputTag[T]): SlicedStream[T, K, W] = {
    javaStream.sideOutputLateData(outputTag)
    this
  }

  /**
   * Sets the [[Trigger]] that should be used to trigger window emission.
   */
  @PublicEvolving
  def trigger(trigger: Trigger[_ >: T, _ >: W]): SlicedStream[T, K, W] = {
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
  def evictor(evictor: Evictor[_ >: T, _ >: W]): SlicedStream[T, K, W] = {
    javaStream.evictor(evictor)
    this
  }

  // ------------------------------------------------------------------------
  //  Operations on the keyed windows
  // ------------------------------------------------------------------------

  // --------------------------- reduce() -----------------------------------

  /**
   * Applies a reduce function to the window. The window function is called for each evaluation
   * of the window for each key individually. The output of the reduce function can be
   * re-interpreted as a keyed stream.
   *
   * @param function The reduce function.
   * @return The data stream that is the result of applying the reduce function to the window.
   */
  def reduceToSlice(function: ReduceFunction[T]): DataStream[Slice[T, K, W]] = {
    asScalaStream(javaStream.reduceToSlice(clean(function)))
  }

  /**
   * Applies a reduce function to the window. The window function is called for each evaluation
   * of the window for each key individually. The output of the reduce function can be
   * re-interpreted as a keyed stream.
   *
   * @param function The reduce function.
   * @return The data stream that is the result of applying the reduce function to the window.
   */
  def reduceToSlice(function: (T, T) => T): DataStream[Slice[T, K, W]] = {
    if (function == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    val cleanFun = clean(function)
    val reducer = new ScalaReduceFunction[T](cleanFun)
    reduceToSlice(reducer)
  }

  // -------------------------- aggregate() ---------------------------------

  /**
   * Applies the given aggregation function to each window and key. The aggregation function
   * is called for each element, aggregating values incrementally and keeping the state to
   * one accumulator per key and window. The output of the reduce function can be
   * re-interpreted as a keyed stream.
   *
   * @param aggregateFunction The aggregation function.
   * @return The data stream that is the result of applying the aggregate function to the window.
   */
  @PublicEvolving
  def aggregateToSlice[ACC: TypeInformation](
      aggregateFunction: AggregateFunction[T, ACC, _]): DataStream[Slice[ACC, K, W]] = {

    val accumulatorType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]

    asScalaStream(javaStream.aggregateToSlice(clean(aggregateFunction), accumulatorType))
  }

  // ---------------------------- apply() -------------------------------------

  /**
    * Applied the given input stream by aggregating each individual element into an iterable
    * collection for downstream processing.
    *
    * @return The data stream that is the iterable result of the buffering operation.
    */
  def applyToSlice(): DataStream[Slice[JList[T], K, W]] = {
    asScalaStream(javaStream.applyToSlice())
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
