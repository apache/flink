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

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{AllWindowedStream => JavaAllWStream}
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window

import scala.reflect.ClassTag

/**
 * A [[AllWindowedStream]] represents a data stream where the stream of
 * elements is split into windows based on a
 * [[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner]]. Window emission
 * is triggered based on a [[Trigger]].
 *
 * If an [[Evictor]] is specified it will be
 * used to evict elements from the window after
 * evaluation was triggered by the [[Trigger]] but before the actual evaluation of the window.
 * When using an evictor window performance will degrade significantly, since
 * pre-aggregation of window results cannot be used.
 *
 * Note that the [[AllWindowedStream()]] is purely and API construct, during runtime
 * the [[AllWindowedStream()]] will be collapsed together with the
 * operation over the window into one single operation.
 *
 * @tparam T The type of elements in the stream.
 * @tparam W The type of [[Window]] that the
 *           [[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner]]
 *           assigns the elements to.
 */
class AllWindowedStream[T, W <: Window](javaStream: JavaAllWStream[T, W]) {

  /**
   * Sets the [[Trigger]] that should be used to trigger window emission.
   */
  def trigger(trigger: Trigger[_ >: T, _ >: W]): AllWindowedStream[T, W] = {
    javaStream.trigger(trigger)
    this
  }

  /**
   * Sets the [[Evictor]] that should be used to evict elements from a window before emission.
   *
   * Note: When using an evictor window performance will degrade significantly, since
   * pre-aggregation of window results cannot be used.
   */
  def evictor(evictor: Evictor[_ >: T, _ >: W]): AllWindowedStream[T, W] = {
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
  def reduceWindow(function: ReduceFunction[T]): DataStream[T] = {
    javaStream.reduceWindow(clean(function))
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
  def apply[R: TypeInformation: ClassTag](function: AllWindowFunction[T, R, W]): DataStream[R] = {
    javaStream.apply(clean(function), implicitly[TypeInformation[R]])
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

}
