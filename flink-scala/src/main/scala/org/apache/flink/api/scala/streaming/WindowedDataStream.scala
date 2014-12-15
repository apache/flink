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

package org.apache.flink.api.scala.streaming
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.{ DataStream => JavaStream }
import org.apache.flink.streaming.api.datastream.{ WindowedDataStream => JavaWStream }
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.reflect.ClassTag
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.invokable.operator.MapInvokable
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.invokable.operator.FlatMapInvokable
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.streaming.api.invokable.StreamInvokable
import scala.collection.JavaConversions._

class WindowedDataStream[T](javaStream: JavaWStream[T]) {

  private[flink] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  /**
   * Defines the slide size (trigger frequency) for the windowed data stream.
   * This controls how often the user defined function will be triggered on
   * the window.
   */
  def every(windowingHelper: WindowingHelper[_]*): WindowedDataStream[T] = new WindowedDataStream[T](javaStream.every(windowingHelper: _*))

  /**
   * Groups the elements of the WindowedDataStream using the given
   * field positions. The window sizes (evictions) and slide sizes
   * (triggers) will be calculated on the whole stream (in a central fashion),
   * but the user defined functions will be applied on a per group basis.
   * </br></br> To get windows and triggers on a per group basis apply the
   * DataStream.window(...) operator on an already grouped data stream.
   *
   */
  def groupBy(fields: Int*): WindowedDataStream[T] =
    new WindowedDataStream[T](javaStream.groupBy(fields: _*))

  /**
   * Groups the elements of the WindowedDataStream using the given
   * field expressions. The window sizes (evictions) and slide sizes
   * (triggers) will be calculated on the whole stream (in a central fashion),
   * but the user defined functions will be applied on a per group basis.
   * </br></br> To get windows and triggers on a per group basis apply the
   * DataStream.window(...) operator on an already grouped data stream.
   *
   */
  def groupBy(firstField: String, otherFields: String*): WindowedDataStream[T] =
    new WindowedDataStream[T](javaStream.groupBy(firstField +: otherFields.toArray: _*))

  /**
   * Groups the elements of the WindowedDataStream using the given
   * KeySelector function. The window sizes (evictions) and slide sizes
   * (triggers) will be calculated on the whole stream (in a central fashion),
   * but the user defined functions will be applied on a per group basis.
   * </br></br> To get windows and triggers on a per group basis apply the
   * DataStream.window(...) operator on an already grouped data stream.
   *
   */
  def groupBy[K: TypeInformation](fun: T => K): WindowedDataStream[T] = {

    val keyExtractor = new KeySelector[T, K] {
      val cleanFun = clean(fun)
      def getKey(in: T) = cleanFun(in)
    }
    new WindowedDataStream[T](javaStream.groupBy(keyExtractor))
  }

  /**
   * Applies a reduce transformation on the windowed data stream by reducing
   * the current window at every trigger.
   *
   */
  def reduce(reducer: ReduceFunction[T]): DataStream[T] = {
    if (reducer == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    new DataStream[T](javaStream.reduce(reducer))
  }

  /**
   * Applies a reduce transformation on the windowed data stream by reducing
   * the current window at every trigger.
   *
   */
  def reduce(fun: (T, T) => T): DataStream[T] = {
    if (fun == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    val reducer = new ReduceFunction[T] {
      val cleanFun = clean(fun)
      def reduce(v1: T, v2: T) = { cleanFun(v1, v2) }
    }
    reduce(reducer)
  }

  /**
   * Applies a reduceGroup transformation on the windowed data stream by reducing
   * the current window at every trigger. In contrast with the simple binary reduce operator, groupReduce exposes the whole window through the Iterable interface.
   * </br>
   * </br>
   * Whenever possible try to use reduce instead of groupReduce for increased efficiency
   */
  def reduceGroup[R: ClassTag: TypeInformation](reducer: GroupReduceFunction[T, R]): DataStream[R] = {
    if (reducer == null) {
      throw new NullPointerException("GroupReduce function must not be null.")
    }
    new DataStream[R](javaStream.reduceGroup(reducer, implicitly[TypeInformation[R]]))
  }

  /**
   * Applies a reduceGroup transformation on the windowed data stream by reducing
   * the current window at every trigger. In contrast with the simple binary reduce operator, groupReduce exposes the whole window through the Iterable interface.
   * </br>
   * </br>
   * Whenever possible try to use reduce instead of groupReduce for increased efficiency
   */
  def reduceGroup[R: ClassTag: TypeInformation](fun: (Iterable[T], Collector[R]) => Unit): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("GroupReduce function must not be null.")
    }
    val reducer = new GroupReduceFunction[T, R] {
      val cleanFun = clean(fun)
      def reduce(in: java.lang.Iterable[T], out: Collector[R]) = { cleanFun(in, out) }
    }
    reduceGroup(reducer)
  }

  /**
   * Applies an aggregation that that gives the maximum of the elements in the window at
   * the given position.
   *
   */
  def max(field: Any): DataStream[T] = field match {
    case field: Int => return new DataStream[T](javaStream.max(field))
    case field: String => return new DataStream[T](javaStream.max(field))
    case _ => throw new IllegalArgumentException("Aggregations are only supported by field position (Int) or field expression (String)")
  }

  /**
   * Applies an aggregation that that gives the minimum of the elements in the window at
   * the given position.
   *
   */
  def min(field: Any): DataStream[T] = field match {
    case field: Int => return new DataStream[T](javaStream.min(field))
    case field: String => return new DataStream[T](javaStream.min(field))
    case _ => throw new IllegalArgumentException("Aggregations are only supported by field position (Int) or field expression (String)")
  }

  /**
   * Applies an aggregation that sums the elements in the window at the given position.
   *
   */
  def sum(field: Any): DataStream[T] = field match {
    case field: Int => return new DataStream[T](javaStream.sum(field))
    case field: String => return new DataStream[T](javaStream.sum(field))
    case _ => throw new IllegalArgumentException("Aggregations are only supported by field position (Int) or field expression (String)")
  }

  /**
   * Applies an aggregation that that gives the maximum element of the window by
   * the given position. When equality, returns the first.
   *
   */
  def maxBy(field: Any): DataStream[T] = field match {
    case field: Int => return new DataStream[T](javaStream.maxBy(field))
    case field: String => return new DataStream[T](javaStream.maxBy(field))
    case _ => throw new IllegalArgumentException("Aggregations are only supported by field position (Int) or field expression (String)")
  }

  /**
   * Applies an aggregation that that gives the minimum element of the window by
   * the given position. When equality, returns the first.
   *
   */
  def minBy(field: Any): DataStream[T] = field match {
    case field: Int => return new DataStream[T](javaStream.minBy(field))
    case field: String => return new DataStream[T](javaStream.minBy(field))
    case _ => throw new IllegalArgumentException("Aggregations are only supported by field position (Int) or field expression (String)")
  }

  /**
   * Applies an aggregation that that gives the minimum element of the window by
   * the given position. When equality, the user can set to get the first or last element with the minimal value.
   *
   */
  def minBy(field: Any, first: Boolean): DataStream[T] = field match {
    case field: Int => return new DataStream[T](javaStream.minBy(field, first))
    case field: String => return new DataStream[T](javaStream.minBy(field, first))
    case _ => throw new IllegalArgumentException("Aggregations are only supported by field position (Int) or field expression (String)")
  }

  /**
   * Applies an aggregation that that gives the maximum element of the window by
   * the given position. When equality, the user can set to get the first or last element with the maximal value.
   *
   */
  def maxBy(field: Any, first: Boolean): DataStream[T] = field match {
    case field: Int => return new DataStream[T](javaStream.maxBy(field, first))
    case field: String => return new DataStream[T](javaStream.maxBy(field, first))
    case _ => throw new IllegalArgumentException("Aggregations are only supported by field position (Int) or field expression (String)")
  }

}