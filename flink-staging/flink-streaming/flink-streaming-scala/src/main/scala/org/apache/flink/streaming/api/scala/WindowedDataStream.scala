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

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.reflect.ClassTag

import org.apache.flink.api.common.functions.{FoldFunction, ReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.streaming.scala.ScalaStreamingAggregator
import org.apache.flink.streaming.api.datastream.{WindowedDataStream => JavaWStream, DiscretizedStream}
import org.apache.flink.streaming.api.functions.WindowMapFunction
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType
import org.apache.flink.streaming.api.functions.aggregation.SumFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.clean
import org.apache.flink.streaming.api.windowing.StreamWindow
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper
import org.apache.flink.util.Collector

class WindowedDataStream[T](javaStream: JavaWStream[T]) {

  /**
   * Gets the name of the current data stream. This name is
   * used by the visualization and logging during runtime.
   *
   * @return Name of the stream.
   */
  def getName : String = javaStream match {
    case stream : DiscretizedStream[_] => javaStream.getName
    case _ => throw new
        UnsupportedOperationException("Only supported for windowing operators.")
  }

  /**
   * Sets the name of the current data stream. This name is
   * used by the visualization and logging during runtime.
   *
   * @return The named operator
   */
  def name(name: String) : WindowedDataStream[T] = javaStream match {
    case stream : DiscretizedStream[_] => javaStream.name(name)
    case _ => throw new
        UnsupportedOperationException("Only supported for windowing operators.")
    this
  }

  /**
   * Defines the slide size (trigger frequency) for the windowed data stream.
   * This controls how often the user defined function will be triggered on
   * the window.
   */
  def every(windowingHelper: WindowingHelper[_]): WindowedDataStream[T] =
    javaStream.every(windowingHelper)

  /**
   * Groups the elements of the WindowedDataStream using the given
   * field positions. The window sizes (evictions) and slide sizes
   * (triggers) will be calculated on the whole stream (in a global fashion),
   * but the user defined functions will be applied on a per group basis.
   * </br></br> To get windows and triggers on a per group basis apply the
   * DataStream.window(...) operator on an already grouped data stream.
   *
   */
  def groupBy(fields: Int*): WindowedDataStream[T] = javaStream.groupBy(fields: _*)

  /**
   * Groups the elements of the WindowedDataStream using the given
   * field expressions. The window sizes (evictions) and slide sizes
   * (triggers) will be calculated on the whole stream (in a global fashion),
   * but the user defined functions will be applied on a per group basis.
   * </br></br> To get windows and triggers on a per group basis apply the
   * DataStream.window(...) operator on an already grouped data stream.
   *
   */
  def groupBy(firstField: String, otherFields: String*): WindowedDataStream[T] =
   javaStream.groupBy(firstField +: otherFields.toArray: _*)   
    
  /**
   * Groups the elements of the WindowedDataStream using the given
   * KeySelector function. The window sizes (evictions) and slide sizes
   * (triggers) will be calculated on the whole stream (in a global fashion),
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
    javaStream.groupBy(keyExtractor)
  }
  
  /**
   * Sets the window discretisation local, meaning that windows will be
   * created in parallel at environment parallelism.
   * 
   */
  def local(): WindowedDataStream[T] = javaStream.local
 
  /**
   * Flattens the result of a window transformation returning the stream of window
   * contents elementwise.
   */
  def flatten(): DataStream[T] = javaStream.flatten()
  
  /**
   * Returns the stream of StreamWindows created by the window tranformation
   */
  def getDiscretizedStream(): DataStream[StreamWindow[T]] = javaStream.getDiscretizedStream()

  /**
   * Applies a reduce transformation on the windowed data stream by reducing
   * the current window at every trigger.
   *
   */
  def reduceWindow(reducer: ReduceFunction[T]): WindowedDataStream[T] = {
    if (reducer == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    javaStream.reduceWindow(reducer)
  }

  /**
   * Applies a reduce transformation on the windowed data stream by reducing
   * the current window at every trigger.
   *
   */
  def reduceWindow(fun: (T, T) => T): WindowedDataStream[T] = {
    if (fun == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    val reducer = new ReduceFunction[T] {
      val cleanFun = clean(fun)
      def reduce(v1: T, v2: T) = { cleanFun(v1, v2) }
    }
    reduceWindow(reducer)
  }

  /**
   * Applies a fold transformation on the windowed data stream by reducing
   * the current window at every trigger.
   *
   */
  def foldWindow[R: TypeInformation: ClassTag](initialValue: R, folder: FoldFunction[T,R]): 
  WindowedDataStream[R] = {
    if (folder == null) {
      throw new NullPointerException("Fold function must not be null.")
    }
    javaStream.foldWindow(initialValue, folder, implicitly[TypeInformation[R]])
  }

  /**
   * Applies a fold transformation on the windowed data stream by reducing
   * the current window at every trigger.
   *
   */
  def foldWindow[R: TypeInformation: ClassTag](initialValue: R)(fun: (R, T) => R): 
  WindowedDataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("Fold function must not be null.")
    }
    val folder = new FoldFunction[T,R] {
      val cleanFun = clean(fun)
      def fold(acc: R, v: T) = { cleanFun(acc, v) }
    }
    foldWindow(initialValue, folder)
  }

  /**
   * Applies a mapWindow transformation on the windowed data stream by calling the mapWindow
   * method on current window at every trigger. In contrast with the simple binary reduce 
   * operator, mapWindow exposes the whole window through the Iterable interface.
   * </br>
   * </br>
   * Whenever possible try to use reduceWindow instead of mapWindow for increased efficiency
   */
  def mapWindow[R: ClassTag: TypeInformation](reducer: WindowMapFunction[T, R]):
  WindowedDataStream[R] = {
    if (reducer == null) {
      throw new NullPointerException("GroupReduce function must not be null.")
    }
    javaStream.mapWindow(reducer, implicitly[TypeInformation[R]])
  }

  /**
   * Applies a mapWindow transformation on the windowed data stream by calling the mapWindow
   * method on current window at every trigger. In contrast with the simple binary reduce 
   * operator, mapWindow exposes the whole window through the Iterable interface.
   * </br>
   * </br>
   * Whenever possible try to use reduceWindow instead of mapWindow for increased efficiency
   */
  def mapWindow[R: ClassTag: TypeInformation](fun: (Iterable[T], Collector[R]) => Unit):
  WindowedDataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("GroupReduce function must not be null.")
    }
    val reducer = new WindowMapFunction[T, R] {
      val cleanFun = clean(fun)
      def mapWindow(in: java.lang.Iterable[T], out: Collector[R]) = { cleanFun(in, out) }
    }
    mapWindow(reducer)
  }

  /**
   * Applies an aggregation that that gives the maximum of the elements in the window at
   * the given position.
   *
   */
  def max(position: Int): WindowedDataStream[T] = aggregate(AggregationType.MAX, position)
  
  /**
   * Applies an aggregation that that gives the maximum of the elements in the window at
   * the given field.
   *
   */
  def max(field: String): WindowedDataStream[T] = aggregate(AggregationType.MAX, field)

  /**
   * Applies an aggregation that that gives the minimum of the elements in the window at
   * the given position.
   *
   */
  def min(position: Int): WindowedDataStream[T] = aggregate(AggregationType.MIN, position)
  
  /**
   * Applies an aggregation that that gives the minimum of the elements in the window at
   * the given field.
   *
   */
  def min(field: String): WindowedDataStream[T] = aggregate(AggregationType.MIN, field)

  /**
   * Applies an aggregation that sums the elements in the window at the given position.
   *
   */
  def sum(position: Int): WindowedDataStream[T] = aggregate(AggregationType.SUM, position)
  
  /**
   * Applies an aggregation that sums the elements in the window at the given field.
   *
   */
  def sum(field: String): WindowedDataStream[T] = aggregate(AggregationType.SUM, field)

  /**
   * Applies an aggregation that that gives the maximum element of the window by
   * the given position. When equality, returns the first.
   *
   */
  def maxBy(position: Int): WindowedDataStream[T] = aggregate(AggregationType.MAXBY,
    position)
    
  /**
   * Applies an aggregation that that gives the maximum element of the window by
   * the given field. When equality, returns the first.
   *
   */
  def maxBy(field: String): WindowedDataStream[T] = aggregate(AggregationType.MAXBY,
    field)

  /**
   * Applies an aggregation that that gives the minimum element of the window by
   * the given position. When equality, returns the first.
   *
   */
  def minBy(position: Int): WindowedDataStream[T] = aggregate(AggregationType.MINBY,
    position)
    
   /**
   * Applies an aggregation that that gives the minimum element of the window by
   * the given field. When equality, returns the first.
   *
   */
  def minBy(field: String): WindowedDataStream[T] = aggregate(AggregationType.MINBY,
    field)
    
  private def aggregate(aggregationType: AggregationType, field: String): 
  WindowedDataStream[T] = {
    val position = fieldNames2Indices(javaStream.getType(), Array(field))(0)
    aggregate(aggregationType, position)
  }  

  def aggregate(aggregationType: AggregationType, position: Int):
  WindowedDataStream[T] = {

    val jStream = javaStream.asInstanceOf[JavaWStream[Product]]
    val outType = jStream.getType().asInstanceOf[TupleTypeInfoBase[_]]

    val agg = new ScalaStreamingAggregator[Product](
      jStream.getType().createSerializer(javaStream.getExecutionConfig),
      position)

    val reducer = aggregationType match {
      case AggregationType.SUM => new agg.Sum(SumFunction.getForClass(
        outType.getTypeAt(position).getTypeClass()))
      case _ => new agg.ProductComparableAggregator(aggregationType, true)
    }

    new WindowedDataStream[Product](
            jStream.reduceWindow(reducer)).asInstanceOf[WindowedDataStream[T]]
  }

}
