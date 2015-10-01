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

import org.apache.flink.streaming.api.datastream.{ KeyedStream => KeyedJavaStream, DataStream => JavaStream }
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator
import org.apache.flink.streaming.api.operators.StreamGroupedReduce
import scala.reflect.ClassTag
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.common.functions.ReduceFunction


class KeyedStream[T, K](javaStream: KeyedJavaStream[T, K]) extends DataStream[T](javaStream) {
 
  /**
   * Creates a new [[DataStream]] by reducing the elements of this DataStream
   * using an associative reduce function. An independent aggregate is kept per key.
   */
  def reduce(reducer: ReduceFunction[T]): DataStream[T] = {
    if (reducer == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
 
    javaStream.reduce(reducer)
  }

  /**
   * Creates a new [[DataStream]] by reducing the elements of this DataStream
   * using an associative reduce function. An independent aggregate is kept per key.
   */
  def reduce(fun: (T, T) => T): DataStream[T] = {
    if (fun == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    val cleanFun = clean(fun)
    val reducer = new ReduceFunction[T] {
      def reduce(v1: T, v2: T) = { cleanFun(v1, v2) }
    }
    reduce(reducer)
  }

  /**
   * Creates a new [[DataStream]] by folding the elements of this DataStream
   * using an associative fold function and an initial value. An independent 
   * aggregate is kept per key.
   */
  def fold[R: TypeInformation: ClassTag](initialValue: R, folder: FoldFunction[T,R]): 
  DataStream[R] = {
    if (folder == null) {
      throw new NullPointerException("Fold function must not be null.")
    }
    
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    
    javaStream.fold(initialValue, folder).
      returns(outType).asInstanceOf[JavaStream[R]]
  }

  /**
   * Creates a new [[DataStream]] by folding the elements of this DataStream
   * using an associative fold function and an initial value. An independent 
   * aggregate is kept per key.
   */
  def fold[R: TypeInformation: ClassTag](initialValue: R, fun: (R,T) => R): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("Fold function must not be null.")
    }
    val cleanFun = clean(fun)
    val folder = new FoldFunction[T,R] {
      def fold(acc: R, v: T) = {
        cleanFun(acc, v)
      }
    }
    fold(initialValue, folder)
  }
  
  /**
   * Applies an aggregation that that gives the current maximum of the data stream at
   * the given position by the given key. An independent aggregate is kept per key.
   *
   */
  def max(position: Int): DataStream[T] = aggregate(AggregationType.MAX, position)
  
  /**
   * Applies an aggregation that that gives the current maximum of the data stream at
   * the given field by the given key. An independent aggregate is kept per key.
   *
   */
  def max(field: String): DataStream[T] = aggregate(AggregationType.MAX, field)
  
  /**
   * Applies an aggregation that that gives the current minimum of the data stream at
   * the given position by the given key. An independent aggregate is kept per key.
   *
   */
  def min(position: Int): DataStream[T] = aggregate(AggregationType.MIN, position)
  
  /**
   * Applies an aggregation that that gives the current minimum of the data stream at
   * the given field by the given key. An independent aggregate is kept per key.
   *
   */
  def min(field: String): DataStream[T] = aggregate(AggregationType.MIN, field)

  /**
   * Applies an aggregation that sums the data stream at the given position by the given 
   * key. An independent aggregate is kept per key.
   *
   */
  def sum(position: Int): DataStream[T] = aggregate(AggregationType.SUM, position)
  
  /**
   * Applies an aggregation that sums the data stream at the given field by the given 
   * key. An independent aggregate is kept per key.
   *
   */
  def sum(field: String): DataStream[T] =  aggregate(AggregationType.SUM, field)

  /**
   * Applies an aggregation that that gives the current minimum element of the data stream by
   * the given position by the given key. An independent aggregate is kept per key. 
   * When equality, the first element is returned with the minimal value.
   *
   */
  def minBy(position: Int): DataStream[T] = aggregate(AggregationType
    .MINBY, position)
    
   /**
   * Applies an aggregation that that gives the current minimum element of the data stream by
   * the given field by the given key. An independent aggregate is kept per key.
   * When equality, the first element is returned with the minimal value.
   *
   */
  def minBy(field: String): DataStream[T] = aggregate(AggregationType
    .MINBY, field )

   /**
   * Applies an aggregation that that gives the current maximum element of the data stream by
   * the given position by the given key. An independent aggregate is kept per key. 
   * When equality, the first element is returned with the maximal value.
   *
   */
  def maxBy(position: Int): DataStream[T] =
    aggregate(AggregationType.MAXBY, position)
    
   /**
   * Applies an aggregation that that gives the current maximum element of the data stream by
   * the given field by the given key. An independent aggregate is kept per key. 
   * When equality, the first element is returned with the maximal value.
   *
   */
  def maxBy(field: String): DataStream[T] =
    aggregate(AggregationType.MAXBY, field)
    
  private def aggregate(aggregationType: AggregationType, field: String): DataStream[T] = {
    val position = fieldNames2Indices(javaStream.getType(), Array(field))(0)
    aggregate(aggregationType, position)
  }

  private def aggregate(aggregationType: AggregationType, position: Int): DataStream[T] = {

    val reducer = aggregationType match {
      case AggregationType.SUM =>
        new SumAggregator(position, javaStream.getType, javaStream.getExecutionConfig)
      case _ =>
        new ComparableAggregator(position, javaStream.getType, aggregationType, true,
          javaStream.getExecutionConfig)
    }

    val invokable =  new StreamGroupedReduce[T](reducer,javaStream.getKeySelector())
     
    new DataStream[T](javaStream.transform("aggregation", javaStream.getType(),invokable))
      .asInstanceOf[DataStream[T]]
  }
  
}
