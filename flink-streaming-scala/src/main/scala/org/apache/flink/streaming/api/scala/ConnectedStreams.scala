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

import org.apache.flink.annotation.{Internal, Public, PublicEvolving}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.{ConnectedStreams => JavaCStream, DataStream => JavaStream}
import org.apache.flink.streaming.api.functions.co._
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.util.Collector

/**
 * [[ConnectedStreams]] represents two connected streams of (possibly) different data types.
 * Connected streams are useful for cases where operations on one stream directly
 * affect the operations on the other stream, usually via shared state between the streams.
 * 
 * An example for the use of connected streams would be to apply rules that change over time
 * onto another stream. One of the connected streams has the rules, the other stream the
 * elements to apply the rules to. The operation on the connected stream maintains the 
 * current set of rules in the state. It may receive either a rule update and update the state
 * or a data element and apply the rules in the state to the element.
 * 
 * The connected stream can be conceptually viewed as a union stream of an Either type, that
 * holds either the first stream's type or the second stream's type.
 */
@Public
class ConnectedStreams[IN1, IN2](javaStream: JavaCStream[IN1, IN2]) {

  // ------------------------------------------------------
  //  Transformations
  // ------------------------------------------------------
  
  /**
   * Applies a CoMap transformation on the connected streams.
   * 
   * The transformation consists of two separate functions, where
   * the first one is called for each element of the first connected stream,
   * and the second one is called for each element of the second connected stream.
   * 
   * @param fun1 Function called per element of the first input.
   * @param fun2 Function called per element of the second input.
   * @return The resulting data stream.
   */
  def map[R: TypeInformation](fun1: IN1 => R, fun2: IN2 => R): 
      DataStream[R] = {
    
    if (fun1 == null || fun2 == null) {
      throw new NullPointerException("Map function must not be null.")
    }
    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)
    val comapper = new CoMapFunction[IN1, IN2, R] {
      def map1(in1: IN1): R = cleanFun1(in1)
      def map2(in2: IN2): R = cleanFun2(in2)
    }

    map(comapper)
  }

  /**
   * Applies a CoMap transformation on these connected streams.
   * 
   * The transformation calls [[CoMapFunction#map1]] for each element
   * in the first stream and [[CoMapFunction#map2]] for each element
   * of the second stream.
   * 
   * On can pass a subclass of [[org.apache.flink.streaming.api.functions.co.RichCoMapFunction]]
   * to gain access to the [[org.apache.flink.api.common.functions.RuntimeContext]]
   * and to additional life cycle methods.
   *
   * @param coMapper
   *         The CoMapFunction used to transform the two connected streams
   * @return
    *        The resulting data stream
   */
  def map[R: TypeInformation](coMapper: CoMapFunction[IN1, IN2, R]): DataStream[R] = {
    if (coMapper == null) {
      throw new NullPointerException("Map function must not be null.")
    }

    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]    
    asScalaStream(javaStream.map(coMapper, outType).asInstanceOf[JavaStream[R]])
  }

  /**
   * Applies the given [[CoProcessFunction]] on the connected input streams,
   * thereby creating a transformed output stream.
   *
   * The function will be called for every element in the input streams and can produce zero
   * or more output elements. Contrary to the [[flatMap(CoFlatMapFunction)]] function,
   * this function can also query the time and set timers. When reacting to the firing of set
   * timers the function can directly emit elements and/or register yet more timers.
   *
   * @param coProcessFunction The [[CoProcessFunction]] that is called for each element
    *                    in the stream.
   * @return The transformed [[DataStream]].
   */
  @PublicEvolving
  def process[R: TypeInformation](
      coProcessFunction: CoProcessFunction[IN1, IN2, R]) : DataStream[R] = {

    if (coProcessFunction == null) {
      throw new NullPointerException("CoProcessFunction function must not be null.")
    }

    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]

    asScalaStream(javaStream.process(coProcessFunction, outType))
  }

  /**
    * Applies the given [[KeyedCoProcessFunction]] on the connected input keyed streams,
    * thereby creating a transformed output stream.
    *
    * The function will be called for every element in the input keyed streams and can produce
    * zero or more output elements. Contrary to the [[flatMap(CoFlatMapFunction)]] function, this
    * function can also query the time and set timers. When reacting to the firing of set timers
    * the function can directly emit elements and/or register yet more timers.
    *
    * @param keyedCoProcessFunction The [[KeyedCoProcessFunction]] that is called for each element
    *                               in the stream.
    * @return The transformed [[DataStream]].
    */
  @PublicEvolving
  def process[K, R: TypeInformation](
      keyedCoProcessFunction: KeyedCoProcessFunction[K, IN1, IN2, R]) : DataStream[R] = {
    if (keyedCoProcessFunction == null) {
      throw new NullPointerException("KeyedCoProcessFunction function must not be null.")
    }

    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]

    asScalaStream(javaStream.process(keyedCoProcessFunction, outType))
  }


  /**
   * Applies a CoFlatMap transformation on these connected streams.
   *
   * The transformation calls [[CoFlatMapFunction#flatMap1]] for each element
   * in the first stream and [[CoFlatMapFunction#flatMap2]] for each element
   * of the second stream.
   *
   * On can pass a subclass of [[org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction]]
   * to gain access to the [[org.apache.flink.api.common.functions.RuntimeContext]]
   * and to additional life cycle methods.
   *
   * @param coFlatMapper
   *         The CoFlatMapFunction used to transform the two connected streams
   * @return
    *        The resulting data stream.
   */
  def flatMap[R: TypeInformation](coFlatMapper: CoFlatMapFunction[IN1, IN2, R]):
          DataStream[R] = {

    if (coFlatMapper == null) {
      throw new NullPointerException("FlatMap function must not be null.")
    }

    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    asScalaStream(javaStream.flatMap(coFlatMapper, outType).asInstanceOf[JavaStream[R]])
  }

  /**
   * Applies a CoFlatMap transformation on the connected streams.
   *
   * The transformation consists of two separate functions, where
   * the first one is called for each element of the first connected stream,
   * and the second one is called for each element of the second connected stream.
   *
   * @param fun1 Function called per element of the first input.
   * @param fun2 Function called per element of the second input.
   * @return The resulting data stream.
   */
  def flatMap[R: TypeInformation](
      fun1: (IN1, Collector[R]) => Unit, 
      fun2: (IN2, Collector[R]) => Unit): DataStream[R] = {
    
    if (fun1 == null || fun2 == null) {
      throw new NullPointerException("FlatMap functions must not be null.")
    }
    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)
    val flatMapper = new CoFlatMapFunction[IN1, IN2, R] {
      def flatMap1(value: IN1, out: Collector[R]): Unit = cleanFun1(value, out)
      def flatMap2(value: IN2, out: Collector[R]): Unit = cleanFun2(value, out)
    }
    flatMap(flatMapper)
  }

  /**
   * Applies a CoFlatMap transformation on the connected streams.
   *
   * The transformation consists of two separate functions, where
   * the first one is called for each element of the first connected stream,
   * and the second one is called for each element of the second connected stream.
   *
   * @param fun1 Function called per element of the first input.
   * @param fun2 Function called per element of the second input.
   * @return The resulting data stream.
   */
  def flatMap[R: TypeInformation](
      fun1: IN1 => TraversableOnce[R],
      fun2: IN2 => TraversableOnce[R]): DataStream[R] = {
    
    if (fun1 == null || fun2 == null) {
      throw new NullPointerException("FlatMap functions must not be null.")
    }
    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)
    
    val flatMapper = new CoFlatMapFunction[IN1, IN2, R] {
      def flatMap1(value: IN1, out: Collector[R]) = { cleanFun1(value) foreach out.collect }
      def flatMap2(value: IN2, out: Collector[R]) = { cleanFun2(value) foreach out.collect }
    }
    
    flatMap(flatMapper)
  }

  // ------------------------------------------------------
  //  grouping and partitioning
  // ------------------------------------------------------
  
  /**
   * Keys the two connected streams together. After this operation, all
   * elements with the same key from both streams will be sent to the
   * same parallel instance of the transformation functions.
   *
   * @param keyPosition1 The first stream's key field
   * @param keyPosition2 The second stream's key field
   * @return The key-grouped connected streams
   */
  def keyBy(keyPosition1: Int, keyPosition2: Int): ConnectedStreams[IN1, IN2] = {
    asScalaStream(javaStream.keyBy(keyPosition1, keyPosition2))
  }

  /**
   * Keys the two connected streams together. After this operation, all
   * elements with the same key from both streams will be sent to the
   * same parallel instance of the transformation functions.
   *
   * @param keyPositions1 The first stream's key fields
   * @param keyPositions2 The second stream's key fields
   * @return The key-grouped connected streams
   */
  def keyBy(keyPositions1: Array[Int], keyPositions2: Array[Int]): ConnectedStreams[IN1, IN2] = {
    asScalaStream(javaStream.keyBy(keyPositions1, keyPositions2))
  }

  /**
   * Keys the two connected streams together. After this operation, all
   * elements with the same key from both streams will be sent to the
   * same parallel instance of the transformation functions.
   *
   * @param field1 The first stream's key expression
   * @param field2 The second stream's key expression
   * @return The key-grouped connected streams
   */
  def keyBy(field1: String, field2: String): ConnectedStreams[IN1, IN2] = {
    asScalaStream(javaStream.keyBy(field1, field2))
  }

  /**
   * Keys the two connected streams together. After this operation, all
   * elements with the same key from both streams will be sent to the
   * same parallel instance of the transformation functions.
   *
   * @param fields1 The first stream's key expressions
   * @param fields2 The second stream's key expressions
   * @return The key-grouped connected streams
   */
  def keyBy(fields1: Array[String], fields2: Array[String]): ConnectedStreams[IN1, IN2] = {
    asScalaStream(javaStream.keyBy(fields1, fields2))
  }

  /**
   * Keys the two connected streams together. After this operation, all
   * elements with the same key from both streams will be sent to the
   * same parallel instance of the transformation functions.
   *
   * @param fun1 The first stream's key function
   * @param fun2 The second stream's key function
   * @return The key-grouped connected streams
   */
  def keyBy[KEY: TypeInformation](fun1: IN1 => KEY, fun2: IN2 => KEY):
      ConnectedStreams[IN1, IN2] = {

    val keyType = implicitly[TypeInformation[KEY]]

    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)

    val keyExtractor1 = new JavaKeySelector[IN1, KEY](cleanFun1)
    val keyExtractor2 = new JavaKeySelector[IN2, KEY](cleanFun2)

    asScalaStream(javaStream.keyBy(keyExtractor1, keyExtractor2, keyType))
  }

  /**
   * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
   * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]]
   */
  private[flink] def clean[F <: AnyRef](f: F): F = {
    new StreamExecutionEnvironment(javaStream.getExecutionEnvironment).scalaClean(f)
  }

  @PublicEvolving
  def transform[R: TypeInformation](
      functionName: String,
      operator: TwoInputStreamOperator[IN1, IN2, R]): DataStream[R] = {
    asScalaStream(javaStream.transform(functionName, implicitly[TypeInformation[R]], operator))
  }
}

@Internal
class JavaKeySelector[IN, K](private[this] val fun: IN => K) extends KeySelector[IN, K] {
  override def getKey(value: IN): K = fun(value)
}
