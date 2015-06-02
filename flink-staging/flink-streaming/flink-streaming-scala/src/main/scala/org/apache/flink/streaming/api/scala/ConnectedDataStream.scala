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

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.{ConnectedDataStream => JavaCStream, DataStream => JavaStream}
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction, CoReduceFunction, CoWindowFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.clean
import org.apache.flink.util.Collector
import scala.collection.JavaConversions.asScalaBuffer
import scala.reflect.ClassTag
import org.apache.flink.streaming.api.operators.co.CoStreamFlatMap
import org.apache.flink.streaming.api.operators.co.CoStreamMap
import org.apache.flink.streaming.api.operators.co.CoStreamReduce

class ConnectedDataStream[IN1, IN2](javaStream: JavaCStream[IN1, IN2]) {

  /**
   * Applies a CoMap transformation on a {@link ConnectedDataStream} and maps
   * the output to a common type. The transformation calls a
   * @param fun1 for each element of the first input and
   * @param fun2 for each element of the second input. Each
   * CoMapFunction call returns exactly one element.
   *
   * The CoMapFunction used to jointly transform the two input
   * DataStreams
   * @return The transformed { @link DataStream}
   */
  def map[R: TypeInformation: ClassTag](fun1: IN1 => R, fun2: IN2 => R): 
  DataStream[R] = {
    if (fun1 == null || fun2 == null) {
      throw new NullPointerException("Map function must not be null.")
    }
    val comapper = new CoMapFunction[IN1, IN2, R] {
      def map1(in1: IN1): R = clean(fun1)(in1)
      def map2(in2: IN2): R = clean(fun2)(in2)
    }

    map(comapper)
  }

  /**
   * Applies a CoMap transformation on a {@link ConnectedDataStream} and maps
   * the output to a common type. The transformation calls a
   * {@link CoMapFunction#map1} for each element of the first input and
   * {@link CoMapFunction#map2} for each element of the second input. Each
   * CoMapFunction call returns exactly one element. The user can also extend
   * {@link RichCoMapFunction} to gain access to other features provided by
   * the {@link RichFuntion} interface.
   *
   * @param coMapper
   * The CoMapFunction used to jointly transform the two input
   * DataStreams
   * @return The transformed { @link DataStream}
   */
  def map[R: TypeInformation: ClassTag](coMapper: CoMapFunction[IN1, IN2, R]): 
  DataStream[R] = {
    if (coMapper == null) {
      throw new NullPointerException("Map function must not be null.")
    }

    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]    
    javaStream.map(coMapper).returns(outType).asInstanceOf[JavaStream[R]]
  }

  /**
   * Applies a CoFlatMap transformation on a {@link ConnectedDataStream} and
   * maps the output to a common type. The transformation calls a
   * {@link CoFlatMapFunction#flatMap1} for each element of the first input
   * and {@link CoFlatMapFunction#flatMap2} for each element of the second
   * input. Each CoFlatMapFunction call returns any number of elements
   * including none. The user can also extend {@link RichFlatMapFunction} to
   * gain access to other features provided by the {@link RichFuntion}
   * interface.
   *
   * @param coFlatMapper
   * The CoFlatMapFunction used to jointly transform the two input
   * DataStreams
   * @return The transformed { @link DataStream}
   */
  def flatMap[R: TypeInformation: ClassTag](coFlatMapper: CoFlatMapFunction[IN1, IN2, R]): 
  DataStream[R] = {
    if (coFlatMapper == null) {
      throw new NullPointerException("FlatMap function must not be null.")
    }
    
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]    
    javaStream.flatMap(coFlatMapper).returns(outType).asInstanceOf[JavaStream[R]]
  }

  /**
   * Applies a CoFlatMap transformation on a {@link ConnectedDataStream} and
   * maps the output to a common type. The transformation calls a
   * @param fun1 for each element of the first input
   * and @param fun2 for each element of the second
   * input. Each CoFlatMapFunction call returns any number of elements
   * including none.
   *
   * @return The transformed { @link DataStream}
   */
  def flatMap[R: TypeInformation: ClassTag](fun1: (IN1, Collector[R]) => Unit, 
      fun2: (IN2, Collector[R]) => Unit): DataStream[R] = {
    if (fun1 == null || fun2 == null) {
      throw new NullPointerException("FlatMap functions must not be null.")
    }
    val flatMapper = new CoFlatMapFunction[IN1, IN2, R] {
      def flatMap1(value: IN1, out: Collector[R]): Unit = clean(fun1)(value, out)
      def flatMap2(value: IN2, out: Collector[R]): Unit = clean(fun2)(value, out)
    }
    flatMap(flatMapper)
  }

  /**
   * Applies a CoFlatMap transformation on a {@link ConnectedDataStream} and
   * maps the output to a common type. The transformation calls a
   * @param fun1 for each element of the first input
   * and @param fun2 for each element of the second
   * input. Each CoFlatMapFunction call returns any number of elements
   * including none.
   *
   * @return The transformed { @link DataStream}
   */
  def flatMap[R: TypeInformation: ClassTag](fun1: IN1 => TraversableOnce[R],
      fun2: IN2 => TraversableOnce[R]): DataStream[R] = {
    if (fun1 == null || fun2 == null) {
      throw new NullPointerException("FlatMap functions must not be null.")
    }
    val flatMapper = new CoFlatMapFunction[IN1, IN2, R] {
      val cleanFun1 = clean(fun1)
      val cleanFun2 = clean(fun2)
      def flatMap1(value: IN1, out: Collector[R]) = { cleanFun1(value) foreach out.collect }
      def flatMap2(value: IN2, out: Collector[R]) = { cleanFun2(value) foreach out.collect }
    }
    flatMap(flatMapper)
  }

  /**
   * GroupBy operation for connected data stream. Groups the elements of
   * input1 and input2 according to keyPosition1 and keyPosition2. Used for
   * applying function on grouped data streams for example
   * {@link ConnectedDataStream#reduce}
   *
   * @param keyPosition1
   * The field used to compute the hashcode of the elements in the
   * first input stream.
   * @param keyPosition2
   * The field used to compute the hashcode of the elements in the
   * second input stream.
   * @return @return The transformed { @link ConnectedDataStream}
   */
  def groupBy(keyPosition1: Int, keyPosition2: Int): ConnectedDataStream[IN1, IN2] = {
    javaStream.groupBy(keyPosition1, keyPosition2)
  }

  /**
   * GroupBy operation for connected data stream. Groups the elements of
   * input1 and input2 according to keyPositions1 and keyPositions2. Used for
   * applying function on grouped data streams for example
   * {@link ConnectedDataStream#reduce}
   *
   * @param keyPositions1
   * The fields used to group the first input stream.
   * @param keyPositions2
   * The fields used to group the second input stream.
   * @return @return The transformed { @link ConnectedDataStream}
   */
  def groupBy(keyPositions1: Array[Int], keyPositions2: Array[Int]): 
  ConnectedDataStream[IN1, IN2] = {
    javaStream.groupBy(keyPositions1, keyPositions2)
  }

  /**
   * GroupBy operation for connected data stream using key expressions. Groups
   * the elements of input1 and input2 according to field1 and field2. A field
   * expression is either the name of a public field or a getter method with
   * parentheses of the {@link DataStream}S underlying type. A dot can be used
   * to drill down into objects, as in {@code "field1.getInnerField2()" }.
   *
   * @param field1
   * The grouping expression for the first input
   * @param field2
   * The grouping expression for the second input
   * @return The grouped { @link ConnectedDataStream}
   */
  def groupBy(field1: String, field2: String): ConnectedDataStream[IN1, IN2] = {
    javaStream.groupBy(field1, field2)
  }

  /**
   * GroupBy operation for connected data stream using key expressions. Groups
   * the elements of input1 and input2 according to fields1 and fields2. A
   * field expression is either the name of a public field or a getter method
   * with parentheses of the {@link DataStream}S underlying type. A dot can be
   * used to drill down into objects, as in {@code "field1.getInnerField2()" }
   * .
   *
   * @param fields1
   * The grouping expressions for the first input
   * @param fields2
   * The grouping expressions for the second input
   * @return The grouped { @link ConnectedDataStream}
   */
  def groupBy(fields1: Array[String], fields2: Array[String]): 
  ConnectedDataStream[IN1, IN2] = {
    javaStream.groupBy(fields1, fields2)
  }

  /**
   * GroupBy operation for connected data stream. Groups the elements of
   * input1 and input2 using fun1 and fun2. Used for applying
   * function on grouped data streams for example
   * {@link ConnectedDataStream#reduce}
   *
   * @param fun1
   * The function used for grouping the first input
   * @param fun2
   * The function used for grouping the second input
   * @return The grouped { @link ConnectedDataStream}
   */
  def groupBy[K: TypeInformation, L: TypeInformation](fun1: IN1 => K, fun2: IN2 => L):
  ConnectedDataStream[IN1, IN2] = {

    val keyExtractor1 = new KeySelector[IN1, K] {
      def getKey(in: IN1) = clean(fun1)(in)
    }
    val keyExtractor2 = new KeySelector[IN2, L] {
      def getKey(in: IN2) = clean(fun2)(in)
    }

    javaStream.groupBy(keyExtractor1, keyExtractor2)
  }

  /**
   * PartitionBy operation for connected data stream. Partitions the elements of
   * input1 and input2 according to keyPosition1 and keyPosition2.
   *
   * @param keyPosition1
   * The field used to compute the hashcode of the elements in the
   * first input stream.
   * @param keyPosition2
   * The field used to compute the hashcode of the elements in the
   * second input stream.
   * @return The transformed { @link ConnectedDataStream}
   */
  def partitionByHash(keyPosition1: Int, keyPosition2: Int): ConnectedDataStream[IN1, IN2] = {
    javaStream.partitionByHash(keyPosition1, keyPosition2)
  }

  /**
   * PartitionBy operation for connected data stream. Partitions the elements of
   * input1 and input2 according to keyPositions1 and keyPositions2.
   *
   * @param keyPositions1
   * The fields used to partition the first input stream.
   * @param keyPositions2
   * The fields used to partition the second input stream.
   * @return The transformed { @link ConnectedDataStream}
   */
  def partitionByHash(keyPositions1: Array[Int], keyPositions2: Array[Int]):
  ConnectedDataStream[IN1, IN2] = {
    javaStream.partitionByHash(keyPositions1, keyPositions2)
  }

  /**
   * PartitionBy operation for connected data stream using key expressions. Partitions
   * the elements of input1 and input2 according to field1 and field2. A field
   * expression is either the name of a public field or a getter method with
   * parentheses of the {@link DataStream}S underlying type. A dot can be used
   * to drill down into objects, as in {@code "field1.getInnerField2()" }.
   *
   * @param field1
   * The partitioning expression for the first input
   * @param field2
   * The partitioning expression for the second input
   * @return The grouped { @link ConnectedDataStream}
   */
  def partitionByHash(field1: String, field2: String): ConnectedDataStream[IN1, IN2] = {
    javaStream.partitionByHash(field1, field2)
  }

  /**
   * PartitionBy operation for connected data stream using key expressions. Partitions
   * the elements of input1 and input2 according to fields1 and fields2.
   *
   * @param fields1
   * The partitioning expressions for the first input
   * @param fields2
   * The partitioning expressions for the second input
   * @return The partitioned { @link ConnectedDataStream}
   */
  def partitionByHash(fields1: Array[String], fields2: Array[String]):
  ConnectedDataStream[IN1, IN2] = {
    javaStream.partitionByHash(fields1, fields2)
  }

  /**
   * PartitionBy operation for connected data stream. Partitions the elements of
   * input1 and input2 using fun1 and fun2.
   *
   * @param fun1
   * The function used for partitioning the first input
   * @param fun2
   * The function used for partitioning the second input
   * @return The partitioned { @link ConnectedDataStream}
   */
  def partitionByHash[K: TypeInformation, L: TypeInformation](fun1: IN1 => K, fun2: IN2 => L):
  ConnectedDataStream[IN1, IN2] = {

    val keyExtractor1 = new KeySelector[IN1, K] {
      def getKey(in: IN1) = clean(fun1)(in)
    }
    val keyExtractor2 = new KeySelector[IN2, L] {
      def getKey(in: IN2) = clean(fun2)(in)
    }

    javaStream.partitionByHash(keyExtractor1, keyExtractor2)
  }

  /**
   * Applies a reduce transformation on a {@link ConnectedDataStream} and maps
   * the outputs to a common type. If the {@link ConnectedDataStream} is
   * batched or windowed then the reduce transformation is applied on every
   * sliding batch/window of the data stream. If the connected data stream is
   * grouped then the reducer is applied on every group of elements sharing
   * the same key. This type of reduce is much faster than reduceGroup since
   * the reduce function can be applied incrementally.
   *
   * @param coReducer
   * The { @link CoReduceFunction} that will be called for every
   *             element of the inputs.
   * @return The transformed { @link DataStream}.
   */
  def reduce[R: TypeInformation: ClassTag](coReducer: CoReduceFunction[IN1, IN2, R]): 
  DataStream[R] = {
    if (coReducer == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]    
    javaStream.reduce(coReducer).returns(outType).asInstanceOf[JavaStream[R]]
  }

  /**
   * Applies a reduce transformation on a {@link ConnectedDataStream} and maps
   * the outputs to a common type. If the {@link ConnectedDataStream} is
   * batched or windowed then the reduce transformation is applied on every
   * sliding batch/window of the data stream. If the connected data stream is
   * grouped then the reducer is applied on every group of elements sharing
   * the same key. This type of reduce is much faster than reduceGroup since
   * the reduce function can be applied incrementally.
   *
   * @return The transformed { @link DataStream}.
   */
  def reduce[R: TypeInformation: ClassTag](reducer1: (IN1, IN1) => IN1,
      reducer2: (IN2, IN2) => IN2,mapper1: IN1 => R, mapper2: IN2 => R): DataStream[R] = {
    if (mapper1 == null || mapper2 == null) {
      throw new NullPointerException("Map functions must not be null.")
    }
    if (reducer1 == null || reducer2 == null) {
      throw new NullPointerException("Reduce functions must not be null.")
    }

    val reducer = new CoReduceFunction[IN1, IN2, R] {
      def reduce1(value1: IN1, value2: IN1): IN1 = clean(reducer1)(value1, value2)
      def map2(value: IN2): R = clean(mapper2)(value)
      def reduce2(value1: IN2, value2: IN2): IN2 = clean(reducer2)(value1, value2)
      def map1(value: IN1): R = clean(mapper1)(value)
    }
    reduce(reducer)
  }

  /**
   * Applies a CoWindow transformation on the connected DataStreams. The
   * transformation calls the {@link CoWindowFunction#coWindow} method for for
   * time aligned windows of the two data streams. System time is used as
   * default to compute windows.
   *
   * @param coWindowFunction
   * The { @link CoWindowFunction} that will be applied for the time
   *             windows.
   * @param windowSize
   * Size of the windows that will be aligned for both streams in
   * milliseconds.
   * @param slideInterval
   * After every function call the windows will be slid by this
   * interval.
   *
   * @return The transformed { @link DataStream}.
   */
  def windowReduce[R: TypeInformation: ClassTag](coWindowFunction: 
      CoWindowFunction[IN1, IN2, R], windowSize: Long, slideInterval: Long):
      DataStream[R] = {
    if (coWindowFunction == null) {
      throw new NullPointerException("CoWindow function must no be null")
    }
    
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]    
    
    javaStream.windowReduce(coWindowFunction, windowSize, slideInterval).
    returns(outType).asInstanceOf[JavaStream[R]]
  }

  /**
   * Applies a CoWindow transformation on the connected DataStreams. The
   * transformation calls the {@link CoWindowFunction#coWindow} method for for
   * time aligned windows of the two data streams. System time is used as
   * default to compute windows.
   *
   * @param coWindower
   * The coWindowing function to be applied for the time windows.
   * @param windowSize
   * Size of the windows that will be aligned for both streams in
   * milliseconds.
   * @param slideInterval
   * After every function call the windows will be slid by this
   * interval.
   *
   * @return The transformed { @link DataStream}.
   */
  def windowReduce[R: TypeInformation: ClassTag](coWindower: (Seq[IN1], Seq[IN2], 
      Collector[R]) => Unit, windowSize: Long, slideInterval: Long):
      DataStream[R] = {
    if (coWindower == null) {
      throw new NullPointerException("CoWindow function must no be null")
    }

    val coWindowFun = new CoWindowFunction[IN1, IN2, R] {
      def coWindow(first: util.List[IN1], second: util.List[IN2], 
          out: Collector[R]): Unit = clean(coWindower)(first, second, out)
    }

    windowReduce(coWindowFun, windowSize, slideInterval)
  }

  /**
   * Returns the first {@link DataStream}.
   *
   * @return The first DataStream.
   */
  def getFirst(): DataStream[IN1] = {
    javaStream.getFirst
  }

  /**
   * Returns the second {@link DataStream}.
   *
   * @return The second DataStream.
   */
  def getSecond(): DataStream[IN2] = {
    javaStream.getSecond
  }

  /**
   * Gets the type of the first input
   *
   * @return The type of the first input
   */
  def getInputType1(): TypeInformation[IN1] = {
    javaStream.getType1
  }

  /**
   * Gets the type of the second input
   *
   * @return The type of the second input
   */
  def getInputType2(): TypeInformation[IN2] = {
    javaStream.getType2
  }

}
