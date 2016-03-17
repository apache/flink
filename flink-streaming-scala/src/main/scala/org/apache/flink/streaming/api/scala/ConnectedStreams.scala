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

import org.apache.flink.annotation.Public
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{ConnectedStreams => JavaCStream, DataStream => JavaStream}
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
 * [[ConnectedStreams]] represents two connected streams of (possible) different data types. It
 * can be used to apply transformations such as [[CoMapFunction]] on two
 * [[DataStream]]s.
 */
@Public
class ConnectedStreams[IN1, IN2](javaStream: JavaCStream[IN1, IN2]) {

  /**
   * Applies a CoMap transformation on a {@link ConnectedStreams} and maps
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
    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)
    val comapper = new CoMapFunction[IN1, IN2, R] {
      def map1(in1: IN1): R = cleanFun1(in1)
      def map2(in2: IN2): R = cleanFun2(in2)
    }

    map(comapper)
  }

  /**
   * Applies a CoMap transformation on a {@link ConnectedStreams} and maps
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
   * Applies a CoFlatMap transformation on a {@link ConnectedStreams} and
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
   * Applies a CoFlatMap transformation on a {@link ConnectedStreams} and
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
    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)
    val flatMapper = new CoFlatMapFunction[IN1, IN2, R] {
      def flatMap1(value: IN1, out: Collector[R]): Unit = cleanFun1(value, out)
      def flatMap2(value: IN2, out: Collector[R]): Unit = cleanFun2(value, out)
    }
    flatMap(flatMapper)
  }

  /**
   * Applies a CoFlatMap transformation on a {@link ConnectedStreams} and
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
    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)
    val flatMapper = new CoFlatMapFunction[IN1, IN2, R] {
      def flatMap1(value: IN1, out: Collector[R]) = { cleanFun1(value) foreach out.collect }
      def flatMap2(value: IN2, out: Collector[R]) = { cleanFun2(value) foreach out.collect }
    }
    flatMap(flatMapper)
  }

  /**
   * GroupBy operation for connected data stream. Groups the elements of
   * input1 and input2 according to keyPosition1 and keyPosition2. Used for
   * applying function on grouped data streams for example
   * {@link ConnectedStreams#reduce}
   *
   * @param keyPosition1
   * The field used to compute the hashcode of the elements in the
   * first input stream.
   * @param keyPosition2
   * The field used to compute the hashcode of the elements in the
   * second input stream.
   * @return @return The transformed { @link ConnectedStreams}
   */
  def keyBy(keyPosition1: Int, keyPosition2: Int): ConnectedStreams[IN1, IN2] = {
    javaStream.keyBy(keyPosition1, keyPosition2)
  }

  /**
   * GroupBy operation for connected data stream. Groups the elements of
   * input1 and input2 according to keyPositions1 and keyPositions2. Used for
   * applying function on grouped data streams for example
   * {@link ConnectedStreams#reduce}
   *
   * @param keyPositions1
   * The fields used to group the first input stream.
   * @param keyPositions2
   * The fields used to group the second input stream.
   * @return @return The transformed { @link ConnectedStreams}
   */
  def keyBy(keyPositions1: Array[Int], keyPositions2: Array[Int]):
  ConnectedStreams[IN1, IN2] = {
    javaStream.keyBy(keyPositions1, keyPositions2)
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
   * @return The grouped { @link ConnectedStreams}
   */
  def keyBy(field1: String, field2: String): ConnectedStreams[IN1, IN2] = {
    javaStream.keyBy(field1, field2)
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
   * @return The grouped { @link ConnectedStreams}
   */
  def keyBy(fields1: Array[String], fields2: Array[String]):
  ConnectedStreams[IN1, IN2] = {
    javaStream.keyBy(fields1, fields2)
  }

  /**
   * GroupBy operation for connected data stream. Groups the elements of
   * input1 and input2 using fun1 and fun2. Used for applying
   * function on grouped data streams for example
   * {@link ConnectedStreams#reduce}
   *
   * @param fun1
   * The function used for grouping the first input
   * @param fun2
   * The function used for grouping the second input
   * @return The grouped { @link ConnectedStreams}
   */
  def keyBy[K1: TypeInformation, K2: TypeInformation](fun1: IN1 => K1, fun2: IN2 => K2):
  ConnectedStreams[IN1, IN2] = {

    val keyType1 = implicitly[TypeInformation[K1]]
    val keyType2 = implicitly[TypeInformation[K2]]
    
    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)
    
    val keyExtractor1 = new KeySelectorWithType[IN1, K1](cleanFun1, keyType1)
    val keyExtractor2 = new KeySelectorWithType[IN2, K2](cleanFun2, keyType2)
    
    javaStream.keyBy(keyExtractor1, keyExtractor2)
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
   * @return The transformed { @link ConnectedStreams}
   */
  def partitionByHash(keyPosition1: Int, keyPosition2: Int): ConnectedStreams[IN1, IN2] = {
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
   * @return The transformed { @link ConnectedStreams}
   */
  def partitionByHash(keyPositions1: Array[Int], keyPositions2: Array[Int]):
  ConnectedStreams[IN1, IN2] = {
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
   * @return The grouped { @link ConnectedStreams}
   */
  def partitionByHash(field1: String, field2: String): ConnectedStreams[IN1, IN2] = {
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
   * @return The partitioned { @link ConnectedStreams}
   */
  def partitionByHash(fields1: Array[String], fields2: Array[String]):
  ConnectedStreams[IN1, IN2] = {
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
   * @return The partitioned { @link ConnectedStreams}
   */
  def partitionByHash[K: TypeInformation, L: TypeInformation](fun1: IN1 => K, fun2: IN2 => L):
  ConnectedStreams[IN1, IN2] = {

    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)

    val keyExtractor1 = new KeySelector[IN1, K] {
      def getKey(in: IN1) = cleanFun1(in)
    }
    val keyExtractor2 = new KeySelector[IN2, L] {
      def getKey(in: IN2) = cleanFun2(in)
    }

    javaStream.partitionByHash(keyExtractor1, keyExtractor2)
  }

  /**
   * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
   * is not disabled in the {@link org.apache.flink.api.common.ExecutionConfig}
   */
  private[flink] def clean[F <: AnyRef](f: F): F = {
    new StreamExecutionEnvironment(javaStream.getExecutionEnvironment).scalaClean(f)
  }

}

class KeySelectorWithType[IN, K](
        private[this] val fun: IN => K,
        private[this] val info: TypeInformation[K])
  extends KeySelector[IN, K] with ResultTypeQueryable[K] {
  
  override def getKey(value: IN): K = fun(value)

  override def getProducedType: TypeInformation[K] = info
}
