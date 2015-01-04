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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream,
  SingleOutputStreamOperator, GroupedDataStream}
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.reflect.ClassTag
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.invokable.operator.MapInvokable
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.invokable.operator.FlatMapInvokable
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.invokable.StreamInvokable
import org.apache.flink.streaming.api.invokable.operator.{ GroupedReduceInvokable, StreamReduceInvokable }
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.function.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.clean
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper
import org.apache.flink.streaming.api.windowing.policy.{ EvictionPolicy, TriggerPolicy }
import org.apache.flink.streaming.api.collector.OutputSelector
import scala.collection.JavaConversions._
import java.util.HashMap
import org.apache.flink.streaming.api.function.aggregation.SumFunction
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction.AggregationType
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala.StreamingConversions._
import org.apache.flink.api.streaming.scala.ScalaStreamingAggregator

class DataStream[T](javaStream: JavaStream[T]) {

  /**
   * Gets the underlying java DataStream object.
   */
  def getJavaStream: JavaStream[T] = javaStream

  /**
   * Sets the degree of parallelism of this operation. This must be greater than 1.
   */
  def setParallelism(dop: Int): DataStream[T] = {
    javaStream match {
      case ds: SingleOutputStreamOperator[_, _] => ds.setParallelism(dop)
      case _ =>
        throw new UnsupportedOperationException("Operator " + javaStream.toString +  " cannot " +
          "have " +
          "parallelism.")
    }
    this
  }

  /**
   * Returns the degree of parallelism of this operation.
   */
  def getParallelism: Int = javaStream match {
    case op: SingleOutputStreamOperator[_, _] => op.getParallelism
    case _ =>
      throw new UnsupportedOperationException("Operator " + javaStream.toString + " does not have" +
        " "  +
        "parallelism.")
  }

  /**
   * Creates a new DataStream by merging DataStream outputs of
   * the same type with each other. The DataStreams merged using this operator
   * will be transformed simultaneously.
   *
   */
  def merge(dataStreams: DataStream[T]*): DataStream[T] =
    javaStream.merge(dataStreams.map(_.getJavaStream): _*)

  /**
   * Creates a new ConnectedDataStream by connecting
   * DataStream outputs of different type with each other. The
   * DataStreams connected using this operators can be used with CoFunctions.
   *
   */
  def connect[T2](dataStream: DataStream[T2]): ConnectedDataStream[T, T2] = 
    javaStream.connect(dataStream.getJavaStream)

  /**
   * Groups the elements of a DataStream by the given key positions (for tuple/array types) to
   * be used with grouped operators like grouped reduce or grouped aggregations
   *
   */
  def groupBy(fields: Int*): DataStream[T] = javaStream.groupBy(fields: _*)

  /**
   * Groups the elements of a DataStream by the given field expressions to
   * be used with grouped operators like grouped reduce or grouped aggregations
   *
   */
  def groupBy(firstField: String, otherFields: String*): DataStream[T] = 
   javaStream.groupBy(firstField +: otherFields.toArray: _*)   
  
  /**
   * Groups the elements of a DataStream by the given K key to
   * be used with grouped operators like grouped reduce or grouped aggregations
   *
   */
  def groupBy[K: TypeInformation](fun: T => K): DataStream[T] = {

    val keyExtractor = new KeySelector[T, K] {
      val cleanFun = clean(fun)
      def getKey(in: T) = cleanFun(in)
    }
    javaStream.groupBy(keyExtractor)
  }

  /**
   * Sets the partitioning of the DataStream so that the output is
   * partitioned by the selected fields. This setting only effects the how the outputs will be
   * distributed between the parallel instances of the next processing operator.
   *
   */
  def partitionBy(fields: Int*): DataStream[T] =
    javaStream.partitionBy(fields: _*)

  /**
   * Sets the partitioning of the DataStream so that the output is
   * partitioned by the selected fields. This setting only effects the how the outputs will be
   * distributed between the parallel instances of the next processing operator.
   *
   */
  def partitionBy(firstField: String, otherFields: String*): DataStream[T] =
   javaStream.partitionBy(firstField +: otherFields.toArray: _*)

  /**
   * Sets the partitioning of the DataStream so that the output is
   * partitioned by the given Key. This setting only effects the how the outputs will be
   * distributed between the parallel instances of the next processing operator.
   *
   */
  def partitionBy[K: TypeInformation](fun: T => K): DataStream[T] = {

    val keyExtractor = new KeySelector[T, K] {
      val cleanFun = clean(fun)
      def getKey(in: T) = cleanFun(in)
    }
    javaStream.partitionBy(keyExtractor)
  }

  /**
   * Sets the partitioning of the DataStream so that the output tuples
   * are broadcasted to every parallel instance of the next component. This
   * setting only effects the how the outputs will be distributed between the
   * parallel instances of the next processing operator.
   *
   */
  def broadcast: DataStream[T] = javaStream.broadcast()

  /**
   * Sets the partitioning of the DataStream so that the output tuples
   * are shuffled to the next component. This setting only effects the how the
   * outputs will be distributed between the parallel instances of the next
   * processing operator.
   *
   */
  def shuffle: DataStream[T] = javaStream.shuffle()

  /**
   * Sets the partitioning of the DataStream so that the output tuples
   * are forwarded to the local subtask of the next component (whenever
   * possible). This is the default partitioner setting. This setting only
   * effects the how the outputs will be distributed between the parallel
   * instances of the next processing operator.
   *
   */
  def forward: DataStream[T] = javaStream.forward()

  /**
   * Sets the partitioning of the DataStream so that the output tuples
   * are distributed evenly to the next component.This setting only effects
   * the how the outputs will be distributed between the parallel instances of
   * the next processing operator.
   *
   */
  def distribute: DataStream[T] = javaStream.distribute()

  /**
   * Initiates an iterative part of the program that creates a loop by feeding
   * back data streams. To create a streaming iteration the user needs to define
   * a transformation that creates two DataStreams.The first one one is the output
   * that will be fed back to the start of the iteration and the second is the output
   * stream of the iterative part.
   * <p>
   * stepfunction: initialStream => (feedback, output)
   * <p>
   * A common pattern is to use output splitting to create feedback and output DataStream.
   * Please refer to the .split(...) method of the DataStream
   * <p>
   * By default a DataStream with iteration will never terminate, but the user
   * can use the maxWaitTime parameter to set a max waiting time for the iteration head.
   * If no data received in the set time the stream terminates.
   *
   *
   */
  def iterate(stepFunction: DataStream[T] => (DataStream[T], DataStream[T]),  maxWaitTimeMillis:
    Long = 0): DataStream[T] = {
    val iterativeStream = javaStream.iterate(maxWaitTimeMillis)

    val (feedback, output) = stepFunction(new DataStream[T](iterativeStream))
    iterativeStream.closeWith(feedback.getJavaStream)
    output
  }

  /**
   * Applies an aggregation that that gives the current maximum of the data stream at
   * the given position.
   *
   */
  def max(position: Int): DataStream[T] = aggregate(AggregationType.MAX, position)

  /**
   * Applies an aggregation that that gives the current minimum of the data stream at
   * the given position.
   *
   */
  def min(position: Int): DataStream[T] = aggregate(AggregationType.MIN, position)

  /**
   * Applies an aggregation that sums the data stream at the given position.
   *
   */
  def sum(position: Int): DataStream[T] = aggregate(AggregationType.SUM, position)

  /**
   * Applies an aggregation that that gives the current minimum element of the data stream by
   * the given position. When equality, the user can set to get the first or last element with
   * the minimal value.
   *
   */
  def minBy(position: Int, first: Boolean = true): DataStream[T] = aggregate(AggregationType
    .MINBY, position, first)

  /**
   * Applies an aggregation that that gives the current maximum element of the data stream by
   * the given position. When equality, the user can set to get the first or last element with
   * the maximal value.
   *
   */
  def maxBy(position: Int, first: Boolean = true): DataStream[T] =
    aggregate(AggregationType.MAXBY, position, first)

  private def aggregate(aggregationType: AggregationType, position: Int, first: Boolean = true):
    DataStream[T] = {

    val jStream = javaStream.asInstanceOf[JavaStream[Product]]
    val outType = jStream.getType().asInstanceOf[TupleTypeInfoBase[_]]

    val agg = new ScalaStreamingAggregator[Product](jStream.getType().createSerializer(), position)

    val reducer = aggregationType match {
      case AggregationType.SUM => new agg.Sum(SumFunction.getForClass(outType.getTypeAt(position).
        getTypeClass()));
      case _ => new agg.ProductComparableAggregator(aggregationType, first)
    }

    val invokable = jStream match {
      case groupedStream: GroupedDataStream[_] => new GroupedReduceInvokable(reducer,
        groupedStream.getKeySelector())
      case _ => new StreamReduceInvokable(reducer)
    }
    new DataStream[Product](jStream.transform("aggregation", jStream.getType(),
      invokable)).asInstanceOf[DataStream[T]]
  }

  /**
   * Creates a new DataStream containing the current number (count) of
   * received records.
   *
   */
  def count: DataStream[Long] = new DataStream[java.lang.Long](
    javaStream.count()).asInstanceOf[DataStream[Long]]

  /**
   * Creates a new DataStream by applying the given function to every element of this DataStream.
   */
  def map[R: TypeInformation: ClassTag](fun: T => R): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("Map function must not be null.")
    }
    val mapper = new MapFunction[T, R] {
      val cleanFun = clean(fun)
      def map(in: T): R = cleanFun(in)
    }

    javaStream.transform("map", implicitly[TypeInformation[R]], new MapInvokable[T, R](mapper))
  }

  /**
   * Creates a new DataStream by applying the given function to every element of this DataStream.
   */
  def map[R: TypeInformation: ClassTag](mapper: MapFunction[T, R]): DataStream[R] = {
    if (mapper == null) {
      throw new NullPointerException("Map function must not be null.")
    }

    javaStream.transform("map", implicitly[TypeInformation[R]], new MapInvokable[T, R](mapper))
  }

  /**
   * Creates a new DataStream by applying the given function to every element and flattening
   * the results.
   */
  def flatMap[R: TypeInformation: ClassTag](flatMapper: FlatMapFunction[T, R]): DataStream[R] = {
    if (flatMapper == null) {
      throw new NullPointerException("FlatMap function must not be null.")
    }
   javaStream.transform("flatMap", implicitly[TypeInformation[R]], 
       new FlatMapInvokable[T, R](flatMapper))
  }

  /**
   * Creates a new DataStream by applying the given function to every element and flattening
   * the results.
   */
  def flatMap[R: TypeInformation: ClassTag](fun: (T, Collector[R]) => Unit): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("FlatMap function must not be null.")
    }
    val flatMapper = new FlatMapFunction[T, R] {
      val cleanFun = clean(fun)
      def flatMap(in: T, out: Collector[R]) { cleanFun(in, out) }
    }
    flatMap(flatMapper)
  }

  /**
   * Creates a new DataStream by applying the given function to every element and flattening
   * the results.
   */
  def flatMap[R: TypeInformation: ClassTag](fun: T => TraversableOnce[R]): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("FlatMap function must not be null.")
    }
    val flatMapper = new FlatMapFunction[T, R] {
      val cleanFun = clean(fun)
      def flatMap(in: T, out: Collector[R]) { cleanFun(in) foreach out.collect }
    }
    flatMap(flatMapper)
  }

  /**
   * Creates a new [[DataStream]] by reducing the elements of this DataStream
   * using an associative reduce function.
   */
  def reduce(reducer: ReduceFunction[T]): DataStream[T] = {
    if (reducer == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    javaStream match {
      case ds: GroupedDataStream[_] => javaStream.transform("reduce",
        javaStream.getType(), new GroupedReduceInvokable[T](reducer, ds.getKeySelector()))
      case _ => javaStream.transform("reduce", javaStream.getType(),
        new StreamReduceInvokable[T](reducer))
    }
  }

  /**
   * Creates a new [[DataStream]] by reducing the elements of this DataStream
   * using an associative reduce function.
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
   * Creates a new DataStream that contains only the elements satisfying the given filter predicate.
   */
  def filter(filter: FilterFunction[T]): DataStream[T] = {
    if (filter == null) {
      throw new NullPointerException("Filter function must not be null.")
    }
    javaStream.filter(filter)
  }

  /**
   * Creates a new DataStream that contains only the elements satisfying the given filter predicate.
   */
  def filter(fun: T => Boolean): DataStream[T] = {
    if (fun == null) {
      throw new NullPointerException("Filter function must not be null.")
    }
    val filter = new FilterFunction[T] {
      val cleanFun = clean(fun)
      def filter(in: T) = cleanFun(in)
    }
    this.filter(filter)
  }

  /**
   * Create a WindowedDataStream that can be used to apply
   * transformation like .reduce(...) or aggregations on
   * preset chunks(windows) of the data stream. To define the windows one or
   * more WindowingHelper-s such as Time, Count and
   * Delta can be used.</br></br> When applied to a grouped data
   * stream, the windows (evictions) and slide sizes (triggers) will be
   * computed on a per group basis. </br></br> For more advanced control over
   * the trigger and eviction policies please use to
   * window(List(triggers), List(evicters))
   */
  def window(windowingHelper: WindowingHelper[_]*): WindowedDataStream[T] =
    javaStream.window(windowingHelper: _*)

  /**
   * Create a WindowedDataStream using the given TriggerPolicy-s and EvictionPolicy-s.
   * Windowing can be used to apply transformation like .reduce(...) or aggregations on
   * preset chunks(windows) of the data stream.</br></br>For most common
   * use-cases please refer to window(WindowingHelper[_]*)
   *
   */
  def window(triggers: List[TriggerPolicy[T]], evicters: List[EvictionPolicy[T]]):
    WindowedDataStream[T] = javaStream.window(triggers, evicters)

  /**
   *
   * Operator used for directing tuples to specific named outputs using an
   * OutputSelector. Calling this method on an operator creates a new
   * SplitDataStream.
   */
  def split(selector: OutputSelector[T]): SplitDataStream[T] = javaStream match {
    case op: SingleOutputStreamOperator[_, _] => op.split(selector)
    case _ =>
      throw new UnsupportedOperationException("Operator " + javaStream.toString + " can not be " +
        "split.")
  }

  /**
   * Creates a new SplitDataStream that contains only the elements satisfying the
   *  given output selector predicate.
   */
  def split(fun: T => String): SplitDataStream[T] = {
    if (fun == null) {
      throw new NullPointerException("OutputSelector must not be null.")
    }
    val selector = new OutputSelector[T] {
      val cleanFun = clean(fun)
      def select(in: T): java.lang.Iterable[String] = {
        List(cleanFun(in))
      }
    }
    split(selector)
  }

  /**
   * Initiates a temporal Join transformation that joins the elements of two
   * data streams on key equality over a specified time window.
   *
   * This method returns a StreamJoinOperator on which the
   * .onWindow(..) should be called to define the
   * window, and then the .where(..) and .equalTo(..) methods can be used to defin
   * the join keys.</p> The user can also use the apply method of the returned JoinedStream
   * to use custom join function.
   *
   */
  def join[R](stream: DataStream[R]): StreamJoinOperator[T, R] =
    new StreamJoinOperator[T, R](javaStream, stream.getJavaStream)

  /**
   * Initiates a temporal cross transformation that builds all pair
   * combinations of elements of both DataStreams, i.e., it builds a Cartesian
   * product.
   *
   * This method returns a StreamJoinOperator on which the
   * .onWindow(..) should be called to define the
   * window, and then the .where(..) and .equalTo(..) methods can be used to defin
   * the join keys.</p> The user can also use the apply method of the returned JoinedStream
   * to use custom join function.
   *
   */
  def cross[R](stream: DataStream[R]): StreamCrossOperator[T, R] =
    new StreamCrossOperator[T, R](javaStream, stream.getJavaStream)

  /**
   * Writes a DataStream to the standard output stream (stdout). For each
   * element of the DataStream the result of .toString is
   * written.
   *
   */
  def print(): DataStream[T] = javaStream.print()

  /**
   * Writes a DataStream to the file specified by path in text format. The
   * writing is performed periodically, in every millis milliseconds. For
   * every element of the DataStream the result of .toString
   * is written.
   *
   */
  def writeAsText(path: String, millis: Long = 0): DataStream[T] =
    javaStream.writeAsText(path, millis)

  /**
   * Writes a DataStream to the file specified by path in text format. The
   * writing is performed periodically, in every millis milliseconds. For
   * every element of the DataStream the result of .toString
   * is written.
   *
   */
  def writeAsCsv(path: String, millis: Long = 0): DataStream[T] =
    javaStream.writeAsCsv(path, millis)

  /**
   * Adds the given sink to this DataStream. Only streams with sinks added
   * will be executed once the StreamExecutionEnvironment.execute(...)
   * method is called.
   *
   */
  def addSink(sinkFuntion: SinkFunction[T]): DataStream[T] =
    javaStream.addSink(sinkFuntion)

  /**
   * Adds the given sink to this DataStream. Only streams with sinks added
   * will be executed once the StreamExecutionEnvironment.execute(...)
   * method is called.
   *
   */
  def addSink(fun: T => Unit): DataStream[T] = {
    if (fun == null) {
      throw new NullPointerException("Sink function must not be null.")
    }
    val sinkFunction = new SinkFunction[T] {
      val cleanFun = clean(fun)
      def invoke(in: T) = cleanFun(in)
    }
    this.addSink(sinkFunction)
  }

}
