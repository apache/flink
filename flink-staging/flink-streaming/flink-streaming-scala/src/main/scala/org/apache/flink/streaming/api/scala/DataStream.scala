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

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala.operators.ScalaCsvOutputFormat
import org.apache.flink.core.fs.{FileSystem, Path}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, FoldFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.streaming.scala.ScalaStreamingAggregator
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream, DataStreamSink, GroupedDataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType
import org.apache.flink.streaming.api.functions.aggregation.SumFunction
import org.apache.flink.streaming.api.functions.sink.{FileSinkFunctionByMillis, SinkFunction}
import org.apache.flink.streaming.api.operators.{StreamGroupedReduce, StreamReduce}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.clean
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper
import org.apache.flink.streaming.api.windowing.policy.{EvictionPolicy, TriggerPolicy}
import org.apache.flink.streaming.util.serialization.SerializationSchema
import org.apache.flink.util.Collector

class DataStream[T](javaStream: JavaStream[T]) {

  /**
   * Gets the underlying java DataStream object.
   */
  def getJavaStream: JavaStream[T] = javaStream

  /**
   * Returns the ID of the DataStream.
   *
   * @return ID of the DataStream
   */
  def getId = javaStream.getId

  /**
   * Returns the TypeInformation for the elements of this DataStream.
   */
  def getType(): TypeInformation[T] = javaStream.getType()

  /**
   * Sets the parallelism of this operation. This must be at least 1.
   */
  def setParallelism(parallelism: Int): DataStream[T] = {
    javaStream match {
      case ds: SingleOutputStreamOperator[_, _] => ds.setParallelism(parallelism)
      case _ =>
        throw new UnsupportedOperationException("Operator " + javaStream.toString +  " cannot " +
          "have " +
          "parallelism.")
    }
    this
  }

  /**
   * Returns the parallelism of this operation.
   */
  def getParallelism = javaStream.getParallelism

  /**
   * Gets the name of the current data stream. This name is
   * used by the visualization and logging during runtime.
   *
   * @return Name of the stream.
   */
  def getName : String = javaStream match {
    case stream : SingleOutputStreamOperator[T,_] => stream.getName
    case _ => throw new
        UnsupportedOperationException("Only supported for operators.")
  }

  /**
   * Sets the name of the current data stream. This name is
   * used by the visualization and logging during runtime.
   *
   * @return The named operator
   */
  def name(name: String) : DataStream[T] = javaStream match {
    case stream : SingleOutputStreamOperator[T,_] => stream.name(name)
    case _ => throw new
        UnsupportedOperationException("Only supported for operators.")
    this
  }
  
  /**
   * Turns off chaining for this operator so thread co-location will not be
   * used as an optimization. </p> Chaining can be turned off for the whole
   * job by [[StreamExecutionEnvironment.disableOperatorChaining()]]
   * however it is not advised for performance considerations.
   * 
   */
  def disableChaining(): DataStream[T] = {
    javaStream match {
      case ds: SingleOutputStreamOperator[_, _] => ds.disableChaining();
      case _ =>
        throw new UnsupportedOperationException("Only supported for operators.")
    }
    this
  }
  
  /**
   * Starts a new task chain beginning at this operator. This operator will
   * not be chained (thread co-located for increased performance) to any
   * previous tasks even if possible.
   * 
   */
  def startNewChain(): DataStream[T] = {
    javaStream match {
      case ds: SingleOutputStreamOperator[_, _] => ds.startNewChain();
      case _ =>
        throw new UnsupportedOperationException("Only supported for operators.")
    }
    this
  }
  
  /**
   * Isolates the operator in its own resource group. This will cause the
   * operator to grab as many task slots as its degree of parallelism. If
   * there are no free resources available, the job will fail to start.
   * All subsequent operators are assigned to the default resource group.
   * 
   */
  def isolateResources(): DataStream[T] = {
    javaStream match {
      case ds: SingleOutputStreamOperator[_, _] => ds.isolateResources();
      case _ =>
        throw new UnsupportedOperationException("Only supported for operators.")
    }
    this
  }
  
  /**
   * By default all operators in a streaming job share the same resource
   * group. Each resource group takes as many task manager slots as the
   * maximum parallelism operator in that group. By calling this method, this
   * operators starts a new resource group and all subsequent operators will
   * be added to this group unless specified otherwise. Please note that
   * local executions have by default as many available task slots as the
   * environment parallelism, so in order to start a new resource group the
   * degree of parallelism for the operators must be decreased from the
   * default.
   */
  def startNewResourceGroup(): DataStream[T] = {
    javaStream match {
      case ds: SingleOutputStreamOperator[_, _] => ds.startNewResourceGroup();
      case _ =>
        throw new UnsupportedOperationException("Only supported for operators.")
    }
    this
  }

  /**
   * Sets the maximum time frequency (ms) for the flushing of the output
   * buffer. By default the output buffers flush only when they are full.
   *
   * @param timeoutMillis
   * The maximum time between two output flushes.
   * @return The operator with buffer timeout set.
   */
  def setBufferTimeout(timeoutMillis: Long): DataStream[T] = {
    javaStream match {
      case ds: SingleOutputStreamOperator[_, _] => ds.setBufferTimeout(timeoutMillis);
      case _ =>
        throw new UnsupportedOperationException("Only supported for operators.")
    }
    this
  }

  /**
   * Creates a new DataStream by merging DataStream outputs of
   * the same type with each other. The DataStreams merged using this operator
   * will be transformed simultaneously.
   *
   */
  def union(dataStreams: DataStream[T]*): DataStream[T] =
    javaStream.union(dataStreams.map(_.getJavaStream): _*)

  /**
   * Creates a new ConnectedDataStream by connecting
   * DataStream outputs of different type with each other. The
   * DataStreams connected using this operators can be used with CoFunctions.
   */
  def connect[T2](dataStream: DataStream[T2]): ConnectedDataStream[T, T2] = 
    javaStream.connect(dataStream.getJavaStream)

  /**
   * Groups the elements of a DataStream by the given key positions (for tuple/array types) to
   * be used with grouped operators like grouped reduce or grouped aggregations.
   */
  def groupBy(fields: Int*): DataStream[T] = javaStream.groupBy(fields: _*)

  /**
   * Groups the elements of a DataStream by the given field expressions to
   * be used with grouped operators like grouped reduce or grouped aggregations.
   */
  def groupBy(firstField: String, otherFields: String*): DataStream[T] = 
   javaStream.groupBy(firstField +: otherFields.toArray: _*)   
  
  /**
   * Groups the elements of a DataStream by the given K key to
   * be used with grouped operators like grouped reduce or grouped aggregations.
   */
  def groupBy[K: TypeInformation](fun: T => K): DataStream[T] = {

    val keyExtractor = new KeySelector[T, K] {
      val cleanFun = clean(fun)
      def getKey(in: T) = cleanFun(in)
    }
    javaStream.groupBy(keyExtractor)
  }

  /**
   * Partitions the elements of a DataStream by the given key positions (for tuple/array types) to
   * be used with grouped operators like grouped reduce or grouped aggregations.
   */
  def partitionByHash(fields: Int*): DataStream[T] = javaStream.partitionByHash(fields: _*)

  /**
   * Groups the elements of a DataStream by the given field expressions to
   * be used with grouped operators like grouped reduce or grouped aggregations.
   */
  def partitionByHash(firstField: String, otherFields: String*): DataStream[T] =
    javaStream.partitionByHash(firstField +: otherFields.toArray: _*)

  /**
   * Groups the elements of a DataStream by the given K key to
   * be used with grouped operators like grouped reduce or grouped aggregations.
   */
  def partitionByHash[K: TypeInformation](fun: T => K): DataStream[T] = {

    val keyExtractor = new KeySelector[T, K] {
      val cleanFun = clean(fun)
      def getKey(in: T) = cleanFun(in)
    }
    javaStream.partitionByHash(keyExtractor)
  }

  /**
   * Sets the partitioning of the DataStream so that the output tuples
   * are broad casted to every parallel instance of the next component. This
   * setting only effects the how the outputs will be distributed between the
   * parallel instances of the next processing operator.
   *
   */
  def broadcast: DataStream[T] = javaStream.broadcast()
  
  /**
   * Sets the partitioning of the DataStream so that the output values all go to 
   * the first instance of the next processing operator. Use this setting with care
   * since it might cause a serious performance bottleneck in the application.
   */
  def global: DataStream[T] = javaStream.global()

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
  def rebalance: DataStream[T] = javaStream.rebalance()

  /**
   * Initiates an iterative part of the program that creates a loop by feeding
   * back data streams. To create a streaming iteration the user needs to define
   * a transformation that creates two DataStreams. The first one is the output
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
   * <p>
   * By default the feedback partitioning is set to match the input, to override this set 
   * the keepPartitioning flag to true
   *
   */
  def iterate[R](stepFunction: DataStream[T] => (DataStream[T], DataStream[R])): DataStream[R] =
    iterate(0)(stepFunction)
  

  /**
   * Initiates an iterative part of the program that creates a loop by feeding
   * back data streams. To create a streaming iteration the user needs to define
   * a transformation that creates two DataStreams. The first one is the output
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
   * <p>
   * By default the feedback partitioning is set to match the input, to override this set 
   * the keepPartitioning flag to true
   *
   */
  def iterate[R](maxWaitTimeMillis:Long = 0)
                (stepFunction: DataStream[T] => (DataStream[T], DataStream[R]), 
                    keepPartitioning: Boolean = false) : DataStream[R] = {
    val iterativeStream = javaStream.iterate(maxWaitTimeMillis)

    val (feedback, output) = stepFunction(new DataStream[T](iterativeStream))
    iterativeStream.closeWith(feedback.getJavaStream, keepPartitioning)
    output
  }

  /**
   * Applies an aggregation that that gives the current maximum of the data stream at
   * the given position.
   *
   */
  def max(position: Int): DataStream[T] = aggregate(AggregationType.MAX, position)
  
  /**
   * Applies an aggregation that that gives the current maximum of the data stream at
   * the given field.
   *
   */
  def max(field: String): DataStream[T] = aggregate(AggregationType.MAX, field)
  
  /**
   * Applies an aggregation that that gives the current minimum of the data stream at
   * the given position.
   *
   */
  def min(position: Int): DataStream[T] = aggregate(AggregationType.MIN, position)
  
  /**
   * Applies an aggregation that that gives the current minimum of the data stream at
   * the given field.
   *
   */
  def min(field: String): DataStream[T] = aggregate(AggregationType.MIN, field)

  /**
   * Applies an aggregation that sums the data stream at the given position.
   *
   */
  def sum(position: Int): DataStream[T] = aggregate(AggregationType.SUM, position)
  
  /**
   * Applies an aggregation that sums the data stream at the given field.
   *
   */
  def sum(field: String): DataStream[T] =  aggregate(AggregationType.SUM, field)

  /**
   * Applies an aggregation that that gives the current minimum element of the data stream by
   * the given position. When equality, the first element is returned with the minimal value.
   *
   */
  def minBy(position: Int): DataStream[T] = aggregate(AggregationType
    .MINBY, position)
    
   /**
   * Applies an aggregation that that gives the current minimum element of the data stream by
   * the given field. When equality, the first element is returned with the minimal value.
   *
   */
  def minBy(field: String): DataStream[T] = aggregate(AggregationType
    .MINBY, field )

   /**
   * Applies an aggregation that that gives the current maximum element of the data stream by
   * the given position. When equality, the first element is returned with the maximal value.
   *
   */
  def maxBy(position: Int): DataStream[T] =
    aggregate(AggregationType.MAXBY, position)
    
   /**
   * Applies an aggregation that that gives the current maximum element of the data stream by
   * the given field. When equality, the first element is returned with the maximal value.
   *
   */
  def maxBy(field: String): DataStream[T] =
    aggregate(AggregationType.MAXBY, field)
    
  private def aggregate(aggregationType: AggregationType, field: String): DataStream[T] = {
    val position = fieldNames2Indices(javaStream.getType(), Array(field))(0)
    aggregate(aggregationType, position)
  }

  private def aggregate(aggregationType: AggregationType, position: Int): DataStream[T] = {

    val jStream = javaStream.asInstanceOf[JavaStream[Product]]
    val outType = jStream.getType().asInstanceOf[TupleTypeInfoBase[_]]

    val agg = new ScalaStreamingAggregator[Product](
      jStream.getType().createSerializer(javaStream.getExecutionEnvironment.getConfig),
      position)

    val reducer = aggregationType match {
      case AggregationType.SUM => new agg.Sum(SumFunction.getForClass(outType.getTypeAt(position).
        getTypeClass()))
      case _ => new agg.ProductComparableAggregator(aggregationType, true)
    }

    val invokable = jStream match {
      case groupedStream: GroupedDataStream[Product] => new StreamGroupedReduce[Product](reducer,
        groupedStream.getKeySelector())
      case _ => new StreamReduce(reducer)
    }
    new DataStream[Product](jStream.transform("aggregation", jStream.getType(),invokable))
      .asInstanceOf[DataStream[T]]
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
    
    map(mapper)
  }

  /**
   * Creates a new DataStream by applying the given function to every element of this DataStream.
   */
  def map[R: TypeInformation: ClassTag](mapper: MapFunction[T, R]): DataStream[R] = {
    if (mapper == null) {
      throw new NullPointerException("Map function must not be null.")
    }

    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    javaStream.map(mapper).returns(outType).asInstanceOf[JavaStream[R]]
  }

  /**
   * Creates a new DataStream by applying the given function to every element and flattening
   * the results.
   */
  def flatMap[R: TypeInformation: ClassTag](flatMapper: FlatMapFunction[T, R]): DataStream[R] = {
    if (flatMapper == null) {
      throw new NullPointerException("FlatMap function must not be null.")
    }
    
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    javaStream.flatMap(flatMapper).returns(outType).asInstanceOf[JavaStream[R]]
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
 
    javaStream.reduce(reducer)
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
   * Creates a new [[DataStream]] by folding the elements of this DataStream
   * using an associative fold function and an initial value.
   */
  def fold[R: TypeInformation: ClassTag](initialValue: R, folder: FoldFunction[T,R]): 
  DataStream[R] = {
    if (folder == null) {
      throw new NullPointerException("Fold function must not be null.")
    }
    
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    javaStream.fold(initialValue, folder).returns(outType).asInstanceOf[JavaStream[R]]
  }

  /**
   * Creates a new [[DataStream]] by folding the elements of this DataStream
   * using an associative fold function and an initial value.
   */
  def fold[R: TypeInformation: ClassTag](initialValue: R, fun: (R,T) => R): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("Fold function must not be null.")
    }
    val folder = new FoldFunction[T,R] {
      val cleanFun = clean(fun)

      def fold(acc: R, v: T) = {
        cleanFun(acc, v)
      }
    }
    fold(initialValue, folder)
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
   * transformation like .reduceWindow(...) or aggregations on
   * preset chunks(windows) of the data stream. To define the windows a
   * WindowingHelper such as Time, Count and
   * Delta can be used.</br></br> When applied to a grouped data
   * stream, the windows (evictions) and slide sizes (triggers) will be
   * computed on a per group basis. </br></br> For more advanced control over
   * the trigger and eviction policies please use to
   * window(List(triggers), List(evicters))
   */
  def window(windowingHelper: WindowingHelper[_]): WindowedDataStream[T] =
    javaStream.window(windowingHelper)

  /**
   * Create a WindowedDataStream using the given Trigger and Eviction policies.
   * Windowing can be used to apply transformation like .reduceWindow(...) or 
   * aggregations on preset chunks(windows) of the data stream.</br></br>For most common
   * use-cases please refer to window(WindowingHelper[_])
   *
   */
  def window(trigger: TriggerPolicy[T], eviction: EvictionPolicy[T]):
    WindowedDataStream[T] = javaStream.window(trigger, eviction)
    
  /**
   * Create a WindowedDataStream based on the full stream history to perform periodic
   * aggregations.
   */  
  def every(windowingHelper: WindowingHelper[_]): WindowedDataStream[T] = 
    javaStream.every(windowingHelper)

  /**
   *
   * Operator used for directing tuples to specific named outputs using an
   * OutputSelector. Calling this method on an operator creates a new
   * SplitDataStream.
   */
  def split(selector: OutputSelector[T]): SplitDataStream[T] = javaStream.split(selector)

  /**
   * Creates a new SplitDataStream that contains only the elements satisfying the
   *  given output selector predicate.
   */
  def split(fun: T => TraversableOnce[String]): SplitDataStream[T] = {
    if (fun == null) {
      throw new NullPointerException("OutputSelector must not be null.")
    }
    val selector = new OutputSelector[T] {
      val cleanFun = clean(fun)
      def select(in: T): java.lang.Iterable[String] = {
        cleanFun(in).toIterable.asJava
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
   * Writes a DataStream to the standard output stream (stderr).
   * 
   * For each element of the DataStream the result of
   * [[AnyRef.toString()]] is written.
   *
   * @return The closed DataStream.
   */
  def printToErr() = javaStream.printToErr()

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
  def writeAsCsv(
      path: String,
      millis: Long = 0,
      rowDelimiter: String = ScalaCsvOutputFormat.DEFAULT_LINE_DELIMITER,
      fieldDelimiter: String = ScalaCsvOutputFormat.DEFAULT_FIELD_DELIMITER,
      writeMode: FileSystem.WriteMode = null): DataStream[T] = {
    require(javaStream.getType.isTupleType, "CSV output can only be used with Tuple DataSets.")
    val of = new ScalaCsvOutputFormat[Product](new Path(path), rowDelimiter, fieldDelimiter)
    if (writeMode != null) {
      of.setWriteMode(writeMode)
    }
    javaStream.write(of.asInstanceOf[OutputFormat[T]], millis)
  }

  /**
   * Writes a DataStream using the given [[OutputFormat]]. The
   * writing is performed periodically, in every millis milliseconds.
   */
  def write(format: OutputFormat[T], millis: Long): DataStreamSink[T] = {
    javaStream.write(format, millis)
  }

  /**
   * Writes the DataStream to a socket as a byte array. The format of the output is
   * specified by a [[SerializationSchema]].
   */
  def writeToSocket(hostname: String, port: Integer, schema: SerializationSchema[T, Array[Byte]]):
    DataStream[T] = javaStream.writeToSocket(hostname, port, schema)

  /**
   * Adds the given sink to this DataStream. Only streams with sinks added
   * will be executed once the StreamExecutionEnvironment.execute(...)
   * method is called.
   *
   */
  def addSink(sinkFunction: SinkFunction[T]): DataStream[T] =
    javaStream.addSink(sinkFunction)

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
