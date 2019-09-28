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
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.state.{FoldingStateDescriptor, ReducingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.datastream.{QueryableStateStream, DataStream => JavaStream, KeyedStream => KeyedJavaStream, WindowedStream => WindowedJavaStream}
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType
import org.apache.flink.streaming.api.functions.aggregation.{ComparableAggregator, SumAggregator}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.query.{QueryableAppendingStateOperator, QueryableValueStateOperator}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.operators.StreamGroupedReduce
import org.apache.flink.streaming.api.scala.function.StatefulFunction
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.util.Collector

@Public
class KeyedStream[T, K](javaStream: KeyedJavaStream[T, K]) extends DataStream[T](javaStream) {

  // ------------------------------------------------------------------------
  //  Properties
  // ------------------------------------------------------------------------

  /**
   * Gets the type of the key by which this stream is keyed.
   */
  @Internal
  def getKeyType = javaStream.getKeyType()


  // ------------------------------------------------------------------------
  //  basic transformations
  // ------------------------------------------------------------------------

  /**
    * Applies the given [[ProcessFunction]] on the input stream, thereby
    * creating a transformed output stream.
    *
    * The function will be called for every element in the stream and can produce
    * zero or more output. The function can also query the time and set timers. When
    * reacting to the firing of set timers the function can emit yet more elements.
    *
    * The function will be called for every element in the input streams and can produce zero
    * or more output elements. Contrary to the [[DataStream#flatMap(FlatMapFunction)]]
    * function, this function can also query the time and set timers. When reacting to the firing
    * of set timers the function can directly emit elements and/or register yet more timers.
    *
    * @param processFunction The [[ProcessFunction]] that is called for each element in the stream.
    *
    * @deprecated Use [[KeyedStream#process(KeyedProcessFunction)]]
    */
  @deprecated("will be removed in a future version")
  @PublicEvolving
  override def process[R: TypeInformation](
    processFunction: ProcessFunction[T, R]): DataStream[R] = {

    if (processFunction == null) {
      throw new NullPointerException("ProcessFunction must not be null.")
    }

    asScalaStream(javaStream.process(processFunction, implicitly[TypeInformation[R]]))
  }

  /**
   * Applies the given [[KeyedProcessFunction]] on the input stream, thereby
   * creating a transformed output stream.
   *
   * The function will be called for every element in the stream and can produce
   * zero or more output. The function can also query the time and set timers. When
   * reacting to the firing of set timers the function can emit yet more elements.
   *
   * The function will be called for every element in the input streams and can produce zero
   * or more output elements. Contrary to the [[DataStream#flatMap(FlatMapFunction)]]
   * function, this function can also query the time and set timers. When reacting to the firing
   * of set timers the function can directly emit elements and/or register yet more timers.
   *
   * @param keyedProcessFunction The [[KeyedProcessFunction]] that is called for each element
   *                             in the stream.
   */
  @PublicEvolving
  def process[R: TypeInformation](
    keyedProcessFunction: KeyedProcessFunction[K, T, R]): DataStream[R] = {

    if (keyedProcessFunction == null) {
      throw new NullPointerException("KeyedProcessFunction must not be null.")
    }

    asScalaStream(javaStream.process(keyedProcessFunction, implicitly[TypeInformation[R]]))
  }


  // ------------------------------------------------------------------------
  //  Joining
  // ------------------------------------------------------------------------

  /**
    * Join elements of this [[KeyedStream]] with elements of another [[KeyedStream]] over
    * a time interval that can be specified with [[IntervalJoin.between]].
    *
    * @param otherStream The other keyed stream to join this keyed stream with
    * @tparam OTHER Type parameter of elements in the other stream
    * @return An instance of [[IntervalJoin]] with this keyed stream and the other keyed stream
    */
  @PublicEvolving
  def intervalJoin[OTHER](otherStream: KeyedStream[OTHER, K]): IntervalJoin[T, OTHER, K] = {
    new IntervalJoin[T, OTHER, K](this, otherStream)
  }

  /**
    * Perform a join over a time interval.
    *
    * @tparam IN1 The type parameter of the elements in the first streams
    * @tparam IN2 The The type parameter of the elements in the second stream
    */
  @PublicEvolving
  class IntervalJoin[IN1, IN2, KEY](val streamOne: KeyedStream[IN1, KEY],
                                    val streamTwo: KeyedStream[IN2, KEY]) {

    /**
      * Specifies the time boundaries over which the join operation works, so that
      * <pre>leftElement.timestamp + lowerBound <= rightElement.timestamp
      * <= leftElement.timestamp + upperBound</pre>
      * By default both the lower and the upper bound are inclusive. This can be configured
      * with [[IntervalJoined.lowerBoundExclusive]] and
      * [[IntervalJoined.upperBoundExclusive]]
      *
      * @param lowerBound The lower bound. Needs to be smaller than or equal to the upperBound
      * @param upperBound The upper bound. Needs to be bigger than or equal to the lowerBound
      */
    @PublicEvolving
    def between(lowerBound: Time, upperBound: Time): IntervalJoined[IN1, IN2, KEY] = {
      val lowerMillis = lowerBound.toMilliseconds
      val upperMillis = upperBound.toMilliseconds
      new IntervalJoined[IN1, IN2, KEY](streamOne, streamTwo, lowerMillis, upperMillis)
    }
  }

  /**
    * IntervalJoined is a container for two streams that have keys for both sides as well as
    * the time boundaries over which elements should be joined.
    *
    * @tparam IN1 Input type of elements from the first stream
    * @tparam IN2 Input type of elements from the second stream
    * @tparam KEY The type of the key
    */
  @PublicEvolving
  class IntervalJoined[IN1, IN2, KEY](private val firstStream: KeyedStream[IN1, KEY],
                                      private val secondStream: KeyedStream[IN2, KEY],
                                      private val lowerBound: Long,
                                      private val upperBound: Long) {

    private var lowerBoundInclusive = true
    private var upperBoundInclusive = true

    /**
      * Set the lower bound to be exclusive
      */
    @PublicEvolving
    def lowerBoundExclusive(): IntervalJoined[IN1, IN2, KEY] = {
      this.lowerBoundInclusive = false
      this
    }

    /**
      * Set the upper bound to be exclusive
      */
    @PublicEvolving
    def upperBoundExclusive(): IntervalJoined[IN1, IN2, KEY] = {
      this.upperBoundInclusive = false
      this
    }

    /**
      * Completes the join operation with the user function that is executed for each joined pair
      * of elements.
      *
      * @param processJoinFunction The user-defined function
      * @tparam OUT The output type
      * @return Returns a DataStream
      */
    @PublicEvolving
    def process[OUT: TypeInformation](
        processJoinFunction: ProcessJoinFunction[IN1, IN2, OUT])
      : DataStream[OUT] = {

      val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

      val javaJoined = new KeyedJavaStream.IntervalJoined[IN1, IN2, KEY](
        firstStream.javaStream.asInstanceOf[KeyedJavaStream[IN1, KEY]],
        secondStream.javaStream.asInstanceOf[KeyedJavaStream[IN2, KEY]],
        lowerBound,
        upperBound,
        lowerBoundInclusive,
        upperBoundInclusive)
      asScalaStream(javaJoined.process(processJoinFunction, outType))
    }
  }

  // ------------------------------------------------------------------------
  //  Windowing
  // ------------------------------------------------------------------------

  /**
   * Windows this [[KeyedStream]] into tumbling time windows.
   *
   * This is a shortcut for either `.window(TumblingEventTimeWindows.of(size))` or
   * `.window(TumblingProcessingTimeWindows.of(size))` depending on the time characteristic
   * set using
   * [[StreamExecutionEnvironment.setStreamTimeCharacteristic()]]
   *
   * @param size The size of the window.
   */
  def timeWindow(size: Time): WindowedStream[T, K, TimeWindow] = {
    new WindowedStream(javaStream.timeWindow(size))
  }

  /**
   * Windows this [[KeyedStream]] into sliding count windows.
   *
   * @param size The size of the windows in number of elements.
   * @param slide The slide interval in number of elements.
   */
  def countWindow(size: Long, slide: Long): WindowedStream[T, K, GlobalWindow] = {
    new WindowedStream(javaStream.countWindow(size, slide))
  }

  /**
   * Windows this [[KeyedStream]] into tumbling count windows.
   *
   * @param size The size of the windows in number of elements.
   */
  def countWindow(size: Long): WindowedStream[T, K, GlobalWindow] = {
    new WindowedStream(javaStream.countWindow(size))
  }

  /**
   * Windows this [[KeyedStream]] into sliding time windows.
   *
   * This is a shortcut for either `.window(SlidingEventTimeWindows.of(size))` or
   * `.window(SlidingProcessingTimeWindows.of(size))` depending on the time characteristic
   * set using
   * [[StreamExecutionEnvironment.setStreamTimeCharacteristic()]]
   *
   * @param size The size of the window.
   */
  def timeWindow(size: Time, slide: Time): WindowedStream[T, K, TimeWindow] = {
    new WindowedStream(javaStream.timeWindow(size, slide))
  }

  /**
   * Windows this data stream to a [[WindowedStream]], which evaluates windows
   * over a key grouped stream. Elements are put into windows by a [[WindowAssigner]]. The
   * grouping of elements is done both by key and by window.
   *
   * A [[org.apache.flink.streaming.api.windowing.triggers.Trigger]] can be defined to specify
   * when windows are evaluated. However, `WindowAssigner` have a default `Trigger`
   * that is used if a `Trigger` is not specified.
   *
   * @param assigner The `WindowAssigner` that assigns elements to windows.
   * @return The trigger windows data stream.
   */
  @PublicEvolving
  def window[W <: Window](assigner: WindowAssigner[_ >: T, W]): WindowedStream[T, K, W] = {
    new WindowedStream(new WindowedJavaStream[T, K, W](javaStream, assigner))
  }

  // ------------------------------------------------------------------------
  //  Non-Windowed aggregation operations
  // ------------------------------------------------------------------------

  /**
   * Creates a new [[DataStream]] by reducing the elements of this DataStream
   * using an associative reduce function. An independent aggregate is kept per key.
   */
  def reduce(reducer: ReduceFunction[T]): DataStream[T] = {
    if (reducer == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
 
    asScalaStream(javaStream.reduce(reducer))
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
      def reduce(v1: T, v2: T) : T = { cleanFun(v1, v2) }
    }
    reduce(reducer)
  }

  /**
   * Creates a new [[DataStream]] by folding the elements of this DataStream
   * using an associative fold function and an initial value. An independent 
   * aggregate is kept per key.
   */
  @deprecated("will be removed in a future version")
  def fold[R: TypeInformation](initialValue: R, folder: FoldFunction[T,R]): 
      DataStream[R] = {
    if (folder == null) {
      throw new NullPointerException("Fold function must not be null.")
    }
    
    val outType : TypeInformation[R] = implicitly[TypeInformation[R]]
    
    asScalaStream(javaStream.fold(initialValue, folder).
      returns(outType).asInstanceOf[JavaStream[R]])
  }

  /**
   * Creates a new [[DataStream]] by folding the elements of this DataStream
   * using an associative fold function and an initial value. An independent 
   * aggregate is kept per key.
   */
  @deprecated("will be removed in a future version")
  def fold[R: TypeInformation](initialValue: R)(fun: (R,T) => R): DataStream[R] = {
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
   * @param position
   *            The field position in the data points to minimize. This is applicable to
   *            Tuple types, Scala case classes, and primitive types (which is considered
   *            as having one field).
   */
  def max(position: Int): DataStream[T] = aggregate(AggregationType.MAX, position)
  
  /**
   * Applies an aggregation that that gives the current maximum of the data stream at
   * the given field by the given key. An independent aggregate is kept per key.
   *
   * @param field
   *            In case of a POJO, Scala case class, or Tuple type, the
   *            name of the (public) field on which to perform the aggregation.
   *            Additionally, a dot can be used to drill down into nested
   *            objects, as in `"field1.fieldxy"`.
   *            Furthermore "*" can be specified in case of a basic type
   *            (which is considered as having only one field).
   */
  def max(field: String): DataStream[T] = aggregate(AggregationType.MAX, field)
  
  /**
   * Applies an aggregation that that gives the current minimum of the data stream at
   * the given position by the given key. An independent aggregate is kept per key.
   *
   * @param position
   *            The field position in the data points to minimize. This is applicable to
   *            Tuple types, Scala case classes, and primitive types (which is considered
   *            as having one field).
   */
  def min(position: Int): DataStream[T] = aggregate(AggregationType.MIN, position)
  
  /**
   * Applies an aggregation that that gives the current minimum of the data stream at
   * the given field by the given key. An independent aggregate is kept per key.
   *
   * @param field
   *            In case of a POJO, Scala case class, or Tuple type, the
   *            name of the (public) field on which to perform the aggregation.
   *            Additionally, a dot can be used to drill down into nested
   *            objects, as in `"field1.fieldxy"`.
   *            Furthermore "*" can be specified in case of a basic type
   *            (which is considered as having only one field).
   */
  def min(field: String): DataStream[T] = aggregate(AggregationType.MIN, field)

  /**
   * Applies an aggregation that sums the data stream at the given position by the given 
   * key. An independent aggregate is kept per key.
   *
   * @param position
   *            The field position in the data points to minimize. This is applicable to
   *            Tuple types, Scala case classes, and primitive types (which is considered
   *            as having one field).
   */
  def sum(position: Int): DataStream[T] = aggregate(AggregationType.SUM, position)
  
  /**
   * Applies an aggregation that sums the data stream at the given field by the given 
   * key. An independent aggregate is kept per key.
   *
   * @param field
   *            In case of a POJO, Scala case class, or Tuple type, the
   *            name of the (public) field on which to perform the aggregation.
   *            Additionally, a dot can be used to drill down into nested
   *            objects, as in `"field1.fieldxy"`.
   *            Furthermore "*" can be specified in case of a basic type
   *            (which is considered as having only one field).
   */
  def sum(field: String): DataStream[T] =  aggregate(AggregationType.SUM, field)

  /**
   * Applies an aggregation that that gives the current minimum element of the data stream by
   * the given position by the given key. An independent aggregate is kept per key. 
   * When equality, the first element is returned with the minimal value.
   *
   * @param position
   *            The field position in the data points to minimize. This is applicable to
   *            Tuple types, Scala case classes, and primitive types (which is considered
   *            as having one field).
   */
  def minBy(position: Int): DataStream[T] = aggregate(AggregationType
    .MINBY, position)
    
   /**
    * Applies an aggregation that that gives the current minimum element of the data stream by
    * the given field by the given key. An independent aggregate is kept per key.
    * When equality, the first element is returned with the minimal value.
    *
    * @param field
    *            In case of a POJO, Scala case class, or Tuple type, the
    *            name of the (public) field on which to perform the aggregation.
    *            Additionally, a dot can be used to drill down into nested
    *            objects, as in `"field1.fieldxy"`.
    *            Furthermore "*" can be specified in case of a basic type
    *            (which is considered as having only one field).
    */
  def minBy(field: String): DataStream[T] = aggregate(AggregationType
    .MINBY, field )

   /**
    * Applies an aggregation that that gives the current maximum element of the data stream by
    * the given position by the given key. An independent aggregate is kept per key.
    * When equality, the first element is returned with the maximal value.
    *
    * @param position
    *            The field position in the data points to minimize. This is applicable to
    *            Tuple types, Scala case classes, and primitive types (which is considered
    *            as having one field).
    */
  def maxBy(position: Int): DataStream[T] =
    aggregate(AggregationType.MAXBY, position)
    
   /**
    * Applies an aggregation that that gives the current maximum element of the data stream by
    * the given field by the given key. An independent aggregate is kept per key.
    * When equality, the first element is returned with the maximal value.
    *
    * @param field
    *            In case of a POJO, Scala case class, or Tuple type, the
    *            name of the (public) field on which to perform the aggregation.
    *            Additionally, a dot can be used to drill down into nested
    *            objects, as in `"field1.fieldxy"`.
    *            Furthermore "*" can be specified in case of a basic type
    *            (which is considered as having only one field).
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

    val invokable =  new StreamGroupedReduce[T](reducer,
      getType().createSerializer(getExecutionConfig))
     
    new DataStream[T](javaStream.transform("aggregation", javaStream.getType(),invokable))
      .asInstanceOf[DataStream[T]]
  }

  // ------------------------------------------------------------------------
  //  functions with state
  // ------------------------------------------------------------------------
  
  /**
   * Creates a new DataStream that contains only the elements satisfying the given stateful filter 
   * predicate. To use state partitioning, a key must be defined using .keyBy(..), in which case
   * an independent state will be kept per key.
   *
   * Note that the user state object needs to be serializable.
   */
  def filterWithState[S : TypeInformation](
        fun: (T, Option[S]) => (Boolean, Option[S])): DataStream[T] = {
    if (fun == null) {
      throw new NullPointerException("Filter function must not be null.")
    }

    val cleanFun = clean(fun)
    val stateTypeInfo: TypeInformation[S] = implicitly[TypeInformation[S]]
    val serializer: TypeSerializer[S] = stateTypeInfo.createSerializer(getExecutionConfig)

    val filterFun = new RichFilterFunction[T] with StatefulFunction[T, Boolean, S] {

      override val stateSerializer: TypeSerializer[S] = serializer

      override def filter(in: T): Boolean = {
        applyWithState(in, cleanFun)
      }
    }

    filter(filterFun)
  }

  /**
   * Creates a new DataStream by applying the given stateful function to every element of this 
   * DataStream. To use state partitioning, a key must be defined using .keyBy(..), in which 
   * case an independent state will be kept per key.
   *
   * Note that the user state object needs to be serializable.
   */
  def mapWithState[R: TypeInformation, S: TypeInformation](
        fun: (T, Option[S]) => (R, Option[S])): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("Map function must not be null.")
    }

    val cleanFun = clean(fun)
    val stateTypeInfo: TypeInformation[S] = implicitly[TypeInformation[S]]
    val serializer: TypeSerializer[S] = stateTypeInfo.createSerializer(getExecutionConfig)
    
    val mapper = new RichMapFunction[T, R] with StatefulFunction[T, R, S] {

      override val stateSerializer: TypeSerializer[S] = serializer
      
      override def map(in: T): R = {
        applyWithState(in, cleanFun)
      }
    }

    map(mapper)
  }
  
  /**
   * Creates a new DataStream by applying the given stateful function to every element and 
   * flattening the results. To use state partitioning, a key must be defined using .keyBy(..), 
   * in which case an independent state will be kept per key.
   *
   * Note that the user state object needs to be serializable.
   */
  def flatMapWithState[R: TypeInformation, S: TypeInformation](
        fun: (T, Option[S]) => (TraversableOnce[R], Option[S])): DataStream[R] = {
    if (fun == null) {
      throw new NullPointerException("Flatmap function must not be null.")
    }

    val cleanFun = clean(fun)
    val stateTypeInfo: TypeInformation[S] = implicitly[TypeInformation[S]]
    val serializer: TypeSerializer[S] = stateTypeInfo.createSerializer(getExecutionConfig)
    
    val flatMapper = new RichFlatMapFunction[T, R] with StatefulFunction[T,TraversableOnce[R],S]{

      override val stateSerializer: TypeSerializer[S] = serializer
      
      override def flatMap(in: T, out: Collector[R]): Unit = {
        applyWithState(in, cleanFun) foreach out.collect
      }
    }

    flatMap(flatMapper)
  }

  /**
    * Publishes the keyed stream as a queryable ValueState instance.
    *
    * @param queryableStateName Name under which to the publish the queryable state instance
    * @return Queryable state instance
    */
  @PublicEvolving
  def asQueryableState(queryableStateName: String) : QueryableStateStream[K, T] = {
    val stateDescriptor = new ValueStateDescriptor(
      queryableStateName,
      dataType.createSerializer(executionConfig))

    asQueryableState(queryableStateName, stateDescriptor)
  }

  /**
    * Publishes the keyed stream as a queryable ValueState instance.
    *
    * @param queryableStateName Name under which to the publish the queryable state instance
    * @param stateDescriptor State descriptor to create state instance from
    * @return Queryable state instance
    */
  @PublicEvolving
  def asQueryableState(
      queryableStateName: String,
      stateDescriptor: ValueStateDescriptor[T]) : QueryableStateStream[K, T] = {

    transform(
      s"Queryable state: $queryableStateName",
      new QueryableValueStateOperator(queryableStateName, stateDescriptor))(dataType)

    stateDescriptor.initializeSerializerUnlessSet(executionConfig)

    new QueryableStateStream(
      queryableStateName,
      stateDescriptor,
      getKeyType.createSerializer(executionConfig))
  }

  /**
    * Publishes the keyed stream as a queryable FoldingState instance.
    *
    * @param queryableStateName Name under which to the publish the queryable state instance
    * @param stateDescriptor State descriptor to create state instance from
    * @return Queryable state instance
    */
  @PublicEvolving
  @deprecated("will be removed in a future version")
  def asQueryableState[ACC](
      queryableStateName: String,
      stateDescriptor: FoldingStateDescriptor[T, ACC]) : QueryableStateStream[K, ACC] =  {

    transform(
      s"Queryable state: $queryableStateName",
      new QueryableAppendingStateOperator(queryableStateName, stateDescriptor))(dataType)

    stateDescriptor.initializeSerializerUnlessSet(executionConfig)

    new QueryableStateStream(
      queryableStateName,
      stateDescriptor,
      getKeyType.createSerializer(executionConfig))
  }

  /**
    * Publishes the keyed stream as a queryable ReducingState instance.
    *
    * @param queryableStateName Name under which to the publish the queryable state instance
    * @param stateDescriptor State descriptor to create state instance from
    * @return Queryable state instance
    */
  @PublicEvolving
  def asQueryableState(
      queryableStateName: String,
      stateDescriptor: ReducingStateDescriptor[T]) : QueryableStateStream[K, T] = {

    transform(
      s"Queryable state: $queryableStateName",
      new QueryableAppendingStateOperator(queryableStateName, stateDescriptor))(dataType)

    stateDescriptor.initializeSerializerUnlessSet(executionConfig)

    new QueryableStateStream(
      queryableStateName,
      stateDescriptor,
      getKeyType.createSerializer(executionConfig))
  }
  
}
