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

import org.apache.flink.annotation.{PublicEvolving, Public}
import org.apache.flink.api.common.functions.{FlatJoinFunction, JoinFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{TimeJoinedStreams => JavaTimeJoinedStreams}
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow}
import org.apache.flink.util.Collector

/**
  * `JoinedStreams` represents two [[DataStream]]s that have been joined.
  * A streaming join operation is evaluated over elements in a window.
  *
  * To finalize the join operation you also need to specify a [[KeySelector]] for
  * both the first and second input and a [[WindowAssigner]]
  *
  * Note: Right now, the groups are being built in memory so you need to ensure that they don't
  * get too big. Otherwise the JVM might crash.
  *
  * Example:
  *
  * {{{
  * val one: DataStream[(String, Int)]  = ...
  * val two: DataStream[(String, Int)] = ...
  *
  * val result = one.join(two)
  *     .where {t => ... }
  *     .window(SlidingTimeWindows.of(Time.of(6, TimeUnit.MILLISECONDS),
  *       Time.of(2, TimeUnit.MILLISECONDS))
  *     .equalTo {t => ... }
  *     .window(TumblingTimeWindows.of(Time.of(2, TimeUnit.SECONDS)))
  *     .apply(new MyJoinFunction())
  * } }}}
  *
  */
@Public
object TimeJoinedStreams {

  /**
    * A join operation that does not yet have its [[KeySelector]]s defined.
    *
    * @tparam T1 Type of the elements from the first input
    * @tparam T2 Type of the elements from the second input
    */
  class Unspecified[T1, T2](input1: DataStream[T1], input2: DataStream[T2]) {

    /**
      * Specifies a [[KeySelector]] for elements from the first input.
      */
    def where[KEY: TypeInformation](keySelector: T1 => KEY): WithKey[T1, T2, KEY] = {
      val cleanFun = clean(keySelector)
      val keyType = implicitly[TypeInformation[KEY]]
      val javaSelector = new KeySelector[T1, KEY] with ResultTypeQueryable[KEY] {
        def getKey(in: T1) = cleanFun(in)
        override def getProducedType: TypeInformation[KEY] = keyType
      }
      new WithKey[T1, T2, KEY](input1, input2, javaSelector, null, keyType)
    }

    /**
      * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
      * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
      */
    private[flink] def clean[F <: AnyRef](f: F): F = {
      new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
    }
  }

  /**
    * Specify a [[WindowAssigner]] for input1 using [[window()]].
    *
    * @tparam T1 Type of the elements from the first input
    * @tparam T2 Type of the elements from the second input
    * @tparam KEY Type of the key. This must be the same for both inputs
    */
  class WithKey[T1, T2, KEY](
                              input1: DataStream[T1],
                              input2: DataStream[T2],
                              keySelector1: KeySelector[T1, KEY],
                              keySelector2: KeySelector[T2, KEY],
                              keyType: TypeInformation[KEY]) {


    /**
      * Specifies the window on which the join operation works.
      */
    @PublicEvolving
    def window(assigner: WindowAssigner[AnyRef, TimeWindow])
    : TimeJoinedStreams.WithOneWindow[T1, T2, KEY] = {
      if (keySelector1 == null ) {
        throw new UnsupportedOperationException(
          "You first need to specify KeySelectors for input1 first")
      }
      new TimeJoinedStreams.WithOneWindow[T1, T2, KEY](
        input1,
        input2,
        keySelector1,
        keySelector2,
        keyType,
        clean(assigner))
    }

    /**
      * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
      * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
      */
    private[flink] def clean[F <: AnyRef](f: F): F = {
      new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
    }
  }

  /**
    * A join operation that has [[KeySelector]]s defined for either both or
    * one input.
    *
    * You need to specify a [[KeySelector]] for the second input using [[equalTo()]]
    *
    * @tparam T1 Type of the elements from the first input
    * @tparam T2 Type of the elements from the second input
    * @tparam KEY Type of the key. This must be the same for both inputs
    */
  class WithOneWindow[T1, T2, KEY](input1: DataStream[T1],
                                  input2: DataStream[T2],
                                  keySelector1: KeySelector[T1, KEY],
                                  keySelector2: KeySelector[T2, KEY],
                                  keyType: TypeInformation[KEY],
                                  windowAssigner1: WindowAssigner[AnyRef, TimeWindow]) {

    /**
      * Specifies a [[KeySelector]] for elements from the second input.
      */
    def equalTo(keySelector: T2 => KEY): TimeJoinedStreams.WithKeysAndOneWindow[T1, T2, KEY] = {
      val cleanFun = clean(keySelector)
      val localKeyType = keyType
      val javaSelector = new KeySelector[T2, KEY] with ResultTypeQueryable[KEY] {
        def getKey(in: T2) = cleanFun(in)
        override def getProducedType: TypeInformation[KEY] = localKeyType
      }
      new WithKeysAndOneWindow[T1, T2, KEY](input1,
        input2,
        keySelector1,
        javaSelector,
        localKeyType,
        windowAssigner1)
    }

    /**
      * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
      * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
      */
    private[flink] def clean[F <: AnyRef](f: F): F = {
      new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
    }
  }

  /**
    * Specify a [[WindowAssigner]] for input1 using [[window()]].
    *
    * @tparam T1 Type of the elements from the first input
    * @tparam T2 Type of the elements from the second input
    * @tparam KEY Type of the key. This must be the same for both inputs
    */
  class WithKeysAndOneWindow[T1, T2, KEY](
                              input1: DataStream[T1],
                              input2: DataStream[T2],
                              keySelector1: KeySelector[T1, KEY],
                              keySelector2: KeySelector[T2, KEY],
                              keyType: TypeInformation[KEY],
                              windowAssigner1: WindowAssigner[AnyRef, TimeWindow]) {


    /**
      * Specifies the window on which the join operation works.
      */
    @PublicEvolving
    def window(assigner: WindowAssigner[AnyRef, TimeWindow])
    : TimeJoinedStreams.WithTwoWindows[T1, T2, KEY] = {
      if (keySelector1 == null || keySelector1 == null) {
        throw new UnsupportedOperationException("You first need to specify KeySelectors for both" +
          "inputs using where() and equalTo().")
      }

      new TimeJoinedStreams.WithTwoWindows[T1, T2, KEY](
        input1,
        input2,
        keySelector1,
        keySelector2,
        keyType,
        windowAssigner1,
        clean(assigner))
    }

    /**
      * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
      * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
      */
    private[flink] def clean[F <: AnyRef](f: F): F = {
      new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
    }
  }

    /**
    * A join operation that has [[KeySelector]]s defined for both inputs as
    * well as a [[WindowAssigner]].
    *
    * @tparam T1 Type of the elements from the first input
    * @tparam T2 Type of the elements from the second input
    * @tparam KEY Type of the key. This must be the same for both inputs
    */
  class WithTwoWindows[T1, T2, KEY](input1: DataStream[T1],
      input2: DataStream[T2],
      keySelector1: KeySelector[T1, KEY],
      keySelector2: KeySelector[T2, KEY],
      keyType: TypeInformation[KEY],
      windowAssigner1: WindowAssigner[AnyRef, TimeWindow],
      windowAssigner2: WindowAssigner[AnyRef, TimeWindow]) {

    /**
      * Completes the join operation with the user function that is executed
      * for windowed groups.
      */
    def apply[O: TypeInformation](fun: (T1, T2) => O): DataStream[O] = {
      require(fun != null, "Join function must not be null.")

      val joiner = new FlatJoinFunction[T1, T2, O] {
        val cleanFun = clean(fun)
        def join(left: T1, right: T2, out: Collector[O]) = {
          out.collect(cleanFun(left, right))
        }
      }
      apply(joiner)
    }

    /**
      * Completes the join operation with the user function that is executed
      * for windowed groups.
      */
    def apply[O: TypeInformation](fun: (T1, T2, Collector[O]) => Unit): DataStream[O] = {
      require(fun != null, "Join function must not be null.")

      val joiner = new FlatJoinFunction[T1, T2, O] {
        val cleanFun = clean(fun)
        def join(left: T1, right: T2, out: Collector[O]) = {
          cleanFun(left, right, out)
        }
      }
      apply(joiner)
    }

    /**
      * Completes the join operation with the user function that is executed
      * for windowed groups.
      */
    def apply[T: TypeInformation](function: JoinFunction[T1, T2, T]): DataStream[T] = {

      val join = new JavaTimeJoinedStreams[T1, T2](input1.javaStream, input2.javaStream)
      asScalaStream(join
        .where(keySelector1)
        .window(windowAssigner1)
        .equalTo(keySelector2)
        .window(windowAssigner2)
        .apply(clean(function), implicitly[TypeInformation[T]]))
    }

    /**
      * Completes the join operation with the user function that is executed
      * for windowed groups.
      */
    def apply[T: TypeInformation](function: FlatJoinFunction[T1, T2, T]): DataStream[T] = {

      val join = new JavaTimeJoinedStreams[T1, T2](input1.javaStream, input2.javaStream)

      asScalaStream(join
        .where(keySelector1)
        .window(windowAssigner1)
        .equalTo(keySelector2)
        .window(windowAssigner2)
        .apply(clean(function), implicitly[TypeInformation[T]]))

    }

    /**
      * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
      * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
      */
    private[flink] def clean[F <: AnyRef](f: F): F = {
      new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
    }
  }


  /**
    * Creates a new join operation from the two given inputs.
    */
  def createJoin[T1, T2](input1: DataStream[T1], input2: DataStream[T2])
  : TimeJoinedStreams.Unspecified[T1, T2] = {
    new TimeJoinedStreams.Unspecified[T1, T2](input1, input2)
  }

}

