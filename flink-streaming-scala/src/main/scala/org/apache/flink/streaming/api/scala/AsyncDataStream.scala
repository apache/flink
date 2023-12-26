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

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaAsyncDataStream}
import org.apache.flink.streaming.api.functions.async.{AsyncFunction => JavaAsyncFunction, AsyncRetryPredicate => JavaAsyncRetryPredicate, AsyncRetryStrategy => JavaAsyncRetryStrategy, ResultFuture => JavaResultFuture}
import org.apache.flink.streaming.api.scala.async._
import org.apache.flink.util.Preconditions

import java.util
import java.util.Optional
import java.util.function.Predicate

import scala.concurrent.duration.TimeUnit

/**
 * A helper class to apply [[AsyncFunction]] to a data stream.
 *
 * Example:
 * {{{
 *   val input: DataStream[String] = ...
 *   val asyncFunction: (String, ResultFuture[String]) => Unit = ...
 *
 *   AsyncDataStream.orderedWait(input, asyncFunction, timeout, TimeUnit.MILLISECONDS, 100)
 * }}}
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
@PublicEvolving
object AsyncDataStream {

  private val DEFAULT_QUEUE_CAPACITY = 100

  /**
   * Apply an asynchronous function on the input data stream. The output order is only maintained
   * with respect to watermarks. Stream records which lie between the same two watermarks, can be
   * re-ordered.
   *
   * @param input
   *   to apply the async function on
   * @param asyncFunction
   *   to use
   * @param timeout
   *   for the asynchronous operation to complete
   * @param timeUnit
   *   of the timeout
   * @param capacity
   *   of the operator which is equivalent to the number of concurrent asynchronous operations
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def unorderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int): DataStream[OUT] = {

    val javaAsyncFunction = wrapAsJavaAsyncFunction(asyncFunction)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .unorderedWait[IN, OUT](input.javaStream, javaAsyncFunction, timeout, timeUnit, capacity)
        .returns(outType))
  }

  /**
   * Apply an asynchronous function on the input data stream. The output order is only maintained
   * with respect to watermarks. Stream records which lie between the same two watermarks, can be
   * re-ordered.
   *
   * @param input
   *   to apply the async function on
   * @param asyncFunction
   *   to use
   * @param timeout
   *   for the asynchronous operation to complete
   * @param timeUnit
   *   of the timeout
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def unorderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit): DataStream[OUT] = {

    unorderedWait(input, asyncFunction, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY)
  }

  /**
   * Apply an asynchronous function on the input data stream. The output order is only maintained
   * with respect to watermarks. Stream records which lie between the same two watermarks, can be
   * re-ordered.
   *
   * @param input
   *   to apply the async function on
   * @param timeout
   *   for the asynchronous operation to complete
   * @param timeUnit
   *   of the timeout
   * @param capacity
   *   of the operator which is equivalent to the number of concurrent asynchronous operations
   * @param asyncFunction
   *   to use
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def unorderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int)(asyncFunction: (IN, ResultFuture[OUT]) => Unit): DataStream[OUT] = {

    Preconditions.checkNotNull(asyncFunction)

    val cleanAsyncFunction = input.executionEnvironment.scalaClean(asyncFunction)

    val func = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {

        cleanAsyncFunction(input, new JavaResultFutureWrapper[OUT](resultFuture))
      }
    }

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .unorderedWait[IN, OUT](input.javaStream, func, timeout, timeUnit, capacity)
        .returns(outType))
  }

  /**
   * Apply an asynchronous function on the input data stream. The output order is only maintained
   * with respect to watermarks. Stream records which lie between the same two watermarks, can be
   * re-ordered.
   *
   * @param input
   *   to apply the async function on
   * @param timeout
   *   for the asynchronous operation to complete
   * @param timeUnit
   *   of the timeout
   * @param asyncFunction
   *   to use
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def unorderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit)(asyncFunction: (IN, ResultFuture[OUT]) => Unit): DataStream[OUT] = {
    unorderedWait(input, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY)(asyncFunction)
  }

  /**
   * Apply an asynchronous function on the input data stream. The output order is the same as the
   * input order of the elements.
   *
   * @param input
   *   to apply the async function on
   * @param asyncFunction
   *   to use
   * @param timeout
   *   for the asynchronous operation to complete
   * @param timeUnit
   *   of the timeout
   * @param capacity
   *   of the operator which is equivalent to the number of concurrent asynchronous operations
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def orderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int): DataStream[OUT] = {

    val javaAsyncFunction = wrapAsJavaAsyncFunction(asyncFunction)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .orderedWait[IN, OUT](input.javaStream, javaAsyncFunction, timeout, timeUnit, capacity)
        .returns(outType))
  }

  /**
   * Apply an asynchronous function on the input data stream. The output order is the same as the
   * input order of the elements.
   *
   * @param input
   *   to apply the async function on
   * @param asyncFunction
   *   to use
   * @param timeout
   *   for the asynchronous operation to complete
   * @param timeUnit
   *   of the timeout
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def orderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit): DataStream[OUT] = {
    orderedWait(input, asyncFunction, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY)
  }

  /**
   * Apply an asynchronous function on the input data stream. The output order is the same as the
   * input order of the elements.
   *
   * @param input
   *   to apply the async function on
   * @param timeout
   *   for the asynchronous operation to complete
   * @param timeUnit
   *   of the timeout
   * @param capacity
   *   of the operator which is equivalent to the number of concurrent asynchronous operations
   * @param asyncFunction
   *   to use
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def orderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int)(asyncFunction: (IN, ResultFuture[OUT]) => Unit): DataStream[OUT] = {

    Preconditions.checkNotNull(asyncFunction)

    val cleanAsyncFunction = input.executionEnvironment.scalaClean(asyncFunction)

    val func = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {
        cleanAsyncFunction(input, new JavaResultFutureWrapper[OUT](resultFuture))
      }
    }

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .orderedWait[IN, OUT](input.javaStream, func, timeout, timeUnit, capacity)
        .returns(outType))
  }

  /**
   * Apply an asynchronous function on the input data stream. The output order is the same as the
   * input order of the elements.
   *
   * @param input
   *   to apply the async function on
   * @param timeout
   *   for the asynchronous operation to complete
   * @param timeUnit
   *   of the timeout
   * @param asyncFunction
   *   to use
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def orderedWait[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit)(asyncFunction: (IN, ResultFuture[OUT]) => Unit): DataStream[OUT] = {

    orderedWait(input, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY)(asyncFunction)
  }

  /**
   * Adds an AsyncWaitOperator with an AsyncRetryStrategy to support retry of AsyncFunction. The
   * order of output stream records may be reordered.
   *
   * @param input
   *   Input {@link DataStream}
   * @param asyncFunction
   *   {@link AsyncFunction}
   * @param timeout
   *   from first invoke to final completion of asynchronous operation, may include multiple
   *   retries, and will be reset in case of restart
   * @param timeUnit
   *   of the given timeout
   * @param capacity
   *   The max number of async i/o operation that can be triggered
   * @param asyncRetryStrategy
   *   The strategy of reattempt async i/o operation that can be triggered
   * @tparam IN
   *   Type of input record
   * @tparam OUT
   *   Type of output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def unorderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int,
      asyncRetryStrategy: AsyncRetryStrategy[OUT]): DataStream[OUT] = {

    Preconditions.checkArgument(timeout > 0)
    Preconditions.checkNotNull(asyncFunction)

    val javaAsyncFunction = wrapAsJavaAsyncFunction(asyncFunction)
    val javaAsyncRetryStrategy = wrapAsJavaAsyncRetryStrategy(asyncRetryStrategy)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .unorderedWaitWithRetry[IN, OUT](
          input.javaStream,
          javaAsyncFunction,
          timeout,
          timeUnit,
          capacity,
          javaAsyncRetryStrategy)
        .returns(outType))
  }

  /**
   * Adds an AsyncWaitOperator with an AsyncRetryStrategy to support retry of AsyncFunction. The
   * order of output stream records may be reordered.
   *
   * @param input
   *   Input {@link DataStream}
   * @param asyncFunction
   *   {@link AsyncFunction}
   * @param timeout
   *   from first invoke to final completion of asynchronous operation, may include multiple
   *   retries, and will be reset in case of restart
   * @param timeUnit
   *   of the given timeout
   * @param asyncRetryStrategy
   *   The strategy of reattempt async i/o operation that can be triggered
   * @tparam IN
   *   Type of input record
   * @tparam OUT
   *   Type of output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def unorderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      asyncRetryStrategy: AsyncRetryStrategy[OUT]): DataStream[OUT] = {

    unorderedWaitWithRetry(
      input,
      asyncFunction,
      timeout,
      timeUnit,
      DEFAULT_QUEUE_CAPACITY,
      asyncRetryStrategy)
  }

  /**
   * Apply an asynchronous function on the input data stream with an AsyncRetryStrategy to support
   * retry. The output order is only maintained with respect to watermarks. Stream records which lie
   * between the same two watermarks, can be re-ordered.
   *
   * @param input
   *   to apply the async function on
   * @param timeout
   *   from first invoke to final completion of asynchronous operation, may include multiple
   *   retries, and will be reset in case of restart
   * @param timeUnit
   *   of the timeout
   * @param capacity
   *   of the operator which is equivalent to the number of concurrent asynchronous operations
   * @param asyncFunction
   *   to use
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def unorderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int,
      asyncRetryStrategy: AsyncRetryStrategy[OUT])(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit): DataStream[OUT] = {

    Preconditions.checkNotNull(asyncFunction)
    Preconditions.checkArgument(timeout > 0)

    val cleanAsyncFunction = input.executionEnvironment.scalaClean(asyncFunction)

    val func = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {

        cleanAsyncFunction(input, new JavaResultFutureWrapper[OUT](resultFuture))
      }
    }
    val javaAsyncRetryStrategy = wrapAsJavaAsyncRetryStrategy(asyncRetryStrategy)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .unorderedWaitWithRetry[IN, OUT](
          input.javaStream,
          func,
          timeout,
          timeUnit,
          capacity,
          javaAsyncRetryStrategy)
        .returns(outType))
  }

  /**
   * Apply an asynchronous function on the input data stream with an AsyncRetryStrategy to support
   * retry. The output order is only maintained with respect to watermarks. Stream records which lie
   * between the same two watermarks, can be re-ordered.
   *
   * @param input
   *   to apply the async function on
   * @param timeout
   *   from first invoke to final completion of asynchronous operation, may include multiple
   *   retries, and will be reset in case of restart
   * @param timeUnit
   *   of the timeout
   * @param asyncFunction
   *   to use
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def unorderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      asyncRetryStrategy: AsyncRetryStrategy[OUT])(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit): DataStream[OUT] = {

    unorderedWaitWithRetry(input, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY, asyncRetryStrategy)(
      asyncFunction)
  }

  /**
   * Adds an AsyncWaitOperator with an AsyncRetryStrategy to support retry of AsyncFunction. The
   * output order is the same as the input order of the elements.
   *
   * @param input
   *   Input {@link DataStream}
   * @param asyncFunction
   *   {@link AsyncFunction}
   * @param timeout
   *   from first invoke to final completion of asynchronous operation, may include multiple
   *   retries, and will be reset in case of restart
   * @param timeUnit
   *   of the given timeout
   * @param capacity
   *   The max number of async i/o operation that can be triggered
   * @param asyncRetryStrategy
   *   The strategy of reattempt async i/o operation that can be triggered
   * @tparam IN
   *   Type of input record
   * @tparam OUT
   *   Type of output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def orderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int,
      asyncRetryStrategy: AsyncRetryStrategy[OUT]): DataStream[OUT] = {

    Preconditions.checkArgument(timeout > 0)
    Preconditions.checkNotNull(asyncFunction)

    val javaAsyncFunction = wrapAsJavaAsyncFunction(asyncFunction)
    val javaAsyncRetryStrategy = wrapAsJavaAsyncRetryStrategy(asyncRetryStrategy)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .orderedWaitWithRetry[IN, OUT](
          input.javaStream,
          javaAsyncFunction,
          timeout,
          timeUnit,
          capacity,
          javaAsyncRetryStrategy)
        .returns(outType))
  }

  /**
   * Adds an AsyncWaitOperator with an AsyncRetryStrategy to support retry of AsyncFunction. The
   * output order is the same as the input order of the elements.
   *
   * @param input
   *   Input {@link DataStream}
   * @param asyncFunction
   *   {@link AsyncFunction}
   * @param timeout
   *   from first invoke to final completion of asynchronous operation, may include multiple
   *   retries, and will be reset in case of restart
   * @param timeUnit
   *   of the given timeout
   * @param asyncRetryStrategy
   *   The strategy of reattempt async i/o operation that can be triggered
   * @tparam IN
   *   Type of input record
   * @tparam OUT
   *   Type of output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def orderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      asyncFunction: AsyncFunction[IN, OUT],
      timeout: Long,
      timeUnit: TimeUnit,
      asyncRetryStrategy: AsyncRetryStrategy[OUT]): DataStream[OUT] = {

    orderedWaitWithRetry(
      input,
      asyncFunction,
      timeout,
      timeUnit,
      DEFAULT_QUEUE_CAPACITY,
      asyncRetryStrategy)
  }

  /**
   * Apply an asynchronous function on the input data stream with an AsyncRetryStrategy to support
   * retry. The output order is the same as the input order of the elements.
   *
   * @param input
   *   to apply the async function on
   * @param timeout
   *   from first invoke to final completion of asynchronous operation, may include multiple
   *   retries, and will be reset in case of restart
   * @param timeUnit
   *   of the timeout
   * @param capacity
   *   of the operator which is equivalent to the number of concurrent asynchronous operations
   * @param asyncFunction
   *   to use
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def orderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      capacity: Int,
      asyncRetryStrategy: AsyncRetryStrategy[OUT])(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit): DataStream[OUT] = {

    Preconditions.checkArgument(timeout > 0)
    Preconditions.checkNotNull(asyncFunction)

    val cleanAsyncFunction = input.executionEnvironment.scalaClean(asyncFunction)

    val func = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {
        cleanAsyncFunction(input, new JavaResultFutureWrapper[OUT](resultFuture))
      }
    }
    val javaAsyncRetryStrategy = wrapAsJavaAsyncRetryStrategy(asyncRetryStrategy)

    val outType: TypeInformation[OUT] = implicitly[TypeInformation[OUT]]

    asScalaStream(
      JavaAsyncDataStream
        .orderedWaitWithRetry[IN, OUT](
          input.javaStream,
          func,
          timeout,
          timeUnit,
          capacity,
          javaAsyncRetryStrategy)
        .returns(outType))
  }

  /**
   * Apply an asynchronous function on the input data stream with an AsyncRetryStrategy to support
   * retry. The output order is the same as the input order of the elements.
   *
   * @param input
   *   to apply the async function on
   * @param timeout
   *   from first invoke to final completion of asynchronous operation, may include multiple
   *   retries, and will be reset in case of restart
   * @param timeUnit
   *   of the timeout
   * @param asyncFunction
   *   to use
   * @tparam IN
   *   Type of the input record
   * @tparam OUT
   *   Type of the output record
   * @return
   *   the resulting stream containing the asynchronous results
   */
  def orderedWaitWithRetry[IN, OUT: TypeInformation](
      input: DataStream[IN],
      timeout: Long,
      timeUnit: TimeUnit,
      asyncRetryStrategy: AsyncRetryStrategy[OUT])(
      asyncFunction: (IN, ResultFuture[OUT]) => Unit): DataStream[OUT] = {

    orderedWaitWithRetry(input, timeout, timeUnit, DEFAULT_QUEUE_CAPACITY, asyncRetryStrategy)(
      asyncFunction)
  }

  private def wrapAsJavaAsyncFunction[IN, OUT: TypeInformation](
      asyncFunction: AsyncFunction[IN, OUT]): JavaAsyncFunction[IN, OUT] = asyncFunction match {
    case richAsyncFunction: RichAsyncFunction[IN, OUT] =>
      new ScalaRichAsyncFunctionWrapper[IN, OUT](richAsyncFunction)
    case _ =>
      new JavaAsyncFunction[IN, OUT] {
        override def asyncInvoke(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {
          asyncFunction.asyncInvoke(input, new JavaResultFutureWrapper[OUT](resultFuture))
        }

        override def timeout(input: IN, resultFuture: JavaResultFuture[OUT]): Unit = {
          asyncFunction.timeout(input, new JavaResultFutureWrapper[OUT](resultFuture))
        }
      }
  }

  private def wrapAsJavaAsyncRetryStrategy[OUT](
      asyncRetryStrategy: AsyncRetryStrategy[OUT]): JavaAsyncRetryStrategy[OUT] = {
    new JavaAsyncRetryStrategy[OUT] {

      override def canRetry(currentAttempts: Int): Boolean =
        asyncRetryStrategy.canRetry(currentAttempts)

      override def getBackoffTimeMillis(currentAttempts: Int): Long =
        asyncRetryStrategy.getBackoffTimeMillis(currentAttempts)

      override def getRetryPredicate: JavaAsyncRetryPredicate[OUT] = {
        new JavaAsyncRetryPredicate[OUT] {
          override def resultPredicate(): Optional[Predicate[util.Collection[OUT]]] = {
            asyncRetryStrategy.getRetryPredicate().resultPredicate match {
              case Some(_) =>
                Optional.of(asyncRetryStrategy.getRetryPredicate().resultPredicate.get)
              case None =>
                Optional.empty()
            }
          }

          override def exceptionPredicate(): Optional[Predicate[Throwable]] = {
            asyncRetryStrategy.getRetryPredicate().exceptionPredicate match {
              case Some(_) =>
                Optional.of(asyncRetryStrategy.getRetryPredicate().exceptionPredicate.get)
              case None =>
                Optional.empty()
            }
          }
        }
      }
    }
  }
}
