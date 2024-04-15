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
package org.apache.flink.streaming.api.scala.async

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.streaming.api.functions.async
import org.apache.flink.streaming.api.functions.async.{AsyncRetryStrategy => JAsyncRetryStrategy}
import org.apache.flink.streaming.util.retryable.{AsyncRetryStrategies => JAsyncRetryStrategies}

import java.{util => ju}
import java.util.function.Predicate

/**
 * Utility class to create concrete {@link AsyncRetryStrategy}.
 *
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
object AsyncRetryStrategies {

  final private class JavaToScalaRetryStrategy[T](retryStrategy: JAsyncRetryStrategy[T])
    extends AsyncRetryStrategy[T] {

    /** @return whether the next attempt can happen */
    override def canRetry(currentAttempts: Int): Boolean = retryStrategy.canRetry(currentAttempts)

    /** @return the delay time of next attempt */
    override def getBackoffTimeMillis(currentAttempts: Int): Long =
      retryStrategy.getBackoffTimeMillis(currentAttempts)

    /** @return the defined retry predicate {@link AsyncRetryPredicate} */
    override def getRetryPredicate(): AsyncRetryPredicate[T] = new AsyncRetryPredicate[T] {
      val retryPredicates: async.AsyncRetryPredicate[T] = retryStrategy.getRetryPredicate

      /**
       * An Optional Java {@Predicate } that defines a condition on asyncFunction's future result
       * which will trigger a later reattempt operation, will be called before user's
       * ResultFuture#complete.
       *
       * @return
       *   predicate on result of {@link ju.Collection}
       */
      override def resultPredicate: Option[Predicate[ju.Collection[T]]] = Option(
        retryPredicates.resultPredicate.orElse(null))

      /**
       * An Optional Java {@Predicate } that defines a condition on asyncFunction's exception which
       * will trigger a later reattempt operation, will be called before user's
       * ResultFuture#completeExceptionally.
       *
       * @return
       *   predicate on {@link Throwable} exception
       */
      override def exceptionPredicate: Option[Predicate[Throwable]] = Option(
        retryPredicates.exceptionPredicate.orElse(null))
    }
  }

  /**
   * FixedDelayRetryStrategyBuilder for building an {@link AsyncRetryStrategy} with fixed delay
   * retrying behaviours.
   */
  @deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
  @PublicEvolving
  @SerialVersionUID(1L)
  class FixedDelayRetryStrategyBuilder[OUT](
      private val maxAttempts: Int,
      private val backoffTimeMillis: Long
  ) {
    private var builder =
      new JAsyncRetryStrategies.FixedDelayRetryStrategyBuilder[OUT](maxAttempts, backoffTimeMillis)

    def ifResult(resultRetryPredicate: Predicate[ju.Collection[OUT]])
        : FixedDelayRetryStrategyBuilder[OUT] = {
      this.builder = this.builder.ifResult(resultRetryPredicate)
      this
    }

    def ifException(
        exceptionRetryPredicate: Predicate[Throwable]): FixedDelayRetryStrategyBuilder[OUT] = {
      this.builder = this.builder.ifException(exceptionRetryPredicate)
      this
    }

    def build(): AsyncRetryStrategy[OUT] = new JavaToScalaRetryStrategy[OUT](builder.build())
  }

  /**
   * ExponentialBackoffDelayRetryStrategyBuilder for building an {@link AsyncRetryStrategy} with
   * exponential delay retrying behaviours.
   */
  @deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
  @PublicEvolving
  @SerialVersionUID(1L)
  class ExponentialBackoffDelayRetryStrategyBuilder[OUT](
      private val maxAttempts: Int,
      private val initialDelay: Long,
      private val maxRetryDelay: Long,
      private val multiplier: Double
  ) {
    private var builder =
      new JAsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder[OUT](
        maxAttempts,
        initialDelay,
        maxRetryDelay,
        multiplier)

    def ifResult(resultRetryPredicate: Predicate[ju.Collection[OUT]])
        : ExponentialBackoffDelayRetryStrategyBuilder[OUT] = {
      this.builder = this.builder.ifResult(resultRetryPredicate)
      this
    }

    def ifException(exceptionRetryPredicate: Predicate[Throwable])
        : ExponentialBackoffDelayRetryStrategyBuilder[OUT] = {
      this.builder = this.builder.ifException(exceptionRetryPredicate)
      this
    }

    def build(): AsyncRetryStrategy[OUT] = new JavaToScalaRetryStrategy[OUT](builder.build())
  }
}
