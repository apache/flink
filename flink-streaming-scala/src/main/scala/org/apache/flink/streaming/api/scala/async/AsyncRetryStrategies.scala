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

import org.apache.flink.streaming.api.functions.async
import org.apache.flink.streaming.api.functions.async.{AsyncRetryStrategy => JAsyncRetryStrategy}
import org.apache.flink.streaming.util.retryable.{AsyncRetryStrategies => JAsyncRetryStrategies}

import java.{util => ju}
import java.util.function.Predicate

object AsyncRetryStrategies {

  final private class JavaToScalaRetryStrategy[T] {
    def convert(retryStrategy: JAsyncRetryStrategy[T]): AsyncRetryStrategy[T] = {
      new AsyncRetryStrategy[T] {
        override def canRetry(currentAttempts: Int): Boolean =
          retryStrategy.canRetry(currentAttempts)

        override def getBackoffTimeMillis(currentAttempts: Int): Long =
          retryStrategy.getBackoffTimeMillis(currentAttempts)

        override def getRetryPredicate(): AsyncRetryPredicate[T] = new AsyncRetryPredicate[T] {
          val retryPredicates: async.AsyncRetryPredicate[T] = retryStrategy.getRetryPredicate

          override def resultPredicate: Option[Predicate[ju.Collection[T]]] = Option(
            retryPredicates.resultPredicate.orElse(null))

          override def exceptionPredicate: Option[Predicate[Throwable]] = Option(
            retryPredicates.exceptionPredicate.orElse(null))
        }
      }
    }
  }

  @SerialVersionUID(1L)
  class FixedDelayRetryStrategyBuilder[OUT](
      private val maxAttempts: Int,
      private val backoffTimeMillis: Long
  ) {
    private val converter = new JavaToScalaRetryStrategy[OUT]
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

    def build(): AsyncRetryStrategy[OUT] = {
      converter.convert(builder.build())
    }
  }

  @SerialVersionUID(1L)
  class ExponentialBackoffDelayRetryStrategyBuilder[OUT](
      private val maxAttempts: Int,
      private val initialDelay: Long,
      private val maxRetryDelay: Long,
      private val multiplier: Double
  ) {
    private val converter = new JavaToScalaRetryStrategy[OUT]
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

    def build(): AsyncRetryStrategy[OUT] = {
      converter.convert(builder.build())
    }
  }
}
