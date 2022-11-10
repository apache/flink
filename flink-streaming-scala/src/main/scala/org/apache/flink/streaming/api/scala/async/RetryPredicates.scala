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
import org.apache.flink.streaming.util.retryable.{RetryPredicates => JRetryPredicates}

import java.util
import java.util.function.Predicate

/** Utility class to create concrete retry predicates. */
@PublicEvolving
object RetryPredicates {

  /** A predicate matches empty result which means an empty {@link Collection}. */
  def EMPTY_RESULT_PREDICATE[T]: Predicate[util.Collection[T]] =
    JRetryPredicates.EMPTY_RESULT_PREDICATE.asInstanceOf[Predicate[util.Collection[T]]]

  /** A predicate matches any exception which means a non-null{@link Throwable}. */
  def HAS_EXCEPTION_PREDICATE: Predicate[Throwable] =
    JRetryPredicates.HAS_EXCEPTION_PREDICATE.asInstanceOf[Predicate[Throwable]]

}
