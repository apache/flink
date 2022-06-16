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

import java.util
import java.util.function.Predicate

/** Interface encapsulates an asynchronous retry predicate. */
@PublicEvolving
trait AsyncRetryPredicate[OUT] {

  /**
   * An Optional Java {@Predicate } that defines a condition on asyncFunction's future result which
   * will trigger a later reattempt operation, will be called before user's ResultFuture#complete.
   *
   * @return
   *   predicate on result of {@link util.Collection}
   */
  def resultPredicate: Option[Predicate[util.Collection[OUT]]]

  /**
   * An Optional Java {@Predicate } that defines a condition on asyncFunction's exception which will
   * trigger a later reattempt operation, will be called before user's
   * ResultFuture#completeExceptionally.
   *
   * @return
   *   predicate on {@link Throwable} exception
   */
  def exceptionPredicate: Option[Predicate[Throwable]]
}
