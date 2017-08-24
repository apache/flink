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

/**
  * The result future collects data/errors from the user code while processing
  * asynchronous I/O operations.
  *
  * @tparam OUT type of the output element
  */
@PublicEvolving
trait ResultFuture[OUT] {

  /**
    * Complete the ResultFuture with a set of result elements.
    *
    * Note that it should be called for exactly one time in the user code.
    * Calling this function for multiple times will cause data lose.
    *
    * Put all results in a [[Iterable]] and then issue ResultFuture.complete(Iterable).
    *
    * @param result to complete the async collector with
    */
  def complete(result: Iterable[OUT])

  /**
    * Complete this ResultFuture with an error.
    *
    * @param throwable to complete the async collector with
    */
  def completeExceptionally(throwable: Throwable)
}
