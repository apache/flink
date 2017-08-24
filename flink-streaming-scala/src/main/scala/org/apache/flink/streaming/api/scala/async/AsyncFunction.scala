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
  * A function to trigger async I/O operations.
  *
  * For each asyncInvoke an async io operation can be triggered, and once it has been done,
  * the result can be collected by calling ResultFuture.complete. For each async operation, its
  * context is stored in the operator immediately after invoking asyncInvoke, avoiding blocking for
  * each stream input as long as the internal buffer is not full.
  *
  * [[ResultFuture]] can be passed into callbacks or futures to collect the result data.
  * An error can also be propagate to the async IO operator by
  * [[ResultFuture.completeExceptionally(Throwable)]].
  *
  * @tparam IN The type of the input element
  * @tparam OUT The type of the output elements
  */
@PublicEvolving
trait AsyncFunction[IN, OUT] {

  /**
    * Trigger the async operation for each stream input
    *
    * @param input element coming from an upstream task
    * @param resultFuture to be completed with the result data
    */
  def asyncInvoke(input: IN, resultFuture: ResultFuture[OUT]): Unit
}
