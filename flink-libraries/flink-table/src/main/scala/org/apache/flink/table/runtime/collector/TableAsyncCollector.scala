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
package org.apache.flink.table.runtime.collector

import org.apache.flink.streaming.api.functions.async.ResultFuture

/**
  * The basic implementation of collector for [[ResultFuture]] in table joining.
  */
abstract class TableAsyncCollector[T] extends ResultFuture[T] {

  private var input: Any = _
  private var reusltFuture: ResultFuture[_] = _

  /**
    * Sets the input row from left table,
    * which will be used to cross join with the result of right table.
    */
  def setInput(input: Any): Unit = {
    this.input = input
  }

  /**
    * Gets the input value from left table,
    * which will be used to cross join with the result of right table.
    */
  def getInput: Any = {
    input
  }

  /**
    * Sets the current collector, which used to emit the final row.
    */
  def setCollector(collector: ResultFuture[_]): Unit = {
    this.reusltFuture = collector
  }

  /**
    * Gets the internal collector which used to emit the final row.
    */
  def getCollector: ResultFuture[_] = {
    this.reusltFuture
  }

  override def completeExceptionally(throwable: Throwable): Unit =
    reusltFuture.completeExceptionally(throwable)
}
