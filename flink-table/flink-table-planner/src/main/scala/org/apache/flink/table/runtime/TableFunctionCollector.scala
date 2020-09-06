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
package org.apache.flink.table.runtime

import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.util.Collector

/**
  * The basic implementation of collector for [[org.apache.flink.table.functions.TableFunction]].
  */
abstract class TableFunctionCollector[T] extends AbstractRichFunction with Collector[T] {

  private var input: Any = _
  private var collector: Collector[_] = _
  private var collected: Boolean = _

  /**
    * Sets the input row from left table,
    * which will be used to cross join with the result of table function.
    */
  def setInput(input: Any): Unit = {
    this.input = input
  }

  /**
    * Gets the input value from left table,
    * which will be used to cross join with the result of table function.
    */
  def getInput: Any = {
    input
  }

  /**
    * Sets the current collector, which used to emit the final row.
    */
  def setCollector(collector: Collector[_]): Unit = {
    this.collector = collector
  }

  /**
    * Gets the internal collector which used to emit the final row.
    */
  def getCollector: Collector[_] = {
    this.collector
  }

  /**
    * Resets the flag to indicate whether [[collect(T)]] has been called.
    */
  def reset(): Unit = {
    collected = false
  }

  /**
    * Whether [[collect(T)]] has been called.
    *
    * @return True if [[collect(T)]] has been called.
    */
  def isCollected: Boolean = collected

  override def collect(record: T): Unit = {
    collected = true
  }
}


