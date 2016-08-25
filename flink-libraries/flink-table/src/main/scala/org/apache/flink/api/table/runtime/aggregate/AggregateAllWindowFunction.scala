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

package org.apache.flink.api.table.runtime.aggregate

import java.lang.Iterable

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.table.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

class AggregateAllWindowFunction(
    propertyReads: Array[WindowPropertyRead[_ <: Any]],
    groupReduceFunction: RichGroupReduceFunction[Row, Row])
  extends RichAllWindowFunction[Row, Row, Window] {

  private var propertyCollector: PropertyCollector = _

  override def open(parameters: Configuration): Unit = {
    groupReduceFunction.open(parameters)
    propertyCollector = new PropertyCollector(propertyReads)
  }

  override def apply(window: Window, input: Iterable[Row], out: Collector[Row]): Unit = {

    // extract the properties from window
    propertyReads.foreach(_.extract(window))

    // set final collector
    propertyCollector.finalCollector = out

    // call wrapped reduce function with property collector
    groupReduceFunction.reduce(input, propertyCollector)
  }
}
