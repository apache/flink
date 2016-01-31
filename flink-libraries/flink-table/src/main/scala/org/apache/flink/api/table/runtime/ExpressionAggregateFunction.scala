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
package org.apache.flink.api.table.runtime

import org.apache.flink.api.table.Row
import org.apache.flink.api.common.functions.{GroupReduceFunction, GroupCombineFunction, RichGroupReduceFunction}
import org.apache.flink.api.java.aggregation.AggregationFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class ExpressionAggregateFunction(
    private val fieldPositions: Seq[Int],
    private val functions: Seq[AggregationFunction[Any]])
  extends RichGroupReduceFunction[Row, Row]
  with GroupCombineFunction[Row, Row]
{

  override def open(conf: Configuration): Unit = {
    var i = 0
    val len = functions.length
    while (i < len) {
      functions(i).initializeAggregate()
      i += 1
    }
  }

  override def reduce(in: java.lang.Iterable[Row], out: Collector[Row]): Unit = {

    val fieldPositions = this.fieldPositions
    val functions = this.functions

    var current: Row = null

    val values = in.iterator()
    while (values.hasNext) {
      current = values.next()

      var i = 0
      val len = functions.length
      while (i < len) {
        functions(i).aggregate(current.productElement(fieldPositions(i)))
        i += 1
      }
    }

    var i = 0
    val len = functions.length
    while (i < len) {
      current.setField(fieldPositions(i), functions(i).getAggregate)
      functions(i).initializeAggregate()
      i += 1
    }

    out.collect(current)
  }

  override def combine(in: java.lang.Iterable[Row], out: Collector[Row]): Unit = {
    reduce(in, out)
  }

}


class NoExpressionAggregateFunction()
  extends GroupReduceFunction[Row, Row]
  with GroupCombineFunction[Row, Row]
{

  override def reduce(in: java.lang.Iterable[Row], out: Collector[Row]): Unit = {

    var first: Row = null

    val values = in.iterator()
    if (values.hasNext) {
      first = values.next()
    }

    out.collect(first)
  }

  override def combine(in: java.lang.Iterable[Row], out: Collector[Row]): Unit = {
    reduce(in, out)
  }

}
