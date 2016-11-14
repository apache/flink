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

import java.math.BigDecimal
import org.apache.flink.api.table.Row
import org.junit.Test
import org.junit.Assert.assertEquals

abstract class AggregateTestBase[T] {

  private val offset = 2
  private val rowArity: Int = offset + aggregator.intermediateDataType.length

  def inputValueSets: Seq[Seq[_]]

  def expectedResults: Seq[T]

  def aggregator: Aggregate[T]

  private def createAggregator(): Aggregate[T] = {
    val agg = aggregator
    agg.setAggOffsetInRow(offset)
    agg
  }

  private def createRow(): Row = {
    new Row(rowArity)
  }

  @Test
  def testAggregate(): Unit = {

    // iterate over input sets
    for((vals, expected) <- inputValueSets.zip(expectedResults)) {

      // prepare mapper
      val rows: Seq[Row] = prepare(vals)

      val result = if (aggregator.supportPartial) {
        // test with combiner
        val (firstVals, secondVals) = rows.splitAt(rows.length / 2)
        val combined = partialAgg(firstVals) :: partialAgg(secondVals) :: Nil
        finalAgg(combined)

      } else {
        // test without combiner
        finalAgg(rows)
      }

      (expected, result) match {
        case (e: BigDecimal, r: BigDecimal) =>
          // BigDecimal.equals() value and scale but we are only interested in value.
          assert(e.compareTo(r) == 0)
        case _ =>
          assertEquals(expected, result)
      }
    }
  }

  private def prepare(vals: Seq[_]): Seq[Row] = {

    val agg = createAggregator()

    vals.map { v =>
      val row = createRow()
      agg.prepare(v, row)
      row
    }
  }

  private def partialAgg(rows: Seq[Row]): Row = {

    val agg = createAggregator()
    val aggBuf = createRow()

    agg.initiate(aggBuf)
    rows.foreach(v => agg.merge(v, aggBuf))

    aggBuf
  }

  private def finalAgg(rows: Seq[Row]): T = {

    val agg = createAggregator()
    val aggBuf = createRow()

    agg.initiate(aggBuf)
    rows.foreach(v => agg.merge(v, aggBuf))

    agg.evaluate(partialAgg(rows))
  }

}
