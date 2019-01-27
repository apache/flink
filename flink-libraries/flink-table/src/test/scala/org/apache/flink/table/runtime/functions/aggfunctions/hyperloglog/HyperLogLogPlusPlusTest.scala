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

package org.apache.flink.table.functions.aggfunctions.hyperloglog

import java.util.Random

import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.runtime.functions.aggfunctions.ApproximateCountDistinct.HllBuffer
import org.apache.flink.table.runtime.functions.aggfunctions.hyperloglog.HyperLogLogPlusPlus
import org.junit.Assert.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.Test

import scala.collection.mutable

class HyperLogLogPlusPlusTest {

  /** Create a HLL++ instance and an input and output buffer. */
  def createEstimator(rsd: Double):
  (HyperLogLogPlusPlus, GenericRow, HllBuffer) = {
    val input = new GenericRow(1)
    val hll = new HyperLogLogPlusPlus(rsd)
    val buffer = createBuffer(hll)
    (hll, input, buffer)
  }

  def createBuffer(hll: HyperLogLogPlusPlus): HllBuffer = {
    val numWords = hll.numWords
    val buffer = new HllBuffer
    buffer.array = new Array[Long](numWords)

    var word = 0
    while (word < numWords) {
      buffer.array(word) = 0
      word += 1
    }

    buffer
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidRelativeSD: Unit = {
    // `relativeSD` should be at most 39%.
    new HyperLogLogPlusPlus(relativeSD = 0.4)
  }

  @Test
  def testAddNulls: Unit = {
    val (hll, input, buffer) = createEstimator(0.05)
    input.setNullAt(0)
    hll.update(buffer, input.getField(0))
    hll.update(buffer, input.getField(0))
    val estimate = hll.query(buffer)
    assertEquals(0L, estimate)
  }

  def testCardinalityEstimates(rsds: Seq[Double],
                               ns: Seq[Int],
                               f: Int => Int,
                               c: Int => Int): Unit = {
    rsds.flatMap(rsd => ns.map(n => (rsd, n))).foreach {
      case (rsd, n) =>
        val (hll, input, buffer) = createEstimator(rsd)
        var i = 0
        while (i < n) {
          input.setInt(0, f(i))
          hll.update(buffer, input.getField(0))
          i += 1
        }
        val estimate = hll.query(buffer).toDouble
        val cardinality = c(n)
        val error = math.abs((estimate / cardinality.toDouble) - 1.0d)
        assertTrue(error < hll.trueRsd * 3.0d)
    }
  }

  @Test
  def testDeterministicCardinalityEstimates: Unit = {
    val repeats = 10
    testCardinalityEstimates(
      Seq(0.1, 0.05, 0.025, 0.01, 0.001),
      Seq(100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000).map(_ * repeats),
      i => i / repeats,
      i => i / repeats)
  }

  @Test
  def testRandomCardinalityEstimates: Unit = {
    val srng = new Random(323981238L)
    val seen = mutable.HashSet.empty[Int]
    val update = (i: Int) => {
      val value = srng.nextInt()
      seen += value
      value
    }
    val eval = (n: Int) => {
      val cardinality = seen.size
      seen.clear()
      cardinality
    }
    testCardinalityEstimates(
      Seq(0.05, 0.01),
      Seq(100, 10000, 500000),
      update,
      eval)
  }

  @Test
  def testMerging: Unit = {
    val (hll, input, buffer1a) = createEstimator(0.05)
    val buffer1b = createBuffer(hll)
    val buffer2 = createBuffer(hll)

    // Add the lower half
    var i = 0
    while (i < 500000) {
      input.setInt(0, i)
      hll.update(buffer1a, input.getField(0))
      i += 1
    }

    // Add the upper half
    i = 500000
    while (i < 1000000) {
      input.setInt(0, i)
      hll.update(buffer1b, input.getField(0))
      i += 1
    }

    // Merge the lower and upper halves.
    hll.merge(buffer1a, buffer1b)

    // Create the other buffer in reverse
    i = 999999
    while (i >= 0) {
      input.setInt(0, i)
      hll.update(buffer2, input.getField(0))
      i -= 1
    }

    // Check if the buffers are equal.
    assertArrayEquals(buffer2.array, buffer1a.array)
  }
}
