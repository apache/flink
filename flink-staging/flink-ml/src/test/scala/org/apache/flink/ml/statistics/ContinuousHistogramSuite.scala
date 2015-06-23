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

package org.apache.flink.ml.statistics

import org.scalatest.{FlatSpec, Matchers}

class ContinuousHistogramSuite extends FlatSpec with Matchers {

  behavior of "Flink's ContinuousHistogram"

  it should "fail if capacity is non-positive" in {
    intercept[IllegalArgumentException] {
      new ContinuousHistogram(0, 0, 1)
    }
  }

  it should "fail if min>=max" in {
    intercept[IllegalArgumentException] {
      new ContinuousHistogram(2, 1, 1)
    }
  }

  it should "fail if list isn't sorted" in {
    intercept[IllegalArgumentException] {
      val l = Array((4.0, 2), (2.0, 2))
      new ContinuousHistogram(2, 1, 5).loadData(l)
    }
  }

  it should "fail if the list has any zero counters" in {
    intercept[IllegalArgumentException] {
      val l = Array((4.0, 2), (5.0, 0))
      new ContinuousHistogram(2, 1, 5).loadData(l)
    }
  }

  it should "succeed if the data is okay and access proper parameters, values and counters" in {

    var h = new ContinuousHistogram(2, 1, 10)
    h.bins should equal(0)

    var l = Array((4.0, 2), (5.0, 6))
    h = new ContinuousHistogram(2, 1, 10)
    h.loadData(l)
    h.bins should equal(2)
    h.getValue(0) should equal(4)
    h.getCounter(1) should equal(6)


    l = Array((1.0, 3), (3.0, 7), (7.0, 1), (10.0, 2))
    h = new ContinuousHistogram(3, 0, 11)
    h.loadData(l)
    h.bins should equal(3)
    h.getValue(0) should equal(2.4)
    h.getCounter(1) should equal(1)
    h.getCounter(2) should equal(2)
    h.add(5)
    h.getValue(1) should equal(6)

    h = new ContinuousHistogram(2, 0, 11)
    h.add(1)
    h.add(2)
    h.add(3)
    h.add(5)
    h.add(1)
    h.bins should equal(2)
    h.getValue(1) should equal(5)
    h.getCounter(0) should equal(4)
  }

  it should "fail in accessing out of bound values and counters" in {
    val l = Array((1.0, 3), (3.0, 7), (7.0, 1), (10.0, 2))
    val h = new ContinuousHistogram(3, 0, 11)
    h.loadData(l)
    intercept[IllegalArgumentException] {
      h.getValue(3)
    }
    intercept[IllegalArgumentException] {
      h.getCounter(3)
    }
  }

  it should "succeed in merging two histograms" in {
    val l = Array((1.0, 3), (3.0, 7), (7.0, 1), (10.0, 2))
    val h = new ContinuousHistogram(3, 0, 11)
    h.loadData(l)

    val l1 = Array((4.0, 2), (5.0, 6))
    val h1 = new ContinuousHistogram(2, 1, 10)
    h1.loadData(l1)

    val h3 = h1.merge(h, 3)
    h3.getValue(1) should equal(5)
    h3.getCounter(2) should equal(2)
  }

  it should "succeed in computing quantile" in {
    val l = Array((1.0, 5), (3.0, 4), (7.0, 3), (10.0, 2), (11.0, 1))
    val h = new ContinuousHistogram(5, 0, 12)
    h.loadData(l)

    h.count(h.quantile(0.05)) should equal(1)
    h.count(h.quantile(0.4)) should equal(6)
    h.count(h.quantile(0.8)) should equal(12)
    h.count(h.quantile(0.95)) should equal(14)
  }
}
