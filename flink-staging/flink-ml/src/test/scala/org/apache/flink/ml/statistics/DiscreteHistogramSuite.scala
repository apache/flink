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

class DiscreteHistogramSuite extends FlatSpec with Matchers {

  behavior of "Flink's DiscreteHistogram"

  it should "fail if capacity is not positive" in {
    intercept[IllegalArgumentException] {
      new DiscreteHistogram(0)
    }
  }

  it should "fail if it is initialized with more values than the allowed capacity" in {
    intercept[IllegalArgumentException] {
      val values = Array((2.0, 3), (1.0, 2), (3.0, 4))
      new DiscreteHistogram(2).loadData(values)
    }
  }

  it should "fail if it is initialized with negative counters" in {
    intercept[IllegalArgumentException] {
      val values = Array((2.0, 3), (1.0, 2), (3.0, -1))
      new DiscreteHistogram(3).loadData(values)
    }
  }

  it should "succeed if the data loaded is okay" in {
    val values = Array((2.0, 3), (1.0, 2), (3.0, 1))
    new DiscreteHistogram(3).loadData(values)
  }

  it should "succeed in adding new values" in {
    var h = new DiscreteHistogram(4)
    h.add(1)
    h.add(2)
    h.add(3)
    h.add(1)
    h.add(3)
    h.add(3)
    h.add(4)
    h.count(1) should equal(2)
    h.count(2) should equal(1)
    h.count(3) should equal(3)
    h.count(4) should equal(1)
    h.count(5) should equal(0)
    intercept[IllegalArgumentException] {
      h.add(5)
    }

    val values = Array((2.0, 3), (1.0, 2), (3.0, 1))
    h = new DiscreteHistogram(4)
    h.loadData(values)
    h.count(2) should equal(3)
    h.count(1) should equal(2)
    h.count(3) should equal(1)
    h.add(2)
    h.count(2) should equal(4)
    h.add(4)
    h.count(4) should equal(1)
    intercept[IllegalArgumentException] {
      h.add(5)
    }
  }

  it should "succeed in merging two histograms" in {
    val h = new DiscreteHistogram(4)
    h.add(1)
    h.add(2)
    h.add(3)
    h.add(1)
    h.add(3)
    h.add(3)
    h.add(4)

    val values = Array((2.0, 3), (1.0, 2), (3.0, 1))
    val h1 = new DiscreteHistogram(4)
    h1.loadData(values)

    intercept[IllegalArgumentException] {
      h.merge(h1, 3)
    }
    val h2 = h.merge(h1, 4)
    h2.count(1) should equal(4)
    h2.count(2) should equal(4)
    h2.count(3) should equal(4)
    h2.count(4) should equal(1)
  }
}
