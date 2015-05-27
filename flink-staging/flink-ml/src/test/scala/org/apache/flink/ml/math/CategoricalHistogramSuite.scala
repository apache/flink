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

package org.apache.flink.ml.math

import org.scalatest.{FlatSpec, Matchers}

class CategoricalHistogramSuite extends FlatSpec with Matchers {

  behavior of "Flink's CategoricalHistogram"

  it should "fail if values is empty" in {
    intercept[IllegalArgumentException] {
      new CategoricalHistogram(Array.ofDim[Double](0))
    }
  }

  it should "fail if length of counts is not equal to length of values" in {
    intercept[IllegalArgumentException] {
      new CategoricalHistogram(Array.ofDim[Double](5), Array.ofDim[Int](1))
    }
  }

  it should "fail if there are any negative values in counters" in {
    intercept[IllegalArgumentException] {
      val values = Array(2.0, 3.0)
      val counters = Array(1, -2)
      new CategoricalHistogram(values, counters)
    }
  }

  it should "fail in accessing out of bounds indexes" in {
    val values = Array(2.0, 3.0, 1.0, 5.0, 9.5)
    val h = new CategoricalHistogram(values)
    intercept[IllegalArgumentException] {
      h.getValue(8)
    }
    intercept[IllegalArgumentException] {
      h.getCounter(-1)
    }
  }

  it should "succeed if the data is okay and access proper parameters, values and counters" in {
    val values = Array(2.0, 3.0, 1.0, 5.0, 9.5)
    var h = new CategoricalHistogram(values)
    h.getValue(2) should equal(1.0)
    h.getCounter(4) should equal(0)
    val counters = Array(1, 0, 4, 6, 1)
    h = new CategoricalHistogram(values, counters)
    h.getCounter(4) should equal(1)
  }

  it should "succeed in adding new values" in {
    val values = Array(2.0, 3.0, 1.0, 5.0, 9.5)
    val counters = Array(1, 0, 4, 6, 1)
    val h = new CategoricalHistogram(values, counters)
    h.add(3.0)
    h.add(9.5)
    h.getCounter(1) should equal(1)
    h.getCounter(4) should equal(2)
  }

  it should "fail in adding a value not specified while constructing the histogram" in {
    val values = Array(2.0, 3.0, 1.0, 5.0, 9.5)
    val counters = Array(1, 0, 4, 6, 1)
    val h = new CategoricalHistogram(values, counters)
    intercept[IllegalArgumentException] {
      h.add(9.4)
    }
  }

  it should "fail in computing a quantile" in {
    val values = Array(2.0, 3.0, 1.0, 5.0, 9.5)
    val counters = Array(1, 0, 4, 6, 1)
    val h = new CategoricalHistogram(values, counters)
    intercept[IllegalArgumentException] {
      h.quantile(0.1)
    }
  }

  it should "succeed in accessing counts of different values" in {
    val values = Array(2.0, 3.0, 1.0, 5.0, 9.5)
    val counters = Array(1, 0, 4, 6, 1)
    val h = new CategoricalHistogram(values, counters)
    h.count(1.0) should equal(4)
    h.count(9.5) should equal(1)
    intercept[IllegalArgumentException] {
      h.count(9.4)
    }
  }

  it should "succeed in merging only and only two similar histograms" in {
    val values1 = Array(2.0, 3.0, 1.0, 5.0, 9.5)
    val values2 = Array(2.0, 3.0, 1.0, 5.0)
    val values3 = Array(2.0, 1.0, 3.0, 5.0, 9.5)
    val counters1 = Array(1, 0, 4, 6, 1)
    val counters2 = Array(5, 1, 10, 0, 1)
    val h1 = new CategoricalHistogram(values1, counters1)
    val h2 = new CategoricalHistogram(values1, counters2)
    val h3 = h1.merge(h2, 5)
    h3.getCounter(2) should equal(14)
    val h4 = new CategoricalHistogram(values2)
    intercept[IllegalArgumentException] {
      h1.merge(h4, 5)
    }
    val h5 = new CategoricalHistogram(values3)
    intercept[IllegalArgumentException] {
      h1.merge(h5, 5)
    }
  }
}
