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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{ExecutionEnvironment}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.math.Vector
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class DataStatsSuite extends FlatSpec with Matchers with FlinkTestBase{

  behavior of "Flink's Data Statistics Collector"

  it should "succeed in evaluating statistics" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromCollection(Seq[Vector](
      DenseVector(Array(1,2)),
      DenseVector(Array(-1,1)),
      DenseVector(Array(4,0)),
      DenseVector(Array(8,1))
    )).setParallelism(2)

    val res = DataStats.dataStats(data, Array(1)).collect().head
    res.dimension should equal(2)
    res.total should equal(4)
    res.fields(0).min should equal(-1)
    res.fields(0).max should equal(8)
    res.fields(0).mean should equal(3)
    res.fields(0).variance should equal(11.5)
    res.fields(1).categoryCounts.get(0).get should equal(1)
    res.fields(1).categoryCounts.get(1).get should equal(2)
    res.fields(1).categoryCounts.get(2).get should equal(1)
    res.fields(1).entropy should equal(1.5 +- 0.001)
    res.fields(1).gini should equal(0.625)
  }
 }
