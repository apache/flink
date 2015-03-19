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

package org.apache.flink.ml.recommendation

import org.apache.flink.api.common.ExecutionMode
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.client.CliFrontendTestUtils
import org.junit.{BeforeClass, Test}
import org.scalatest.ShouldMatchers

import org.apache.flink.api.scala._

class ALSITCase extends ShouldMatchers {

  @Test
  def testMatrixFactorization(): Unit = {
    import ALSData._

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setDegreeOfParallelism(2)

    val als = ALS()
      .setIterations(iterations)
      .setLambda(lambda)
      .setBlocks(4)
      .setNumFactors(numFactors)

    val inputDS = env.fromCollection(data)

    val model = als.fit(inputDS)

    val testData = env.fromCollection(expectedResult.map{
      case (userID, itemID, rating) => (userID, itemID)
    })

    val predictions = model.transform(testData).collect

    predictions.length should equal(expectedResult.length)

    val resultMap = expectedResult map {
      case (uID, iID, value) => (uID, iID) -> value
    } toMap

    predictions foreach {
      case (uID, iID, value) => {
        resultMap.isDefinedAt(((uID, iID))) should be(true)

        value should be(resultMap((uID, iID)) +- 0.1)
      }
    }

    val risk = model.empiricalRisk(inputDS).collect(0)

    risk should be(expectedEmpiricalRisk +- 1)
  }
}

object ALSITCase {

  @BeforeClass
  def setup(): Unit = {
    CliFrontendTestUtils.pipeSystemOutToNull()
  }
}

object ALSData {

  val iterations = 9
  val lambda = 1.0
  val numFactors = 5

  val data: Seq[(Int, Int, Double)] = {
    Seq(
      (2,13,534.3937734561154),
      (6,14,509.63176469621936),
      (4,14,515.8246770897443),
      (7,3,495.05234565105),
      (2,3,532.3281786219485),
      (5,3,497.1906356844367),
      (3,3,512.0640508585093),
      (10,3,500.2906742233019),
      (1,4,521.9189079662882),
      (2,4,515.0734651491396),
      (1,7,522.7532725967008),
      (8,4,492.65683825096403),
      (4,8,492.65683825096403),
      (10,8,507.03319667905413),
      (7,1,522.7532725967008),
      (1,1,572.2230209271174),
      (2,1,563.5849190220224),
      (6,1,518.4844061038742),
      (9,1,529.2443732217674),
      (8,1,543.3202505434103),
      (7,2,516.0188923307859),
      (1,2,563.5849190220224),
      (1,11,515.1023793011227),
      (8,2,536.8571133978352),
      (2,11,507.90776961762225),
      (3,2,532.3281786219485),
      (5,11,476.24185144363304),
      (4,2,515.0734651491396),
      (4,11,469.92049343738233),
      (3,12,509.4713776280098),
      (4,12,494.6533165132021),
      (7,5,482.2907867916308),
      (6,5,477.5940040923741),
      (4,5,480.9040684364228),
      (1,6,518.4844061038742),
      (6,6,470.6605085832807),
      (8,6,489.6360564705307),
      (4,6,472.74052954447046),
      (7,9,482.5837650471611),
      (5,9,487.00175463269863),
      (9,9,500.69514584780944),
      (4,9,477.71644808419325),
      (7,10,485.3852917539852),
      (8,10,507.03319667905413),
      (3,10,500.2906742233019),
      (5,15,488.08215944254437),
      (6,15,480.16929757607346)
    )
  }

  val expectedResult: Seq[(Int, Int, Double)] = {
    Seq(
      (2, 2, 526.1037),
      (5, 9, 468.5680),
      (10, 3, 484.8975),
      (5, 13, 451.6228),
      (1, 15, 493.4956),
      (4, 11, 456.3862)
    )
  }

  val expectedEmpiricalRisk = 505374.1877
}
