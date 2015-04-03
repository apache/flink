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
package org.apache.flink.ml.preprocessing

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{Vector, DenseVector}
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest._


class StandardScalerITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Standard Scaler"

  import StandardScalerData._

  it should "first center and then properly scale the given vectors" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)
    val transformer = new StandardScaler()
    val scaledVectors = transformer.transform(dataSet).collect

    scaledVectors.length should equal(expectedVectors.length)

    scaledVectors zip expectedVectors foreach {
      case (scaledVector, expectedVector) => {
        for (i <- 0 until scaledVector.size) {
          scaledVector(i) should be(expectedVector(i) +- (0.000001))
        }
      }
    }
  }
}

object StandardScalerData {

  val data: Seq[Vector] = List(DenseVector(Array(2104.00, 3.00)),
    DenseVector(Array(1600.00, 3.00)),
    DenseVector(Array(2400.00, 3.00)),
    DenseVector(Array(1416.00, 2.00)),
    DenseVector(Array(3000.00, 4.00)),
    DenseVector(Array(1985.00, 4.00)),
    DenseVector(Array(1534.00, 3.00)),
    DenseVector(Array(1427.00, 3.00)),
    DenseVector(Array(1380.00, 3.00)),
    DenseVector(Array(1494.00, 3.00)),
    DenseVector(Array(1940.00, 4.00)),
    DenseVector(Array(2000.00, 3.00)),
    DenseVector(Array(1890.00, 3.00)),
    DenseVector(Array(4478.00, 5.00)),
    DenseVector(Array(1268.00, 3.00)),
    DenseVector(Array(2300.00, 4.00)),
    DenseVector(Array(1320.00, 2.00)),
    DenseVector(Array(1236.00, 3.00)),
    DenseVector(Array(2609.00, 4.00)),
    DenseVector(Array(3031.00, 4.00)),
    DenseVector(Array(1767.00, 3.00)),
    DenseVector(Array(1888.00, 2.00)),
    DenseVector(Array(1604.00, 3.00)),
    DenseVector(Array(1962.00, 4.00)),
    DenseVector(Array(3890.00, 3.00)),
    DenseVector(Array(1100.00, 3.00)),
    DenseVector(Array(1458.00, 3.00)),
    DenseVector(Array(2526.00, 3.00)),
    DenseVector(Array(2200.00, 3.00)),
    DenseVector(Array(2637.00, 3.00)),
    DenseVector(Array(1839.00, 2.00)),
    DenseVector(Array(1000.00, 1.00)),
    DenseVector(Array(2040.00, 4.00)),
    DenseVector(Array(3137.00, 3.00)),
    DenseVector(Array(1811.00, 4.00)),
    DenseVector(Array(1437.00, 3.00)),
    DenseVector(Array(1239.00, 3.00)),
    DenseVector(Array(2132.00, 4.00)),
    DenseVector(Array(4215.00, 4.00)),
    DenseVector(Array(2162.00, 4.00)),
    DenseVector(Array(1664.00, 2.00)),
    DenseVector(Array(2238.00, 3.00)),
    DenseVector(Array(2567.00, 4.00)),
    DenseVector(Array(1200.00, 3.00)),
    DenseVector(Array(852.00, 2.00)),
    DenseVector(Array(1852.00, 4.00)),
    DenseVector(Array(1203.00, 3.00))
  )

  val expectedVectors: Seq[Vector] = List(
    DenseVector(Array(0.131415422021048, -0.226093367577688)),
    DenseVector(Array(-0.509640697590685, -0.226093367577688)),
    DenseVector(Array(0.507908698618414, -0.226093367577688)),
    DenseVector(Array(-0.743677058718778, -1.554391902096608)),
    DenseVector(Array(1.271070745775239, 1.102205166941232)),
    DenseVector(Array(-0.019945050665056, 1.102205166941232)),
    DenseVector(Array(-0.593588522777936, -0.226093367577688)),
    DenseVector(Array(-0.729685754520903, -0.226093367577688)),
    DenseVector(Array(-0.789466781548187, -0.226093367577688)),
    DenseVector(Array(-0.644465992588391, -0.226093367577688)),
    DenseVector(Array(-0.077182204201818, 1.102205166941232)),
    DenseVector(Array(-0.000865999486135, -0.226093367577688)),
    DenseVector(Array(-0.140779041464887, -0.226093367577688)),
    DenseVector(Array(3.150993255271550, 2.430503701460152)),
    DenseVector(Array(-0.931923697017461, -0.226093367577688)),
    DenseVector(Array(0.380715024092277, 1.102205166941232)),
    DenseVector(Array(-0.865782986263870, -1.554391902096608)),
    DenseVector(Array(-0.972625672865825, -0.226093367577688)),
    DenseVector(Array(0.773743478378042, 1.102205166941232)),
    DenseVector(Array(1.310500784878341, 1.102205166941232)),
    DenseVector(Array(-0.297227261132036, -0.226093367577688)),
    DenseVector(Array(-0.143322914955409, -1.554391902096608)),
    DenseVector(Array(-0.504552950609640, -0.226093367577688)),
    DenseVector(Array(-0.049199595806068, 1.102205166941232)),
    DenseVector(Array(2.403094449057862, -0.226093367577688)),
    DenseVector(Array(-1.145609070221372, -0.226093367577688)),
    DenseVector(Array(-0.690255715417800, -0.226093367577688)),
    DenseVector(Array(0.668172728521347, -0.226093367577688)),
    DenseVector(Array(0.253521349566139, -0.226093367577688)),
    DenseVector(Array(0.809357707245360, -0.226093367577688)),
    DenseVector(Array(-0.205647815473217, -1.554391902096608)),
    DenseVector(Array(-1.272802744747510, -2.882690436615528)),
    DenseVector(Array(0.050011470324320, 1.102205166941232)),
    DenseVector(Array(1.445326079876047, -0.226093367577688)),
    DenseVector(Array(-0.241262044340535, 1.102205166941232)),
    DenseVector(Array(-0.716966387068289, -0.226093367577688)),
    DenseVector(Array(-0.968809862630041, -0.226093367577688)),
    DenseVector(Array(0.167029650888366, 1.102205166941232)),
    DenseVector(Array(2.816473891267809, 1.102205166941232)),
    DenseVector(Array(0.205187753246207, 1.102205166941232)),
    DenseVector(Array(-0.428236745893957, -1.554391902096608)),
    DenseVector(Array(0.301854945886072, -0.226093367577688)),
    DenseVector(Array(0.720322135077064, 1.102205166941232)),
    DenseVector(Array(-1.018415395695235, -0.226093367577688)),
    DenseVector(Array(-1.461049383046193, -1.554391902096608)),
    DenseVector(Array(-0.189112637784819, 1.102205166941232)),
    DenseVector(Array(-1.014599585459451, -0.226093367577688))
  )
}
