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

package org.apache.flink.ml.regression

import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.jblas.DoubleMatrix

object RegressionData {

  val expectedWeights: DoubleMatrix = new DoubleMatrix(1, 1, 3.0094)
  val expectedWeight0: Double = 9.8158
  val expectedSquaredResidualSum: Double = 49.7596

  val data: Seq[LabeledVector] = Seq(
    LabeledVector(DenseVector(0.2714), 10.7949),
    LabeledVector(DenseVector(0.1008), 10.6426),
    LabeledVector(DenseVector(0.5078), 10.5603),
    LabeledVector(DenseVector(0.5856), 12.8707),
    LabeledVector(DenseVector(0.7629), 10.7026),
    LabeledVector(DenseVector(0.0830), 9.8571),
    LabeledVector(DenseVector(0.6616), 10.5001),
    LabeledVector(DenseVector(0.5170), 11.2063),
    LabeledVector(DenseVector(0.1710), 9.1892),
    LabeledVector(DenseVector(0.9386), 12.2408),
    LabeledVector(DenseVector(0.5905), 11.0307),
    LabeledVector(DenseVector(0.4406), 10.1369),
    LabeledVector(DenseVector(0.9419), 10.7609),
    LabeledVector(DenseVector(0.6559), 12.5328),
    LabeledVector(DenseVector(0.4519), 13.3560),
    LabeledVector(DenseVector(0.8397), 14.7424),
    LabeledVector(DenseVector(0.5326), 11.1057),
    LabeledVector(DenseVector(0.5539), 11.6157),
    LabeledVector(DenseVector(0.6801), 11.5744),
    LabeledVector(DenseVector(0.3672), 11.1775),
    LabeledVector(DenseVector(0.2393), 9.7991),
    LabeledVector(DenseVector(0.5789), 9.8173),
    LabeledVector(DenseVector(0.8669), 12.5642),
    LabeledVector(DenseVector(0.4068), 9.9952),
    LabeledVector(DenseVector(0.1126), 8.4354),
    LabeledVector(DenseVector(0.4438), 13.7058),
    LabeledVector(DenseVector(0.3002), 10.6672),
    LabeledVector(DenseVector(0.4014), 11.6080),
    LabeledVector(DenseVector(0.8334), 13.6926),
    LabeledVector(DenseVector(0.4036), 9.5261),
    LabeledVector(DenseVector(0.3902), 11.5837),
    LabeledVector(DenseVector(0.3604), 11.5831),
    LabeledVector(DenseVector(0.1403), 10.5038),
    LabeledVector(DenseVector(0.2601), 10.9382),
    LabeledVector(DenseVector(0.0868), 9.7325),
    LabeledVector(DenseVector(0.4294), 12.0113),
    LabeledVector(DenseVector(0.2573), 9.9219),
    LabeledVector(DenseVector(0.2976), 10.0963),
    LabeledVector(DenseVector(0.4249), 11.9999),
    LabeledVector(DenseVector(0.1192), 12.0442)
  )

  val expectedPolynomialWeights = Seq(0.2375, -0.3493, -0.1674)
  val expectedPolynomialWeight0 = 0.0233
  val expectedPolynomialSquaredResidualSum = 1.5389e+03

  val polynomialData: Seq[LabeledVector] = Seq(
    LabeledVector(DenseVector(3.6663), 2.1415),
    LabeledVector(DenseVector(4.0761), 10.9835),
    LabeledVector(DenseVector(0.5714), 7.2507),
    LabeledVector(DenseVector(4.1102), 11.9274),
    LabeledVector(DenseVector(2.8456), -4.2798),
    LabeledVector(DenseVector(0.4389), 7.1929),
    LabeledVector(DenseVector(1.2532), 4.5097),
    LabeledVector(DenseVector(2.4610), -3.6059),
    LabeledVector(DenseVector(4.3088), 18.1132),
    LabeledVector(DenseVector(4.3420), 19.2674),
    LabeledVector(DenseVector(0.7093), 7.0664),
    LabeledVector(DenseVector(4.3677), 20.1836),
    LabeledVector(DenseVector(4.3073), 18.0609),
    LabeledVector(DenseVector(2.1842), -2.2090),
    LabeledVector(DenseVector(3.6013), 1.1306),
    LabeledVector(DenseVector(0.6385), 7.1903),
    LabeledVector(DenseVector(1.8979), -0.2668),
    LabeledVector(DenseVector(4.1208), 12.2281),
    LabeledVector(DenseVector(3.5649), 0.6086),
    LabeledVector(DenseVector(4.3177), 18.4202),
    LabeledVector(DenseVector(2.9508), -4.1284),
    LabeledVector(DenseVector(0.1607), 6.1964),
    LabeledVector(DenseVector(3.8211), 4.9638),
    LabeledVector(DenseVector(4.2030), 14.6677),
    LabeledVector(DenseVector(3.0543), -3.8132),
    LabeledVector(DenseVector(3.4098), -1.2891),
    LabeledVector(DenseVector(3.3441), -1.9390),
    LabeledVector(DenseVector(1.7650), 0.7293),
    LabeledVector(DenseVector(2.9497), -4.1310),
    LabeledVector(DenseVector(0.7703), 6.9131),
    LabeledVector(DenseVector(3.1772), -3.2060),
    LabeledVector(DenseVector(0.1432), 6.0899),
    LabeledVector(DenseVector(1.2462), 4.5567),
    LabeledVector(DenseVector(0.2078), 6.4562),
    LabeledVector(DenseVector(0.4371), 7.1903),
    LabeledVector(DenseVector(3.7056), 2.8017),
    LabeledVector(DenseVector(3.1267), -3.4873),
    LabeledVector(DenseVector(1.4269), 3.2918),
    LabeledVector(DenseVector(4.2760), 17.0085),
    LabeledVector(DenseVector(0.1550), 6.1622),
    LabeledVector(DenseVector(1.9743), -0.8192),
    LabeledVector(DenseVector(1.7170), 1.0957),
    LabeledVector(DenseVector(3.4448), -0.9065),
    LabeledVector(DenseVector(3.5784), 0.7986),
    LabeledVector(DenseVector(0.8409), 6.6861),
    LabeledVector(DenseVector(2.2039), -2.3274),
    LabeledVector(DenseVector(2.0051), -1.0359),
    LabeledVector(DenseVector(2.9084), -4.2092),
    LabeledVector(DenseVector(3.1921), -3.1140),
    LabeledVector(DenseVector(3.3961), -1.4323)
  )
}
