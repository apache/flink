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
import org.apache.flink.ml.math.{SparseVector, DenseVector}

object RegressionData {

  val expectedWeights = Array[Double](3.0094)
  val expectedWeight0: Double = 9.8158
  val expectedSquaredResidualSum: Double = 49.7596/2

  val sparseData: Seq[LabeledVector] = Seq(
    new LabeledVector(1.0, new SparseVector(10, Array(0, 2, 3), Array(1.0, 1.0, 1.0))),
    new LabeledVector(1.0, new SparseVector(10, Array(0, 1, 5, 9), Array(1.0, 1.0, 1.0, 1.0))),
    new LabeledVector(0.0, new SparseVector(10, Array(0, 2), Array(0.0, 1.0))),
    new LabeledVector(0.0, new SparseVector(10, Array(0), Array(0.0))),
    new LabeledVector(0.0, new SparseVector(10, Array(0, 2), Array(0.0, 1.0))),
    new LabeledVector(0.0, new SparseVector(10, Array(0), Array(0.0))))

  val expectedWeightsSparseInput = Array(0.5448906338353784, 0.15718880164669916,
                                         0.034001300318125725, 0.38770183218867915, 0.0,
                                         0.15718880164669916, 0.0, 0.0, 0.0, 0.15718880164669916)

  val expectedInterceptSparseInput = -0.006918274867886108


  val data: Seq[LabeledVector] = Seq(
    LabeledVector(10.7949, DenseVector(0.2714)),
    LabeledVector(10.6426, DenseVector(0.1008)),
    LabeledVector(10.5603, DenseVector(0.5078)),
    LabeledVector(12.8707, DenseVector(0.5856)),
    LabeledVector(10.7026, DenseVector(0.7629)),
    LabeledVector(9.8571, DenseVector(0.0830)),
    LabeledVector(10.5001, DenseVector(0.6616)),
    LabeledVector(11.2063, DenseVector(0.5170)),
    LabeledVector(9.1892, DenseVector(0.1710)),
    LabeledVector(12.2408, DenseVector(0.9386)),
    LabeledVector(11.0307, DenseVector(0.5905)),
    LabeledVector(10.1369, DenseVector(0.4406)),
    LabeledVector(10.7609, DenseVector(0.9419)),
    LabeledVector(12.5328, DenseVector(0.6559)),
    LabeledVector(13.3560, DenseVector(0.4519)),
    LabeledVector(14.7424, DenseVector(0.8397)),
    LabeledVector(11.1057, DenseVector(0.5326)),
    LabeledVector(11.6157, DenseVector(0.5539)),
    LabeledVector(11.5744, DenseVector(0.6801)),
    LabeledVector(11.1775, DenseVector(0.3672)),
    LabeledVector(9.7991, DenseVector(0.2393)),
    LabeledVector(9.8173, DenseVector(0.5789)),
    LabeledVector(12.5642, DenseVector(0.8669)),
    LabeledVector(9.9952, DenseVector(0.4068)),
    LabeledVector(8.4354, DenseVector(0.1126)),
    LabeledVector(13.7058, DenseVector(0.4438)),
    LabeledVector(10.6672, DenseVector(0.3002)),
    LabeledVector(11.6080, DenseVector(0.4014)),
    LabeledVector(13.6926, DenseVector(0.8334)),
    LabeledVector(9.5261, DenseVector(0.4036)),
    LabeledVector(11.5837, DenseVector(0.3902)),
    LabeledVector(11.5831, DenseVector(0.3604)),
    LabeledVector(10.5038, DenseVector(0.1403)),
    LabeledVector(10.9382, DenseVector(0.2601)),
    LabeledVector(9.7325, DenseVector(0.0868)),
    LabeledVector(12.0113, DenseVector(0.4294)),
    LabeledVector(9.9219, DenseVector(0.2573)),
    LabeledVector(10.0963, DenseVector(0.2976)),
    LabeledVector(11.9999, DenseVector(0.4249)),
    LabeledVector(12.0442, DenseVector(0.1192))
  )

  val expectedNoInterceptWeights = Array[Double](5.0)
  val expectedNoInterceptWeight0: Double = 0.0

  val noInterceptData: Seq[LabeledVector] = Seq(
    LabeledVector(217.228709, DenseVector(43.4457419)),
    LabeledVector(450.037048, DenseVector(90.0074095)),
    LabeledVector( 67.553478, DenseVector(13.5106955)),
    LabeledVector( 26.976958, DenseVector( 5.3953916)),
    LabeledVector(403.808709, DenseVector(80.7617418)),
    LabeledVector(203.932158, DenseVector(40.7864316)),
    LabeledVector(146.974958, DenseVector(29.3949916)),
    LabeledVector( 46.869291, DenseVector( 9.3738582)),
    LabeledVector(450.780834, DenseVector(90.1561667)),
    LabeledVector(386.535619, DenseVector(77.3071239)),
    LabeledVector(202.644342, DenseVector(40.5288684)),
    LabeledVector(227.586507, DenseVector(45.5173013)),
    LabeledVector(408.801080, DenseVector(81.7602161)),
    LabeledVector(146.118550, DenseVector(29.2237100)),
    LabeledVector(156.475382, DenseVector(31.2950763)),
    LabeledVector(291.822515, DenseVector(58.3645030)),
    LabeledVector( 61.506887, DenseVector(12.3013775)),
    LabeledVector(363.949913, DenseVector(72.7899827)),
    LabeledVector(398.050744, DenseVector(79.6101487)),
    LabeledVector(246.053111, DenseVector(49.2106221)),
    LabeledVector(225.494661, DenseVector(45.0989323)),
    LabeledVector(265.986844, DenseVector(53.1973689)),
    LabeledVector(110.459912, DenseVector(22.0919823)),
    LabeledVector(122.716974, DenseVector(24.5433947)),
    LabeledVector(128.014314, DenseVector(25.6028628)),
    LabeledVector(252.538913, DenseVector(50.5077825)),
    LabeledVector(393.632082, DenseVector(78.7264163)),
    LabeledVector( 77.698941, DenseVector(15.5397881)),
    LabeledVector(206.187568, DenseVector(41.2375135)),
    LabeledVector(244.073426, DenseVector(48.8146851)),
    LabeledVector(364.946890, DenseVector(72.9893780)),
    LabeledVector(  4.627494, DenseVector( 0.9254987)),
    LabeledVector(485.359565, DenseVector(97.0719130)),
    LabeledVector(347.359190, DenseVector(69.4718380)),
    LabeledVector(419.663211, DenseVector(83.9326422)),
    LabeledVector(488.518318, DenseVector(97.7036635)),
    LabeledVector( 28.082962, DenseVector( 5.6165925)),
    LabeledVector(211.002441, DenseVector(42.2004881)),
    LabeledVector(250.624124, DenseVector(50.1248248)),
    LabeledVector(489.776669, DenseVector(97.9553337))
  )


  val expectedPolynomialWeights = Seq(0.2375, -0.3493, -0.1674)
  val expectedPolynomialWeight0 = 0.0233
  val expectedPolynomialSquaredResidualSum = 1.5389e+03/2

  val polynomialData: Seq[LabeledVector] = Seq(
    LabeledVector(2.1415, DenseVector(3.6663)),
    LabeledVector(10.9835, DenseVector(4.0761)),
    LabeledVector(7.2507, DenseVector(0.5714)),
    LabeledVector(11.9274, DenseVector(4.1102)),
    LabeledVector(-4.2798, DenseVector(2.8456)),
    LabeledVector(7.1929, DenseVector(0.4389)),
    LabeledVector(4.5097, DenseVector(1.2532)),
    LabeledVector(-3.6059, DenseVector(2.4610)),
    LabeledVector(18.1132, DenseVector(4.3088)),
    LabeledVector(19.2674, DenseVector(4.3420)),
    LabeledVector(7.0664, DenseVector(0.7093)),
    LabeledVector(20.1836, DenseVector(4.3677)),
    LabeledVector(18.0609, DenseVector(4.3073)),
    LabeledVector(-2.2090, DenseVector(2.1842)),
    LabeledVector(1.1306, DenseVector(3.6013)),
    LabeledVector(7.1903, DenseVector(0.6385)),
    LabeledVector(-0.2668, DenseVector(1.8979)),
    LabeledVector(12.2281, DenseVector(4.1208)),
    LabeledVector(0.6086, DenseVector(3.5649)),
    LabeledVector(18.4202, DenseVector(4.3177)),
    LabeledVector(-4.1284, DenseVector(2.9508)),
    LabeledVector(6.1964, DenseVector(0.1607)),
    LabeledVector(4.9638, DenseVector(3.8211)),
    LabeledVector(14.6677, DenseVector(4.2030)),
    LabeledVector(-3.8132, DenseVector(3.0543)),
    LabeledVector(-1.2891, DenseVector(3.4098)),
    LabeledVector(-1.9390, DenseVector(3.3441)),
    LabeledVector(0.7293, DenseVector(1.7650)),
    LabeledVector(-4.1310, DenseVector(2.9497)),
    LabeledVector(6.9131, DenseVector(0.7703)),
    LabeledVector(-3.2060, DenseVector(3.1772)),
    LabeledVector(6.0899, DenseVector(0.1432)),
    LabeledVector(4.5567, DenseVector(1.2462)),
    LabeledVector(6.4562, DenseVector(0.2078)),
    LabeledVector(7.1903, DenseVector(0.4371)),
    LabeledVector(2.8017, DenseVector(3.7056)),
    LabeledVector(-3.4873, DenseVector(3.1267)),
    LabeledVector(3.2918, DenseVector(1.4269)),
    LabeledVector(17.0085, DenseVector(4.2760)),
    LabeledVector(6.1622, DenseVector(0.1550)),
    LabeledVector(-0.8192, DenseVector(1.9743)),
    LabeledVector(1.0957, DenseVector(1.7170)),
    LabeledVector(-0.9065, DenseVector(3.4448)),
    LabeledVector(0.7986, DenseVector(3.5784)),
    LabeledVector(6.6861, DenseVector(0.8409)),
    LabeledVector(-2.3274, DenseVector(2.2039)),
    LabeledVector(-1.0359, DenseVector(2.0051)),
    LabeledVector(-4.2092, DenseVector(2.9084)),
    LabeledVector(-3.1140, DenseVector(3.1921)),
    LabeledVector(-1.4323, DenseVector(3.3961))
  )

  val expectedRegWeights = Array[Double](0.0, 0.0, 0.0, 0.18, 0.2, 0.24)
  val expectedRegWeight0 = 0.74

  // Example values from scikit-learn L1 test: http://git.io/vf4V2
  val regularizationData: Seq[LabeledVector] = Seq(
    LabeledVector(1.0, DenseVector(1.0,0.9 ,0.8 ,0.0 ,0.0 ,0.0)),
    LabeledVector(1.0, DenseVector(1.0,0.84,0.98,0.0 ,0.0 ,0.0)),
    LabeledVector(1.0, DenseVector(1.0,0.96,0.88,0.0 ,0.0 ,0.0)),
    LabeledVector(1.0, DenseVector(1.0,0.91,0.99,0.0 ,0.0 ,0.0)),
    LabeledVector(2.0, DenseVector(0.0,0.0 ,0.0 ,0.89,0.91,1.0)),
    LabeledVector(2.0, DenseVector(0.0,0.0 ,0.0 ,0.79,0.84,1.0)),
    LabeledVector(2.0, DenseVector(0.0,0.0 ,0.0 ,0.91,0.95,1.0)),
    LabeledVector(2.0, DenseVector(0.0,0.0 ,0.0 ,0.93,1.0 ,1.0))
  )
}
