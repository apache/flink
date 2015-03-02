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

  val expectedBetas: DoubleMatrix = new DoubleMatrix(1, 1, 3.0094)
  val expectedBeta0: Double = 9.8158
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
}
