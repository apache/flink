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
package org.apache.flink.ml.preprocess

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.math.DenseMatrix
import org.scalatest.FlatSpec

class ScalerSuite  extends FlatSpec{
  behavior of "Flink's Stnadard Scaler"

  it should "contain the scaled data" in {
    import ScalerData._

    val env = ExecutionEnvironment.getExecutionEnvironment

    val numRows = 47
    val numCols = 2

    val matrix = env.fromCollection(data)
    matrix.print()
    val scaler = new Scaler

    val scaledMatrix = scaler.transform(matrix,new ParameterMap)

    println(scaledMatrix.toString)

  }
}

object ScalerData {

  val data : Seq[DenseMatrix]= List(DenseMatrix(47,2,
    Array(2.1040,
      1.6000,
      2.4000,
      1.4160,
      3.0000,
      1.9850,
      1.5340,
      1.4270,
      1.3800,
      1.4940,
      1.9400,
      2.0000,
      1.8900,
      4.4780,
      1.2680,
      2.3000,
      1.3200,
      1.2360,
      2.6090,
      3.0310,
      1.7670,
      1.8880,
      1.6040,
      1.9620,
      3.8900,
      1.1000,
      1.4580,
      2.5260,
      2.2000,
      2.6370,
      1.8390,
      1.0000,
      2.0400,
      3.1370,
      1.8110,
      1.4370,
      1.2390,
      2.1320,
      4.2150,
      2.1620,
      1.6640,
      2.2380,
      2.5670,
      1.2000,
      8.5200,
      1.8520,
      1.2030,
      3.0000,
      3.0000,
      3.0000,
      2.0000,
      4.0000,
      4.0000,
      3.0000,
      3.0000,
      3.0000,
      3.0000,
      4.0000,
      3.0000,
      3.0000,
      5.0000,
      3.0000,
      4.0000,
      2.0000,
      3.0000,
      4.0000,
      4.0000,
      3.0000,
      2.0000,
      3.0000,
      4.0000,
      3.0000,
      3.0000,
      3.0000,
      3.0000,
      3.0000,
      3.0000,
      2.0000,
      1.0000,
      4.0000,
      3.0000,
      4.0000,
      3.0000,
      3.0000,
      4.0000,
      4.0000,
      4.0000,
      2.0000,
      3.0000,
      4.0000,
      3.0000,
      2.0000,
      4.0000,
      3.0000)))
}

