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

package org.apache.flink.ml.outlier

import breeze.linalg.{sum, DenseVector => BreezeDenseVector}
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.outlier.StochasticOutlierSelection.BreezeLabeledVector
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class StochasticOutlierSelectionITSuite extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "Stochastic Outlier Selection algorithm"
  val EPSILON = 1e-16

  /*
    Unit-tests created based on the Python scripts of the algorithms author'
    https://github.com/jeroenjanssens/scikit-sos

    For more information about SOS, see https://github.com/jeroenjanssens/sos
    J.H.M. Janssens, F. Huszar, E.O. Postma, and H.J. van den Herik. Stochastic
    Outlier Selection. Technical Report TiCC TR 2012-001, Tilburg University,
    Tilburg, the Netherlands, 2012.
   */

  val perplexity = 3
  val errorTolerance = 0
  val maxIterations = 5000
  val parameters = new StochasticOutlierSelection().setPerplexity(perplexity).parameters

  it should "Compute the perplexity of the vector and return the correct error" in {
    val vector = BreezeDenseVector(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 9.0, 10.0))

    val output = Array(
      0.39682901665799636,
      0.15747326846175236,
      0.06248996227359784,
      0.024797830280027126,
      0.009840498605275054,
      0.0039049953849556816,
      6.149323865970302E-4,
      2.4402301428445443E-4,
      9.683541280042027E-5
    )

    val search = StochasticOutlierSelection.binarySearch(
      vector,
      Math.log(perplexity),
      maxIterations,
      errorTolerance
    ).toArray

    search should be(output)
  }

  it should "Compute the distance matrix and give symmetrical distances" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromCollection(List(
      BreezeLabeledVector(0, BreezeDenseVector(Array(1.0, 3.0))),
      BreezeLabeledVector(1, BreezeDenseVector(Array(5.0, 1.0)))
    ))

    val distanceMatrix = StochasticOutlierSelection
      .computeDissimilarityVectors(data)
      .map(_.data)
      .collect()
      .toArray

    distanceMatrix(0) should be(distanceMatrix(1))
  }

  it should "Compute the distance matrix and give the correct distances" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val expectedDistanceMatrix = Array(
      Array(Math.sqrt(2.0), Math.sqrt(10.0)),
      Array(Math.sqrt(2.0), Math.sqrt(16.0)),
      Array(Math.sqrt(16.0), Math.sqrt(10.0))
    )

    val data = env.fromCollection(Array(
      BreezeLabeledVector(0, BreezeDenseVector(Array(1.0, 1.0))),
      BreezeLabeledVector(1, BreezeDenseVector(Array(2.0, 2.0))),
      BreezeLabeledVector(2, BreezeDenseVector(Array(5.0, 1.0)))
    ))

    val distanceMatrix = StochasticOutlierSelection
      .computeDissimilarityVectors(data)
      .map(_.data.toArray)
      .collect()
      .sortBy(dist => sum(dist))
      .toArray

    distanceMatrix should be(expectedDistanceMatrix)
  }

  it should "Computing the affinity matrix and return the correct affinity" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromCollection(List(
      BreezeLabeledVector(0, BreezeDenseVector(Array(1.0, 1.0))),
      BreezeLabeledVector(1, BreezeDenseVector(Array(2.0, 1.0))),
      BreezeLabeledVector(2, BreezeDenseVector(Array(1.0, 2.0))),
      BreezeLabeledVector(3, BreezeDenseVector(Array(2.0, 2.0))),
      BreezeLabeledVector(4, BreezeDenseVector(Array(5.0, 8.0))) // The outlier!
    ))

    val distanceMatrix = StochasticOutlierSelection.computeDissimilarityVectors(data)


    val affinityMatrix = StochasticOutlierSelection.computeAffinity(distanceMatrix, parameters)
      .collect()
      .map(_.data.toArray)
      .sortBy(dist => sum(dist))
      .toArray

    val expectedAffinityMatrix = Array(
      Array(
        1.6502458086204375E-6, 3.4496775759599478E-6, 6.730049701933432E-6, 1.544221669904019E-5),
      Array(0.2837044890495805, 0.4103155587026411, 0.4103155587026411, 0.0025393148189994897),
      Array(0.43192525601205634, 0.30506325262816036, 0.43192525601205634, 0.0023490595181415333),
      Array(0.44804626736879755, 0.3212891538762665, 0.44804626736879755, 0.0022108233460722557),
      Array(0.46466276524577704, 0.46466276524577704, 0.3382687394674377, 0.002071952211368232)
    )

    affinityMatrix should be(expectedAffinityMatrix)
  }

  it should "Compute the binding probabilities and return the correct probabilities" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val expectedBindingProbabilityMatrix = Array(
      Array(0.00000000000000000, 0.3659685430819966, 0.36596854308199660,
        0.2664300527549236, 0.0016328610810832176),
      Array(0.06050907527090226, 0.1264893287483121, 0.24677254025174370,
        0.5662290557290419, 0.0000000000000000000),
      Array(0.25630819225892230, 0.3706990977807361, 0.37069909778073610,
        0.0000000000000000, 0.0022936121796053232),
      Array(0.36737364041784460, 0.0000000000000000, 0.26343993596023335,
        0.3673736404178446, 0.0018127832040774768),
      Array(0.36877315905154990, 0.2604492865700658, 0.00000000000000000,
        0.3687731590515499, 0.0020043953268345785)
    )

    // The distance matrix
    val data = env.fromCollection(List(
      BreezeLabeledVector(0, new BreezeDenseVector(
        Array(0.00000000e+00, 4.64702705e-01, 4.64702705e-01, 3.38309859e-01, 2.07338848e-03))),
      BreezeLabeledVector(1, new BreezeDenseVector(
        Array(4.48047312e-01, 0.00000000e+00, 3.21290213e-01, 4.48047312e-01, 2.21086260e-03))),
      BreezeLabeledVector(2, new BreezeDenseVector(
        Array(4.31883411e-01, 3.05021457e-01, 0.00000000e+00, 4.31883411e-01, 2.34741892e-03))),
      BreezeLabeledVector(3, new BreezeDenseVector(
        Array(2.83688288e-01, 4.10298990e-01, 4.10298990e-01, 0.00000000e+00, 2.53862706e-03))),
      BreezeLabeledVector(4, new BreezeDenseVector(
        Array(1.65000529e-06, 3.44920263e-06, 6.72917236e-06, 1.54403440e-05, 0.00000000e+00)))
    ))

    val bindingProbabilityMatrix = StochasticOutlierSelection.computeBindingProbabilities(data)
      .map(_.data.toArray)
      .collect()
      .sortBy(_ (0)) // Sort by the first element, because the sum is always equal to 1
      .toArray

    bindingProbabilityMatrix should be(expectedBindingProbabilityMatrix)
  }


  it should "Compute the product of the vector, should return the correct values" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromCollection(List(
      BreezeLabeledVector(0, BreezeDenseVector(0.5, 0.3)),
      BreezeLabeledVector(1, BreezeDenseVector(0.25, 0.1)),
      BreezeLabeledVector(2, BreezeDenseVector(0.8, 0.8))
    ))

    val outlierMatrix = StochasticOutlierSelection.computeOutlierProbability(data)
      .map(_._2)
      .collect()
      .sortBy(dist => dist)
      .toArray

    // The math by hand
    val expectedOutlierMatrix = Array(
      (1.0 - 0.5) * (1.0 - 0.0) * (1.0 - 0.8),
      (1.0 - 0.0) * (1.0 - 0.25) * (1.0 - 0.8),
      (1.0 - 0.3) * (1.0 - 0.1) * (1.0 - 0)
    )

    outlierMatrix should be(expectedOutlierMatrix)
  }

  it should "Verifying the output of the SOS algorithm assign the one true outlier" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromCollection(List(
      LabeledVector(0.0, DenseVector(1.0, 1.0)),
      LabeledVector(1.0, DenseVector(2.0, 1.0)),
      LabeledVector(2.0, DenseVector(1.0, 2.0)),
      LabeledVector(3.0, DenseVector(2.0, 2.0)),
      LabeledVector(4.0, DenseVector(5.0, 8.0)) // The outlier!
    ))

    val sos = new StochasticOutlierSelection().setPerplexity(3)

    val outputVector = sos
      .transform(data)
      .collect()

    val expectedOutputVector = Map(
      0 -> 0.2790094479202896,
      1 -> 0.25775014551682535,
      2 -> 0.22136130977995766,
      3 -> 0.12707053787018444,
      4 -> 0.9922779902453757 // The outlier!
    )

    outputVector.foreach(output =>
      expectedOutputVector(output._1) should be(output._2 +- EPSILON))
  }
}
