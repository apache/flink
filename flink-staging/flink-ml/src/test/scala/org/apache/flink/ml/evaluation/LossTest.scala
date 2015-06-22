package org.apache.flink.ml.evaluation

import org.apache.flink.api.scala._
import org.apache.flink.ml.data.ToyData
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.regression.SimpleLeastSquaresRegression
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}


class LossTest
  extends FlatSpec
   with Matchers
   with FlinkTestBase {

  behavior of "Loss Functions"

  it should "work for squared loss" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val yy = env.fromCollection(Seq((0.0, 1.0), (0.0, 0.0), (3.0, 5.0)))

    val loss = Loss.squaredLoss

    val result = loss.loss(yy).collect()

    result.length shouldBe 1
    result.head shouldBe (1.6666666666 +- 1e-4)
  }

  it should "work for zero one loss" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val yy = env.fromCollection(Seq("a" -> "a", "a" -> "b", "b" -> "c", "d" -> "d"))

    val loss = Loss.zeroOneLoss[String]

    val result = loss.loss(yy).collect()

    result.length shouldBe 1
    result.head shouldBe (0.5 +- 1e9)
  }

  it should "work for zero one loss applied to signs" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val yy = env.fromCollection(Seq[(Double,Double)](-2.3 -> 2.3, -1.0 -> -10.5, 2.0 -> 3.0, 4.0 -> -5.0))

    val loss = Loss.zeroOneSignumLoss

    val result = loss.loss(yy).collect()

    result.length shouldBe 1
    result.head shouldBe (0.5 +- 1e9)
  }

  it should "work with a slightly more involved case with linear regression" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
        val center = DenseVector(1.0, -2.0, 3.0, -4.0, 5.0)
        val weights = DenseVector(2.0, 1.0, 0.0, -1.0, -2.0)
        val n = 1000
        val noise = 0.5
        val ds = env.fromCollection(ToyData.singleGaussianLinearProblem(n, center, weights, noise))

        val slr = new SimpleLeastSquaresRegression
        slr.fit(ds)

        val test = ds.map(x => (x.vector, x.label))

        val labels = slr.evaluate(test)

        val error = Loss.squaredLoss.loss(labels)
        val expectedError = noise*noise

        error.collect().head shouldBe (expectedError +- expectedError/5)
  }
}
