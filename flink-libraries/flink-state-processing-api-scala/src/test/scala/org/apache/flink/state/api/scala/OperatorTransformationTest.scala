package org.apache.flink.state.api.scala

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.TestLogger
import org.junit.Test

class OperatorTransformationTest extends TestLogger {

  @Test
  @throws[Exception]
  def testScalaDatasetBootstrapWith() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements(1, 2, 3)

    val transformation = OperatorTransformation
      .bootstrapWith[Int](data)
  }
}
