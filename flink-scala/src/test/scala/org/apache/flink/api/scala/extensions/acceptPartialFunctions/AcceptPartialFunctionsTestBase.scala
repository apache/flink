package org.apache.flink.api.scala.extensions.acceptPartialFunctions

import org.apache.flink.api.scala._
import org.apache.flink.util.TestLogger
import org.scalatest.junit.JUnitSuiteLike

abstract class AcceptPartialFunctionsTestBase extends TestLogger with JUnitSuiteLike {

  private val env = ExecutionEnvironment.getExecutionEnvironment

  protected val tuples = env.fromElements(1 -> "hello", 2 -> "world")
  protected val caseObjects = env.fromElements(KeyValuePair(1, "hello"), KeyValuePair(2, "world"))

  protected val groupedTuples = tuples.groupBy(_._1)
  protected val groupedCaseObjects = caseObjects.groupBy(_.id)

}
