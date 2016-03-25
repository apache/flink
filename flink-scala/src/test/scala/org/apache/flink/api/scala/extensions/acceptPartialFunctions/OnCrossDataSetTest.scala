package org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.api.java.operators.CrossOperator
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions.base.AcceptPartialFunctionsTestBase
import org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions.data.KeyValuePair
import org.junit.Test

class OnCrossDataSetTest extends AcceptPartialFunctionsTestBase {

  @Test
  def testCrossProjectingOnTuple(): Unit = {
    val test =
      tuples.cross(tuples).projecting {
        case ((_, v1), (_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[CrossOperator[_, _, _]],
      "projecting for cross on tuples should produce a CrossOperator")
  }

  @Test
  def testCrossProjectingOnCaseClass(): Unit = {
    val test =
      caseObjects.cross(caseObjects).projecting {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[CrossOperator[_, _, _]],
      "projecting for cross on case objects should produce a CrossOperator")
  }

}
