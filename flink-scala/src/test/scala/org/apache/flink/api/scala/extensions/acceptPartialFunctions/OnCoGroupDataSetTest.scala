package org.apache.flink.api.scala.extensions.acceptPartialFunctions

import org.apache.flink.api.java.operators.CoGroupOperator
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions.base.AcceptPartialFunctionsTestBase
import org.apache.flink.api.scala.extensions.acceptPartialFunctions.data.KeyValuePair
import org.apache.flink.api.scala.extensions._
import org.junit.Test

class OnCoGroupDataSetTest extends AcceptPartialFunctionsTestBase {

  @Test
  def testProjectingOnTuple(): Unit = {
    val test =
      tuples.coGroup(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }.projecting {
        case ((_, v1) #:: _, (_, v2) #:: _) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[CoGroupOperator[_, _, _]],
      "projecting on tuples should produce a CoGroupOperator")
  }

  @Test
  def testProjectingOnCaseClass(): Unit = {
    val test =
      caseObjects.coGroup(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }.projecting {
        case (KeyValuePair(_, v1) #:: _, KeyValuePair(_, v2) #:: _) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[CoGroupOperator[_, _, _]],
      "projecting on case objects should produce a CoGroupOperator")
  }

}
