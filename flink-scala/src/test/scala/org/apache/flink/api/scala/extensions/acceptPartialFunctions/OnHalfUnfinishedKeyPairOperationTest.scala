package org.apache.flink.api.scala.extensions.acceptPartialFunctions

import org.apache.flink.api.java.operators.CoGroupOperator
import org.apache.flink.api.java.operators.JoinOperator.EquiJoin
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions.base.AcceptPartialFunctionsTestBase
import org.apache.flink.api.scala.extensions.acceptPartialFunctions.data.KeyValuePair
import org.junit.Test

class OnHalfUnfinishedKeyPairOperationTest extends AcceptPartialFunctionsTestBase {

  @Test
  def testJoinIsEqualToOnTuple(): Unit = {
    val test =
      tuples.join(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "isEqualTo for join on tuples should produce a EquiJoin")
  }

  @Test
  def testJoinIsEqualToOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "isEqualTo for join on case objects should produce a EquiJoin")
  }

  @Test
  def testCoGroupIsEqualToOnTuple(): Unit = {
    val test =
      tuples.coGroup(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }
    assert(test.javaSet.isInstanceOf[CoGroupOperator[_, _, _]],
      "isEqualTo for coGroup on tuples should produce a CoGroupOperator")
  }

  @Test
  def testCoGroupIsEqualToOnCaseClass(): Unit = {
    val test =
      caseObjects.coGroup(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }
    assert(test.javaSet.isInstanceOf[CoGroupOperator[_, _, _]],
      "isEqualTo for coGroup on case objects should produce a CoGroupOperator")
  }

}
