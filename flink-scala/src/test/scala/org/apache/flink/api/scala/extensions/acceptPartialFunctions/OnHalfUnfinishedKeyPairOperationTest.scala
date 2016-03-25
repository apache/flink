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
  def testInnerJoinIsEqualToOnTuple(): Unit = {
    val test =
      tuples.join(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "isEqualTo for inner join on tuples should produce a EquiJoin")
  }

  @Test
  def testInnerJoinIsEqualToOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "isEqualTo for inner join on case objects should produce a EquiJoin")
  }

  @Test
  def testRightOuterJoinIsEqualToOnTuple(): Unit = {
    val test =
      tuples.rightOuterJoin(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }
    assert(test.isInstanceOf[JoinFunctionAssigner[_, _]],
      "isEqualTo for right outer join on tuples should produce a JoinFunctionAssigner")
  }

  @Test
  def testRightOuterJoinIsEqualToOnCaseClass(): Unit = {
    val test =
      caseObjects.rightOuterJoin(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[JoinFunctionAssigner[_, _]],
      "isEqualTo for right outer join on case objects should produce a JoinFunctionAssigner")
  }

  @Test
  def testLeftOuterJoinIsEqualToOnTuple(): Unit = {
    val test =
      tuples.leftOuterJoin(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }
    assert(test.isInstanceOf[JoinFunctionAssigner[_, _]],
      "isEqualTo for left outer join on tuples should produce a JoinFunctionAssigner")
  }

  @Test
  def testLeftOuterJoinIsEqualToOnCaseClass(): Unit = {
    val test =
      caseObjects.leftOuterJoin(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[JoinFunctionAssigner[_, _]],
      "isEqualTo for left outer join on case objects should produce a JoinFunctionAssigner")
  }

  @Test
  def testFullOuterJoinIsEqualToOnTuple(): Unit = {
    val test =
      tuples.fullOuterJoin(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }
    assert(test.isInstanceOf[JoinFunctionAssigner[_, _]],
      "isEqualTo for full outer join on tuples should produce a JoinFunctionAssigner")
  }

  @Test
  def testFullOuterJoinIsEqualToOnCaseClass(): Unit = {
    val test =
      caseObjects.fullOuterJoin(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[JoinFunctionAssigner[_, _]],
      "isEqualTo for full outer join on case objects should produce a JoinFunctionAssigner")
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
      "isEqualTo for co-group on tuples should produce a CoGroupOperator")
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
      "isEqualTo for co-group on case objects should produce a CoGroupOperator")
  }

}
