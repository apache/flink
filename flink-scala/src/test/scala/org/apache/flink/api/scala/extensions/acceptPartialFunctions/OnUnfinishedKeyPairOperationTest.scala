package org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions.base.AcceptPartialFunctionsTestBase
import org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions.data.KeyValuePair
import org.junit.Test

class OnUnfinishedKeyPairOperationTest extends AcceptPartialFunctionsTestBase {

  @Test
  def testInnerJoinWhereClauseOnTuple(): Unit = {
    val test =
      tuples.join(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for inner join on tuples should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testInnerJoinWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for inner join on case objects should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testRightOuterJoinWhereClauseOnTuple(): Unit = {
    val test =
      tuples.join(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for right outer join on tuples should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testRightOuterJoinWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for right outer join on case objects should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testLeftOuterJoinWhereClauseOnTuple(): Unit = {
    val test =
      tuples.join(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for left outer join on tuples should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testLeftOuterJoinWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for left outer join on case objects should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testFullOuterJoinWhereClauseOnTuple(): Unit = {
    val test =
      tuples.fullOuterJoin(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for full outer join on tuples should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testFullOuterJoinWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.fullOuterJoin(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for full outer join on case objects should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testCoGroupWhereClauseOnTuple(): Unit = {
    val test =
      tuples.coGroup(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for co-group on tuples should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testCoGroupWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.coGroup(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for co-group on case objects should produce a HalfUnfinishedKeyPairOperation")
  }

}
