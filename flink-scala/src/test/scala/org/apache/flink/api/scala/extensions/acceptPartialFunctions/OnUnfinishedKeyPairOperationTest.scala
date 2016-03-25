package org.apache.flink.api.scala.extensions.acceptPartialFunctions

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions.base.AcceptPartialFunctionsTestBase
import org.apache.flink.api.scala.extensions.acceptPartialFunctions.data.KeyValuePair
import org.junit.Test

class OnUnfinishedKeyPairOperationTest extends AcceptPartialFunctionsTestBase {

  @Test
  def testJoinWhereClauseOnTuple(): Unit = {
    val test =
      tuples.join(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for join on tuples should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testJoinWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for join on case objects should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testCoGroupWhereClauseOnTuple(): Unit = {
    val test =
      tuples.coGroup(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for coGroup on tuples should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testCoGroupWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.coGroup(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for coGroup on case objects should produce a HalfUnfinishedKeyPairOperation")
  }

}
