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
package org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.api.scala.extensions.base.AcceptPFTestBase
import org.apache.flink.api.scala.extensions.data.KeyValuePair
import org.junit.Test

class OnUnfinishedKeyPairOperationTest extends AcceptPFTestBase {

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
      "whereClause for inner join on case objects " +
        "should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testRightOuterJoinWhereClauseOnTuple(): Unit = {
    val test =
      tuples.join(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for right outer join on tuples " +
        "should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testRightOuterJoinWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for right outer join on case objects " +
        "should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testLeftOuterJoinWhereClauseOnTuple(): Unit = {
    val test =
      tuples.join(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for left outer join on tuples " +
        "should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testLeftOuterJoinWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for left outer join on case objects " +
        "should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testFullOuterJoinWhereClauseOnTuple(): Unit = {
    val test =
      tuples.fullOuterJoin(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for full outer join on tuples " +
        "should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testFullOuterJoinWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.fullOuterJoin(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for full outer join on case objects " +
        "should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testCoGroupWhereClauseOnTuple(): Unit = {
    val test =
      tuples.coGroup(tuples).whereClause {
        case (id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for co-group on tuples " +
        "should produce a HalfUnfinishedKeyPairOperation")
  }

  @Test
  def testCoGroupWhereClauseOnCaseClass(): Unit = {
    val test =
      caseObjects.coGroup(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }
    assert(test.isInstanceOf[HalfUnfinishedKeyPairOperation[_, _, _]],
      "whereClause for co-group on case objects " +
        "should produce a HalfUnfinishedKeyPairOperation")
  }

}
