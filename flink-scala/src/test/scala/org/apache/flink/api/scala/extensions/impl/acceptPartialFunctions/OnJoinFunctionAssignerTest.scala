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

import org.apache.flink.api.java.operators.JoinOperator.EquiJoin
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.api.scala.extensions.base.AcceptPFTestBase
import org.apache.flink.api.scala.extensions.data.KeyValuePair
import org.junit.Test

class OnJoinFunctionAssignerTest extends AcceptPFTestBase {

  @Test
  def testInnerJoinProjectingOnTuple(): Unit = {
    val test =
      tuples.join(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }.projecting {
        case ((_, v1), (_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "projecting inner join on tuples should produce a EquiJoin")
  }

  @Test
  def testInnerJoinProjectingOnCaseClass(): Unit = {
    val test =
      caseObjects.join(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }.projecting {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "projecting inner join on case objects should produce a EquiJoin")
  }

  @Test
  def testRightOuterJoinProjectingOnTuple(): Unit = {
    val test =
      tuples.rightOuterJoin(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }.projecting {
        case ((_, v1), (_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "projecting right outer join on tuples should produce a EquiJoin")
  }

  @Test
  def testRightOuterJoinProjectingOnCaseClass(): Unit = {
    val test =
      caseObjects.rightOuterJoin(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }.projecting {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "projecting right outer join on case objects should produce a EquiJoin")
  }

  @Test
  def testLeftOuterJoinProjectingOnTuple(): Unit = {
    val test =
      tuples.leftOuterJoin(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }.projecting {
        case ((_, v1), (_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "projecting left outer join on tuples should produce a EquiJoin")
  }

  @Test
  def testLeftOuterJoinProjectingOnCaseClass(): Unit = {
    val test =
      caseObjects.leftOuterJoin(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }.projecting {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "projecting left outer join on case objects should produce a EquiJoin")
  }

  @Test
  def testFullOuterJoinProjectingOnTuple(): Unit = {
    val test =
      tuples.fullOuterJoin(tuples).whereClause {
        case (id, _) => id
      }.isEqualTo {
        case (id, _) => id
      }.projecting {
        case ((_, v1), (_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "projecting full outer join on tuples should produce a EquiJoin")
  }

  @Test
  def testFullOuterJoinProjectingOnCaseClass(): Unit = {
    val test =
      caseObjects.fullOuterJoin(caseObjects).whereClause {
        case KeyValuePair(id, _) => id
      }.isEqualTo {
        case KeyValuePair(id, _) => id
      }.projecting {
        case (KeyValuePair(_, v1), KeyValuePair(_, v2)) => s"$v1 $v2"
      }
    assert(test.javaSet.isInstanceOf[EquiJoin[_, _, _]],
      "projecting full outer join on case objects should produce a EquiJoin")
  }

}
