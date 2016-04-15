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

import org.apache.flink.api.java.operators.CoGroupOperator
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.api.scala.extensions.base.AcceptPFTestBase
import org.apache.flink.api.scala.extensions.data.KeyValuePair
import org.junit.Test

class OnCoGroupDataSetTest extends AcceptPFTestBase {

  @Test
  def testCoGroupProjectingOnTuple(): Unit = {
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
  def testCoGroupProjectingOnCaseClass(): Unit = {
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
