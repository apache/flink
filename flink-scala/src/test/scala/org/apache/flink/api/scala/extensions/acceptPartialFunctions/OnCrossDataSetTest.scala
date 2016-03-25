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

import org.apache.flink.api.java.operators.CrossOperator
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.api.scala.extensions.base.AcceptPFTestBase
import org.apache.flink.api.scala.extensions.data.KeyValuePair
import org.junit.Test

class OnCrossDataSetTest extends AcceptPFTestBase {

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
