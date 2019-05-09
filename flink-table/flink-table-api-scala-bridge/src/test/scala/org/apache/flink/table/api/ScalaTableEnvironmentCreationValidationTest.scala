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

package org.apache.flink.table.api

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.junit.{Rule, Test}
import org.junit.rules.ExpectedException

/**
  * Test failures for the creation of scala [[TableEnvironment]]s.
  */
class ScalaTableEnvironmentCreationValidationTest {

  // used for accurate exception information checking.
  val expectedException = ExpectedException.none()

  @Rule
  def thrown = expectedException

  @Test def testBatchTableEnvironment(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Create BatchTableEnvironment failed.")
    BatchTableEnvironment.create(ExecutionEnvironment.getExecutionEnvironment)
  }

  @Test def testStreamTableEnvironment(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Create StreamTableEnvironment failed.")
    StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment)
  }
}
