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

package org.apache.flink.table.planner.runtime.batch.sql.join

import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._

import org.junit.{Before, Test}

import scala.collection.Seq

class ScalarQueryITCase extends BatchTestBase {

  lazy val l = Seq(
    row(1, 2.0),
    row(1, 2.0),
    row(2, 1.0),
    row(2, 1.0),
    row(3, 3.0),
    row(null, null),
    row(null, 5.0),
    row(6, null)
  )

  lazy val r = Seq(
    row(2, 3.0),
    row(2, 3.0),
    row(3, 2.0),
    row(4, 1.0),
    row(null, null),
    row(null, 5.0),
    row(6, null)
  )

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("l", l, INT_DOUBLE, "a, b")
    registerCollection("r", r, INT_DOUBLE, "c, d")
  }

  @Test
  def testScalarSubQuery(): Unit = {
    checkResult(
      "SELECT * FROM l WHERE a = (SELECT c FROM r where c = 3)",
      Seq(row(3, 3.0)))
  }

  @Test(expected = classOf[JobExecutionException])
  def testScalarSubQueryException(): Unit = {
    checkResult(
      "SELECT * FROM l WHERE a = (SELECT c FROM r)",
      Seq(row(3, 3.0)))
  }
}

