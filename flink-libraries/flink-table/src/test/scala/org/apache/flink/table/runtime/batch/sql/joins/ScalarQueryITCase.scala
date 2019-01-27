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

package org.apache.flink.table.runtime.batch.sql.joins

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.junit.{Before, Test}

import scala.collection.Seq

class ScalarQueryITCase extends BatchTestBase with JoinITCaseBase {

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
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("l", l, INT_DOUBLE, "a, b", Seq(true, true))
    registerCollection("r", r, INT_DOUBLE, "c, d", Seq(true, true))
  }

  @Test
  def testRewriteScalarQueryWithoutCorrelation(): Unit = {
    Seq(
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r) > 0",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r) > 0.9",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r) >= 1",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r) >= 0.1",
      "SELECT * FROM l WHERE 0 < (SELECT COUNT(*) FROM r)",
      "SELECT * FROM l WHERE 0.99 < (SELECT COUNT(*) FROM r)",
      "SELECT * FROM l WHERE 1 <= (SELECT COUNT(*) FROM r)",
      "SELECT * FROM l WHERE 0.01 <= (SELECT COUNT(*) FROM r)"
    ).foreach(checkResult(_, l))

    Seq(
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE c > 5) > 0",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE c > 5) > 0.9",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE c > 5) >= 1",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE c > 5) >= 0.1",
      "SELECT * FROM l WHERE 0 < (SELECT COUNT(*) FROM r WHERE c > 5)",
      "SELECT * FROM l WHERE 0.99 < (SELECT COUNT(*) FROM r WHERE c > 5)",
      "SELECT * FROM l WHERE 1 <= (SELECT COUNT(*) FROM r WHERE c > 5)",
      "SELECT * FROM l WHERE 0.01 <= (SELECT COUNT(*) FROM r WHERE c > 5)"
    ).foreach(checkResult(_, l))

    Seq(
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE c > 15) > 0",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE c > 15) > 0.9",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE c > 15) >= 1",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE c > 15) >= 0.1",
      "SELECT * FROM l WHERE 0 < (SELECT COUNT(*) FROM r WHERE c > 15)",
      "SELECT * FROM l WHERE 0.99 < (SELECT COUNT(*) FROM r WHERE c > 15)",
      "SELECT * FROM l WHERE 1 <= (SELECT COUNT(*) FROM r WHERE c > 15)",
      "SELECT * FROM l WHERE 0.01 <= (SELECT COUNT(*) FROM r WHERE c > 15)"
    ).foreach(checkResult(_, Seq.empty))
  }

  @Test
  def testRewriteScalarQueryWithCorrelation(): Unit = {
    Seq(
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c) > 0",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c) > 0.9",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c) >= 1",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c) >= 0.1",
      "SELECT * FROM l WHERE 0 < (SELECT COUNT(*) FROM r WHERE a = c)",
      "SELECT * FROM l WHERE 0.99 < (SELECT COUNT(*) FROM r WHERE a = c)",
      "SELECT * FROM l WHERE 1 <= (SELECT COUNT(*) FROM r WHERE a = c)",
      "SELECT * FROM l WHERE 0.01 <= (SELECT COUNT(*) FROM r WHERE a = c)"
    ).foreach(checkResult(_, Seq(row(2, 1.0), row(2, 1.0), row(3, 3.0), row(6, null))))

    Seq(
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c AND c > 5) > 0",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c AND c > 5) > 0.9",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c AND c > 5) >= 1",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c AND c > 5) >= 0.1",
      "SELECT * FROM l WHERE 0 < (SELECT COUNT(*) FROM r WHERE a = c AND c > 5)",
      "SELECT * FROM l WHERE 0.99 < (SELECT COUNT(*) FROM r WHERE a = c AND c > 5)",
      "SELECT * FROM l WHERE 1 <= (SELECT COUNT(*) FROM r WHERE a = c AND c > 5)",
      "SELECT * FROM l WHERE 0.01 <= (SELECT COUNT(*) FROM r WHERE a = c AND c > 5)"
    ).foreach(checkResult(_, Seq(row(6, null))))

    Seq(
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c AND c > 15) > 0",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c AND c > 15) > 0.9",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c AND c > 15) >= 1",
      "SELECT * FROM l WHERE (SELECT COUNT(*) FROM r WHERE a = c AND c > 15) >= 0.1",
      "SELECT * FROM l WHERE 0 < (SELECT COUNT(*) FROM r WHERE a = c AND c > 15)",
      "SELECT * FROM l WHERE 0.99 < (SELECT COUNT(*) FROM r WHERE a = c AND c > 15)",
      "SELECT * FROM l WHERE 1 <= (SELECT COUNT(*) FROM r WHERE a = c AND c > 15)",
      "SELECT * FROM l WHERE 0.01 <= (SELECT COUNT(*) FROM r WHERE a = c AND c > 15)"
    ).foreach(checkResult(_, Seq.empty))
  }

}


