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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class SortLimitTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testSortProcessingTimeAscWithOffSet0AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime LIMIT 1 OFFSET 0")
  }

  @Test
  def testSortProcessingTimeDescWithOffSet0AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime DESC LIMIT 1 OFFSET 0")
  }

  @Test
  def testSortProcessingTimeAscWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime LIMIT 1")
  }

  @Test
  def testSortProcessingTimeFirstDescWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime DESC LIMIT 1")
  }

  @Test
  def testSortRowTimeFirstDescWithLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime DESC LIMIT 0")
  }

  @Test
  def testSortRowTimeAscWithOffSet0AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime LIMIT 1 OFFSET 0")
  }

  @Test
  def testSortRowTimeAscWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime LIMIT 1")
  }

  @Test
  def testSortRowTimeFirstDescWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime DESC LIMIT 1")
  }

  @Test
  def testSortRowTimeDescWithOffSet0AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime DESC LIMIT 1 OFFSET 0")
  }

  @Test
  def testOrderByLimit(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM MyTable ORDER BY a, b DESC LIMIT 10", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testOrderByFetch(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM MyTable ORDER BY a, b DESC FETCH FIRST 10 ROWS ONLY",
      ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSortProcessingTimeWithLimit(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime, c LIMIT 2")
  }

  @Test
  def testSortProcessingTimeWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime, c OFFSET 2")
  }

  @Test
  def testSortRowTimeWithLimit(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime, c LIMIT 2")
  }

  @Test
  def testSortRowTimeWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime, c OFFSET 2")
  }

  @Test
  def testSortProcessingTimeDescWithLimit(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime desc, c LIMIT 2")
  }

  @Test
  def testSortProcessingTimeDescWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime desc, c OFFSET 2")
  }

  @Test
  def testSortRowTimeDescWithLimit(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime desc, c LIMIT 2")
  }

  @Test
  def testSortRowTimeDescWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime desc, c OFFSET 2")
  }

  @Test
  def testSortProcessingTimeSecondWithLimit(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, proctime LIMIT 2")
  }

  @Test
  def testSortProcessingTimeSecondWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, proctime OFFSET 2")
  }

  @Test
  def testSortRowTimeSecondWithLimit(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, rowtime LIMIT 2")
  }

  @Test
  def testSortRowTimeSecondWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, rowtime OFFSET 2")
  }

  @Test
  def testSortProcessingTimeSecondDescWithLimit(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, proctime desc LIMIT 2")
  }

  @Test
  def testSortProcessingTimeSecondDescWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, proctime desc OFFSET 2")
  }

  @Test
  def testSortRowTimeDescSecondWithLimit(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, rowtime desc LIMIT 2")
  }

  @Test
  def testSortRowTimeDescSecondWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, rowtime desc OFFSET 2")
  }

  @Test
  def testSortProcessingTimeSecondDescWithLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, proctime desc LIMIT 0")
  }

  @Test
  def testSortProcessingTimeSecondDescWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, proctime desc LIMIT 1")
  }

  @Test
  def testSortProcessingTimeSecondAscWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, proctime LIMIT 1")
  }

  @Test
  def testSortProcessingTimeFirstDescWithLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime desc LIMIT 0")
  }

  @Test
  def testSortProcessingTimeFirstAscWithLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime, c LIMIT 0")
  }

  @Test
  def testSortProcessingTimeFirstAscWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime, c LIMIT 1")
  }

  @Test
  def testSortProcessingTimeFirstDescWithOffSet0AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime, c desc LIMIT 1 OFFSET 0")
  }

  @Test
  def testSortProcessingTimeFirstDescWithOffSet1AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime, c desc LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortProcessingTimeFirstAscWithOffSet1AndLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime, c LIMIT 0 OFFSET 1")
  }

  @Test
  def testSortProcessingTimeFirstAscWithOffSet1AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime, c LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortProcessingTimeDescWithOffSet1AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime desc LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortProcessingTimeAscWithOffSet1AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY proctime LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortRowTimeSecondDescWithLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, rowtime DESC LIMIT 0")
  }

  @Test
  def testSortRowTimeSecondDescWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, rowtime DESC LIMIT 1")
  }

  @Test
  def testSortRowTimeSecondAscWithLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, rowtime LIMIT 0")
  }

  @Test
  def testSortRowTimeSecondAscWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c, rowtime LIMIT 1")
  }

  @Test
  def testSortRowTimeFirstAscWithLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime, c LIMIT 0")
  }

  @Test
  def testSortRowTimeFirstAscWithLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime, c LIMIT 1")
  }

  @Test
  def testSortRowTimeFirstDescWithOffSet1AndLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime, c desc LIMIT 0 OFFSET 1")
  }

  @Test
  def testSortRowTimeFirstDescWithOffSet1AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime, c desc LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortRowTimeFirstAscWithOffSet1AndLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime, c LIMIT 0 OFFSET 1")
  }

  @Test
  def testSortRowTimeFirstAscWithOffSet1AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime, c LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortRowTimeAscWithLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime LIMIT 0")
  }

  @Test
  def testSortRowTimeDescWithOffSet1AndLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime desc LIMIT 0 OFFSET 1")
  }

  @Test
  def testSortRowTimeDescWithOffSet1AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime desc LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortRowTimeAscWithOffSet1AndLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime LIMIT 0 OFFSET 1")
  }

  @Test
  def testSortRowTimeAscWithOffSet1AndLimit1(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime LIMIT 1 OFFSET 1")
  }

  @Test
  def testSortRowTimeDescWithOffSet0AndLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime desc LIMIT 0 OFFSET 0")
  }

  @Test
  def testSortRowTimeAscWithOffSet0AndLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY rowtime LIMIT 0 OFFSET 0")
  }

  @Test
  def testSortWithoutTimeWithLimit(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c LIMIT 2")
  }

  @Test
  def testSortWithoutTimeWithLimit0(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c LIMIT 0")
  }

  @Test
  def testSortWithoutTimeWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c OFFSET 2")
  }
}
