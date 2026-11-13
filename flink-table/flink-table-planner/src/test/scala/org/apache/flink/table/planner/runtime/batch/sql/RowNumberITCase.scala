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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row

import org.junit.jupiter.api.{BeforeEach, Test}

/** Correctness for batch `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) WHERE rn <= N`. */
class RowNumberITCase extends BatchTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    val data =
      Seq(row(1, "a", 10L), row(2, "a", 30L), row(3, "a", 30L), row(4, "b", 5L), row(5, "b", 7L))
    val tType = new RowTypeInfo(INT_TYPE_INFO, STRING_TYPE_INFO, LONG_TYPE_INFO)
    registerCollection("T", data, tType, "id, grp, v")
  }

  @Test
  def testKeepFirstPerGroup(): Unit = {
    checkResult(
      "SELECT grp, v FROM (" +
        "SELECT grp, v, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY v ASC) rn FROM T) t " +
        "WHERE rn = 1",
      Seq(row("a", 10L), row("b", 5L))
    )
  }

  @Test
  def testKeepLastPerGroup(): Unit = {
    checkResult(
      "SELECT grp FROM (" +
        "SELECT grp, v, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY v DESC) rn FROM T) t " +
        "WHERE rn = 1",
      Seq(row("a"), row("b")))
  }

  @Test
  def testTopNKeepsExactlyNAcrossTies(): Unit = {
    checkResult(
      "SELECT grp, v FROM (" +
        "SELECT grp, v, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY v ASC) rn FROM T) t " +
        "WHERE rn <= 2",
      Seq(row("a", 10L), row("a", 30L), row("b", 5L), row("b", 7L))
    )
  }
}
