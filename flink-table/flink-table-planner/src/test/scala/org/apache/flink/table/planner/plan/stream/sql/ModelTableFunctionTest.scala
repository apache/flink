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

import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.jupiter.api.Test

/** Tests for model table-valued function. */
class ModelTableFunctionTest extends TableTestBase {

  private val util = streamTestUtil()
  util.tableEnv.executeSql(s"""
                              |CREATE TABLE MyTable (
                              |  a INT,
                              |  b BIGINT,
                              |  c STRING,
                              |  d DECIMAL(10, 3),
                              |  rowtime TIMESTAMP(3),
                              |  proctime as PROCTIME(),
                              |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
                              |) with (
                              |  'connector' = 'values'
                              |)
                              |""".stripMargin)

  util.tableEnv.executeSql(s"""
                              |CREATE MODEL MyModel
                              |INPUT (a INT, b BIGINT)
                              |OUTPUT(c STRING, D ARRAY<INT>)
                              |with (
                              |  'provider' = 'openai'
                              |)
                              |""".stripMargin)

  @Test
  def testMLPredictTVFWithNamedArguments(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(ML_PREDICT(data => TABLE MyTable, input_model => MODEL MyModel, input_column => DESCRIPTOR(a, b)))
        |""".stripMargin
    util.verifyExplain(sql)
  }

  @Test
  def testMLPredictTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b)))
        |""".stripMargin
    util.verifyExplain(sql)
  }
}
