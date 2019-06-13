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

package org.apache.flink.table.runtime.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{BatchScalaTableEnvUtil, BatchTestBase}
import org.apache.flink.test.util.TestBaseUtils

import org.junit._

import java.math.BigDecimal

import scala.collection.JavaConverters._

class GroupWindowITCase extends BatchTestBase {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"))

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val table = BatchScalaTableEnvUtil.fromCollection(tEnv,
      data, "long, int, double, float, bigdec, string")

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.sum, 'w.start, 'w.end, 'w.rowtime)

    val expected =
      "Hello world,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,1970-01-01 00:00:00.009\n" +
      "Hello world,4,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,1970-01-01 00:00:00.019\n" +
      "Hello,7,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004\n" +
      "Hello,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,1970-01-01 00:00:00.009\n" +
      "Hallo,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004\n" +
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004\n"

    val results = executeQuery(windowedTable)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllEventTimeTumblingWindowOverTime(): Unit = {
    val table = BatchScalaTableEnvUtil.fromCollection(
      tEnv, data, "long, int, double, float, bigdec, string")

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w)
      .select('int.sum, 'w.start, 'w.end, 'w.rowtime)

    val expected =
      "10,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004\n" +
      "6,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,1970-01-01 00:00:00.009\n" +
      "4,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,1970-01-01 00:00:00.019\n"

    val results = executeQuery(windowedTable)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
