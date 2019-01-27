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
package org.apache.flink.table.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.java.{Over => JOver}
import org.apache.flink.table.api.scala.{Over => SOver, _}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class OverWindowAggregateTest extends TableTestBase {

  private val util = batchTestUtil()
  private val t = util.addTable[(Int, Long, String, Long)]("Table3", 'a, 'b, 'c)

  @Test
  def testOverWindowCurrentRowAndCurrentRow(): Unit = {
    val resJava = t.window(
      JOver.partitionBy("c").orderBy("a").preceding("current_row").following("current_row").as("w"))
        .select("c, count(a) OVER w")

    val resScala = t
        .window(SOver partitionBy 'c orderBy 'a preceding CURRENT_ROW following CURRENT_ROW as 'w)
        .select('c, 'a.count over 'w)


    verifyTableEquals(resScala, resJava)
    util.verifyPlan(resJava)
    util.verifyPlan(resScala)
  }
}
