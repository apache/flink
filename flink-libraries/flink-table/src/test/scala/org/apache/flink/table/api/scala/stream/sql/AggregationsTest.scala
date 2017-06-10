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
package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.OverAgg0
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class AggregationsTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testOverAggregation(): Unit = {
    streamUtil.addFunction("overAgg", new OverAgg0)

    val sqlQuery = "SELECT overAgg(c, a) FROM MyTable"

    streamUtil.tEnv.sql(sqlQuery)
  }
}
