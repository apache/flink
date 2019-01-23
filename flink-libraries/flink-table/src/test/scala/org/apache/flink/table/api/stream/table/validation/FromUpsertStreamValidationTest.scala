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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class FromUpsertStreamValidationTest extends TableTestBase {

  @Test(expected = classOf[TableException])
  def testInvalidKeyOnAppendStream(): Unit = {
    val util = streamTestUtil()
    // can not apply key on append stream
    util.addTable[(Int, Long, String)]('a.key, 'b, 'c)
  }

  @Test(expected = classOf[TableException])
  def testInvalidToTableOnUpsertStream(): Unit = {
    val data = List((true, (1L, 1, 1d)))
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val stream = env
      .fromCollection(data)
    stream.toTableFromAppendStream(tEnv, 'long, 'int, 'double)
  }

  @Test(expected = classOf[TableException])
  def testInvalidInputType(): Unit = {
    val util = streamTestUtil()
    // throw exception. Can only upsert from a datastream of type Tuple2
    util.addTableFromUpsert[(Long, Int, String)]('long, 'int, 'string)
  }
}

