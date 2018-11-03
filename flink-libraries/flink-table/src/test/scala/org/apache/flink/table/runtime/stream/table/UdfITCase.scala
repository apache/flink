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

package org.apache.flink.table.runtime.stream.table

import scala.collection.mutable

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.runtime.utils.StreamITCase
import org.apache.flink.table.utils.ScalarFunction0
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.Before
import org.junit.Assert._
import org.junit.Test

class UdfITCase extends AbstractTestBase {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
  @Before
  def clear(): Unit = {
    StreamITCase.clear
  }

  @Test
  def testUdfOpen(): Unit = {
    val data1 = new mutable.MutableList[(Int, String)]
    data1.+=((1, "Hi1"))
    data1.+=((2, "Hi2"))
    data1.+=((3, "Hi3"))

    val data2 = new mutable.MutableList[(Int, String)]
    data2.+=((1, "Hello1"))
    data2.+=((2, "Hello2"))
    data2.+=((3, "Hello3"))

    val t1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b)
    val t2 = env.fromCollection(data2).toTable(tEnv, 'c, 'd)

    val fun0 = new ScalarFunction0
    val t = t1.join(t2, 'a === 'c ).select('a, 'b, 'd).where(fun0(concat('b,'d)))

    StreamITCase.clear
    val results = t.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = Seq("1,Hi1,Hello1", "2,Hi2,Hello2", "3,Hi3,Hello3")
    assertEquals(expected.sorted.mkString(","), StreamITCase.testResults.sorted.mkString(","))
  }
}
