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

import scala.collection.mutable

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.concat
import org.apache.flink.table.runtime.utils.{TableProgramsClusterTestBase}
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import scala.collection.JavaConverters._

import org.apache.flink.table.utils.ScalarFunction0

@RunWith(classOf[Parameterized])
class UdfITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsClusterTestBase(mode, configMode) {

  @Test
  def testUdfOpen(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

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

    val results = t.toDataSet[Row].collect()
    val expected = Seq("1,Hi1,Hello1", "2,Hi2,Hello2", "3,Hi3,Hello3").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
