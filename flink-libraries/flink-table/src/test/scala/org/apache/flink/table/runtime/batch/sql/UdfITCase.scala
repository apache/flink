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
package org.apache.flink.table.runtime.batch.sql

import java.util.Random

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.hive.functions.{HiveFunctionWrapper, HiveGenericUDF}
import org.apache.flink.table.runtime.batch.sql.TestData.{data3, nullablesOfData3, type3}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

class UdfITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("Table3", data3, type3, nullablesOfData3, 'a, 'b, 'c)
    tEnv.registerFunction("rand_udf", RandUDF)
  }

  @Test
  def testPushFilterPastProjectWithNondeterministicProject(): Unit = {
    val sql = "SELECT * FROM (SELECT rand_udf(a) AS a FROM Table3) t WHERE a > 0"
    val result = executeQuery(sql)
    result.foreach { row =>
      assertEquals(1, row.getArity)
      assertTrue(row.getField(0).asInstanceOf[Int] > 0)
    }
  }

  @Test
  def testHiveGenericUDFConcat(): Unit = {

    val concatUdf = new HiveGenericUDF(new HiveFunctionWrapper[GenericUDF](
      "org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat"))
    tEnv.registerFunction("concat", concatUdf)
    val result = executeQuery("SELECT concat(c, c) FROM Table3")
    result.foreach { row =>
      assertEquals(1, row.getArity)
      val base = row.toString.substring(0, row.getField(0).toString.length / 2)
      assertEquals(base + base, row.toString)
    }
  }

}

object RandUDF extends ScalarFunction {
  def eval(value: Int): Int = {
    val r = new Random()
    if (r.nextBoolean()) {
      -1
    } else {
      value
    }
  }

  override def isDeterministic = false
}
