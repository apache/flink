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

package org.apache.flink.table.examples.scala

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfigOptions, TableEnvironment}
import org.apache.flink.table.api.scala._

object SimpleInteractiveExample {

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val tEnv = TableEnvironment.getBatchTableEnvironment(env)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)

    val data = Seq(
      ("US", "Red", 10),
      ("UK", "Blue", 20),
      ("CN", "Yellow", 30),
      ("US", "Blue", 40),
      ("UK", "Red", 50),
      ("CN", "Red", 60),
      ("US", "Yellow", 70),
      ("UK", "Yellow", 80),
      ("CN", "Blue", 90),
      ("US", "Blue", 100)
    )

    try {
      val t = tEnv.fromCollection(data).as('country, 'color, 'count)

      val t1 = t.filter('count < 100)
      t1.cache()
      val x = t1.collect().size
      println(s"Number of orders whose count is less than 10: $x")

      println("\nCount by country:")
      val t2 = t1.groupBy('country).select('country, 'count.sum as 'sum)
      val res2 = t2.collect()
      res2.foreach(println)

      println("\nCount by color:")
      val t3 = t1.groupBy('color).select('color, 'count.avg as 'avg)
      val res3 = t3.collect()
      res3.foreach(println)
    } catch {
      case e: Throwable =>
        println(s"Caught unexpected exception: $e")
        e.printStackTrace()
    } finally {
      tEnv.close()
    }
  }
}
