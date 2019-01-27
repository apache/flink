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
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Simple example that shows how to use the Stream SQL API to calculate pv and uv
  * for website visits.
  *
  * <p>It also shows how to use toRetractStream.
  *
  * <p>Usage: <code>RetractPvUvSQL --output &lt;path&gt;</code><br>
  * If no parameters are provided, the program will print result to stdout.
  *
  */
object RetractPvUvSQL {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.fromElements(
      PageVisit("2017-09-16 09:00:00", 1001, "/page1"),
      PageVisit("2017-09-16 09:00:00", 1001, "/page2"),
      PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
      PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
      PageVisit("2017-09-16 10:30:00", 1005, "/page2"))

    // register the DataStream as table "visit_table"
    tEnv.registerDataStream("visit_table", input, 'visit_time, 'user_id, 'visit_page)

    // run a SQL query on the Table
    val table = tEnv.sqlQuery(
      "SELECT " +
        "  visit_time, " +
        "  DATE_FORMAT(max(visit_time), 'HH') as ts, " +
        "  count(user_id) as pv, " +
        "  count(distinct user_id) as uv " +
        "FROM visit_table " +
        "GROUP BY visit_time")

    val dataStream = tEnv.toRetractStream[Row](table)

    if (params.has("output")) {
      val outPath = params.get("output")
      System.out.println("Output path: " + outPath)
      dataStream.writeAsCsv(outPath)
    } else {
      System.out.println("Printing result to stdout. Use --output to specify output path.")
      dataStream.print()
    }
    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class PageVisit(visit_time: String, user_id: Long, visit_page: String)
}
