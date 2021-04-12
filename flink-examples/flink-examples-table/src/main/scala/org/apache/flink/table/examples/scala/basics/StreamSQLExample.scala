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
package org.apache.flink.table.examples.scala.basics

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
  * Simple example for demonstrating the use of SQL on a Stream Table in Scala.
  *
  * <p>Usage: <code>StreamSQLExample --planner &lt;blink|flink&gt;</code><br>
  *
  * <p>This example shows how to:
  *  - Convert DataStreams to Tables
  *  - Register a Table under a name
  *  - Run a StreamSQL query on the registered Table
  *
  */
object StreamSQLExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val planner = if (params.has("planner")) params.get("planner") else "blink"

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = if (planner == "blink") {  // use blink planner in streaming mode
      val settings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build()
      StreamTableEnvironment.create(env, settings)
    } else if (planner == "flink") {  // use flink planner in streaming mode
      val settings = EnvironmentSettings.newInstance()
          .useOldPlanner()
          .inStreamingMode()
          .build()
      StreamTableEnvironment.create(env, settings)
    } else {
      System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
        "where planner (it is either flink or blink, and the default is blink) indicates whether the " +
        "example uses flink planner or blink planner.")
      return
    }

    val orderA: DataStream[Order] = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)))

    val orderB: DataStream[Order] = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)))

    // convert DataStream to Table
    val tableA = tEnv.fromDataStream(orderA, $"user", $"product", $"amount")
    // register DataStream as Table
    tEnv.createTemporaryView("OrderB", orderB, $"user", $"product", $"amount")

    // union the two tables
    val result = tEnv.sqlQuery(
      s"""
         |SELECT * FROM $tableA WHERE amount > 2
         |UNION ALL
         |SELECT * FROM OrderB WHERE amount < 2
        """.stripMargin)

    result.toAppendStream[Order].print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int)

}
