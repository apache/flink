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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala._

/**
 * Simple example for demonstrating the use of SQL on a table backed by a [[DataStream]] in Scala
 * DataStream API.
 *
 * In particular, the example shows how to
 *   - convert two bounded data streams to tables,
 *   - register a table as a view under a name,
 *   - run a stream SQL query on registered and unregistered tables,
 *   - and convert the insert-only table back to a data stream.
 *
 * The example executes a single Flink job. The results are written to stdout.
 */
object StreamSQLExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up the Scala DataStream API
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // set up the Scala Table API
    val tableEnv = StreamTableEnvironment.create(env)

    val orderA = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)))

    val orderB = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)))

    // convert the first DataStream to a Table object
    // it will be used "inline" and is not registered in a catalog
    val tableA = tableEnv.fromDataStream(orderA)

    // convert the second DataStream and register it as a view
    // it will be accessible under a name
    tableEnv.createTemporaryView("TableB", orderB)

    // union the two tables
    val result = tableEnv.sqlQuery(
      s"""
         |SELECT * FROM $tableA WHERE amount > 2
         |UNION ALL
         |SELECT * FROM TableB WHERE amount < 2
        """.stripMargin)

    // convert the Table back to an insert-only DataStream of type `Order`
    tableEnv.toDataStream(result, classOf[Order]).print()

    // after the table program is converted to a DataStream program,
    // we must use `env.execute()` to submit the job
    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  /** Simple case class. */
  case class Order(user: Long, product: String, amount: Int)
}
