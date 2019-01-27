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

import java.sql.Timestamp
import java.util.TimeZone

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Simple example that shows the use of SQL Join on Stream Tables.
  *
  */
object StreamJoinSQLExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t_order: DataStream[Order] = env.fromCollection(Seq(
      Order(Timestamp.valueOf("2018-10-15 09:01:20"), 2, 1, 7),
      Order(Timestamp.valueOf("2018-10-15 09:05:02"), 3, 2, 9),
      Order(Timestamp.valueOf("2018-10-15 09:05:02"), 1, 3, 9),
      Order(Timestamp.valueOf("2018-10-15 10:07:22"), 1, 4, 9),
      Order(Timestamp.valueOf("2018-10-15 10:55:01"), 5, 5, 8)))

    val t_shipment: DataStream[Shipment] = env.fromCollection(Seq(
      Shipment(Timestamp.valueOf("2018-10-15 09:11:00"), 3),
      Shipment(Timestamp.valueOf("2018-10-15 10:01:21"), 1),
      Shipment(Timestamp.valueOf("2018-10-15 11:31:10"), 5)))

    // register the DataStreams under the name "t_order" and "t_shipment"
    tEnv.registerDataStream("t_order", t_order, 'createTime, 'unit, 'orderId, 'productId)
    tEnv.registerDataStream("t_shipment", t_shipment, 'createTime, 'orderId)

    // run a SQL to get orders whose ship date are within one hour of the order date
    val result = tEnv.sqlQuery(
      "SELECT o.createTime, o.productId, o.orderId, s.createTime AS shipTime" +
        " FROM t_order AS o" +
        " JOIN t_shipment AS s" +
        "  ON o.orderId = s.orderId" +
        "  AND s.createTime BETWEEN o.createTime AND o.createTime + INTERVAL '1' HOUR")

    result.toAppendStream[Row].print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(createTime: Timestamp, unit: Int, orderId: Long, productId: Long)

  case class Shipment(createTime: Timestamp, orderId: Long)

}
