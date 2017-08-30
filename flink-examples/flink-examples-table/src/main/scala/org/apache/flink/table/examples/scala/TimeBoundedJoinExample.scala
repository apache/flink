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

import org.apache.flink.api.common.io.GenericInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

import scala.util.Random

/**
  * A temporary example to show how to use the TimeBoundedJoin.
  * Will be removed before code committing.
  */
object TimeBoundedJoinExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(10)

    val orderA: DataStream[Order] = env.createInput(new InfiniteSource(1000, 0))
      .setParallelism(1)
      .assignTimestampsAndWatermarks(new WatermarksAssigner(500))

    val orderB: DataStream[Order] = env.createInput(new InfiniteSource(1000, 0))
      .setParallelism(1).assignTimestampsAndWatermarks(new WatermarksAssigner(500))

    // register the DataStreams under the name "OrderA" and "OrderB"
    tEnv.registerDataStream("OrderA", orderA, 'userA, 'productA, 'amountA, 'rtA.rowtime)
    tEnv.registerDataStream("OrderB", orderB, 'userB, 'productB, 'amountB, 'rtB.rowtime)
    tEnv.queryConfig

    // union the two tables
    val result = tEnv.sql(
      "SELECT userA, productA, amountA, DATE_FORMAT(rtA, '%i:%S'), DATE_FORMAT(rtB, '%i:%S'), " +
        "productB FROM OrderA, OrderB" +
        " WHERE OrderA.productA = OrderB.productB " +
        "AND OrderA.rtA BETWEEN OrderB.rtB - INTERVAL '10' SECOND AND OrderB.rtB + INTERVAL '8' " +
        //"AND OrderA.rtA BETWEEN OrderB.rtB - INTERVAL '10' SECOND AND OrderB.rtB - INTERVAL '8'
        // " +
        //"AND OrderA.rtA BETWEEN OrderB.rtB + INTERVAL '6' SECOND AND OrderB.rtB + INTERVAL '10'
        // " +
        "SECOND")

    result.toAppendStream[Order2].addSink(new SinkFunction[Order2] {
      override def invoke(value: Order2): Unit = {
        println(value)
      }
    })
    println(env.getExecutionPlan)
    env.execute()
  }

  /**
    * InfiniteSource
    *
    * @param interval
    */
  class InfiniteSource(interval: Int, offset: Long)
    extends GenericInputFormat[Order]
      with ResultTypeQueryable[Order] {
    var a: Long = 0
    val b: Seq[String] = Seq("beer", "diaper", "rubber")
    var c: Int = 0

    override def reachedEnd(): Boolean = {
      false
    }

    override def nextRecord(reuse: Order): Order = {
      Thread.sleep(interval)
      a += 1
      c += 1
      Order(a, b(1), c, System.currentTimeMillis() - Random.nextInt(1000) - offset)
    }

    override def getProducedType: TypeInformation[Order] = {
      implicitly[TypeInformation[Order]]
    }
  }


  /**
    * WatermarksAssigner
    *
    * @param interval
    */
  class WatermarksAssigner(interval: Long) extends AssignerWithPunctuatedWatermarks[Order] {
    var lastWatermarks: Long = 0

    override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
      element.rt
    }

    override def checkAndGetNextWatermark(lastElement: Order, extractedTimestamp: Long): Watermark = {
      if (extractedTimestamp >= lastWatermarks + interval) {
        lastWatermarks = lastWatermarks + ((extractedTimestamp - lastWatermarks) / interval) *
          interval
        new Watermark(lastWatermarks)
      } else {
        null
      }
    }
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int, rt: Long)

  case class Order2(userA: Long, productA: String, amountA: Int, rtA: String, rtB: String,
    productB: String)


}
