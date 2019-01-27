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

package org.apache.flink.table.temptable

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.expressions.Expression

/**
  * A Linear Regression Example.
  */
object FlinkInteractiveExample extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv = TableEnvironment.getBatchTableEnvironment(env)

  var model = Model(100, 100, 100)
  val trainingData = tEnv.fromCollection(Seq(
    (1.0, 1.0, 1.0, 3.0),
    (1.0, 2.0, 1.0, 4.0),
    (1.0, 1.0, 6.0, 13.0),
    (1.0, 3.0, 4.0, 11.0),
    (1.0, 5.0, 2.0, 9.0),
    (1.0, 7.0, 9.0, 25.0),
    (1.0, 8.0, 3.0, 14.0),
    (1.0, 1.5, 6.0, 13.5),
    (1.0, 10.0, 11.0, 32.0)
  )).as('x0, 'x1, 'x2, 'y)

  var iterations = 800
  val learningRate = 0.003

  val filteredTrainingData = trainingData.filter('x0 < 100)

  filteredTrainingData.cache()

  // first run
  val filteredDataSize = filteredTrainingData.collect().size

  // decision
  if (filteredDataSize < 20) {

    while (iterations > 0) {

      println(s"do iteration round: #${iterations}")

      val expr: Expression = 'y - ('x0 * model.th0  + 'x1 * model.th1 + 'x2 * model.th2)

      val delta = filteredTrainingData
        .select(
          expr * 'x0 as 'x0,
          expr * 'x1 as 'x1,
          expr * 'x2 as 'x2
        )
        .select(
          'x0.sum as 'd0,
          'x1.sum as 'd1,
          'x2.sum as 'd2
        ).collect().head

      val deltaModel = Model(
        delta.getField(0).asInstanceOf[Double],
        delta.getField(1).asInstanceOf[Double],
        delta.getField(2).asInstanceOf[Double]
      )

      model = model + (deltaModel * learningRate)
      iterations -= 1
    }
  } else {

    val res = filteredTrainingData
      .select('x0.avg as 'x0, 'x1.avg as 'x1, 'x2.avg as 'x2).collect().head

    val d0 = res.getField(0).asInstanceOf[Double]
    val d1 = res.getField(1).asInstanceOf[Double]
    val d2 = res.getField(2).asInstanceOf[Double]

    model = Model(d0, d1, d2)
  }

  println(s"model is $model")

  tEnv.tableServiceManager.close()
}

case class Model(th0: Double, th1: Double, th2: Double) {

  def +(other: Model): Model =
    Model(th0 + other.th0, th1 + other.th1, th2 + other.th2)

  def *(c: Double): Model =
    Model(c * th0, c * th1, c * th2)
}
