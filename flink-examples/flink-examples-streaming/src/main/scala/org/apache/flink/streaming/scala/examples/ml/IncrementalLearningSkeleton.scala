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

package org.apache.flink.streaming.scala.examples.ml

import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Skeleton for incremental machine learning algorithm consisting of a
 * pre-computed model, which gets updated for the new inputs and new input data
 * for which the job provides predictions.
 *
 * This may serve as a base of a number of algorithms, e.g. updating an
 * incremental Alternating Least Squares model while also providing the
 * predictions.
 *
 * This example shows how to use:
 *
 *  - Connected streams
 *  - CoFunctions
 *  - Tuple data types
 *
 */
object IncrementalLearningSkeleton {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // build new model on every second of new data
    val trainingData: DataStream[Int] = env.addSource(new FiniteTrainingDataSource)
    val newData: DataStream[Int] = env.addSource(new FiniteNewDataSource)

    val model: DataStream[Array[Double]] = trainingData
      .assignTimestampsAndWatermarks(new LinearTimestamp)
      .timeWindowAll(Time.of(5000, TimeUnit.MILLISECONDS))
      .apply(new PartialModelBuilder)

    // use partial model for newData
    val prediction: DataStream[Int] = newData.connect(model).map(new Predictor)

    // emit result
    if (params.has("output")) {
      prediction.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      prediction.print()
    }

    // execute program
    env.execute("Streaming Incremental Learning")
  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************

  /**
   * Feeds new data for newData. By default it is implemented as constantly
   * emitting the Integer 1 in a loop.
   */
  private class FiniteNewDataSource extends SourceFunction[Int] {
    override def run(ctx: SourceContext[Int]) = {
      Thread.sleep(15)
      (0 until 50).foreach{ _ =>
        Thread.sleep(5)
        ctx.collect(1)
      }
    }

    override def cancel() = {
      // No cleanup needed
    }
  }

  /**
   * Feeds new training data for the partial model builder. By default it is
   * implemented as constantly emitting the Integer 1 in a loop.
   */
  private class FiniteTrainingDataSource extends SourceFunction[Int] {
    override def run(ctx: SourceContext[Int]) = (0 until 8200).foreach( _ => ctx.collect(1) )

    override def cancel() = {
      // No cleanup needed
    }
  }

  private class LinearTimestamp extends AssignerWithPunctuatedWatermarks[Int] {
    var counter = 0L

    override def extractTimestamp(element: Int, previousElementTimestamp: Long): Long = {
      counter += 10L
      counter
    }

    override def checkAndGetNextWatermark(lastElement: Int, extractedTimestamp: Long) = {
      new Watermark(counter - 1)
    }
  }

  /**
   * Builds up-to-date partial models on new training data.
   */
  private class PartialModelBuilder extends AllWindowFunction[Int, Array[Double], TimeWindow] {

    protected def buildPartialModel(values: Iterable[Int]): Array[Double] = Array[Double](1)

    override def apply(window: TimeWindow,
                       values: Iterable[Int],
                       out: Collector[Array[Double]]): Unit = {
      out.collect(buildPartialModel(values))
    }
  }

  /**
   * Creates newData using the model produced in batch-processing and the
   * up-to-date partial model.
   *
   * By default emits the Integer 0 for every newData and the Integer 1
   * for every model update.
   *
   */
  private class Predictor extends CoMapFunction[Int, Array[Double], Int] {

    var batchModel: Array[Double] = null
    var partialModel: Array[Double] = null

    override def map1(value: Int): Int = {
      // Return newData
      predict(value)
    }

    override def map2(value: Array[Double]): Int = {
      // Update model
      partialModel = value
      batchModel = getBatchModel()
      1
    }

    // pulls model built with batch-job on the old training data
    protected def getBatchModel(): Array[Double] = Array[Double](0)

    // performs newData using the two models
    protected def predict(inTuple: Int): Int = 0
  }

}
