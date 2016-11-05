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

package org.apache.flink.streaming.scala.examples.join

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Example illustrating a windowed stream join between two data streams.
 *
 * The example works on two input streams with pairs (name, grade) and (name, salary)
 * respectively. It joins the steams based on "name" within a configurable window.
 *
 * The example uses a built-in sample data generator that generates
 * the steams of pairs at a configurable rate.
 */
object WindowJoin {

  // *************************************************************************
  //  Program Data Types
  // *************************************************************************

  case class Grade(name: String, grade: Int)
  
  case class Salary(name: String, salary: Int)
  
  case class Person(name: String, grade: Int, salary: Int)

  // *************************************************************************
  //  Program
  // *************************************************************************

  def main(args: Array[String]) {
    // parse the parameters
    val params = ParameterTool.fromArgs(args)
    val windowSize = params.getLong("windowSize", 2000)
    val rate = params.getLong("rate", 3)

    println("Using windowSize=" + windowSize + ", data rate=" + rate)
    println("To customize example, use: WindowJoin " +
      "[--windowSize <window-size-in-millis>] [--rate <elements-per-second>]")

    // obtain execution environment, run this example in "ingestion time"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // // create the data sources for both grades and salaries
    val grades = WindowJoinSampleData.getGradeSource(env, rate)
    val salaries = WindowJoinSampleData.getSalarySource(env, rate)

    // join the two input streams by name on a window.
    // for testability, this functionality is in a separate method.
    val joined = joinStreams(grades, salaries, windowSize)

    // print the results with a single thread, rather than in parallel
    joined.print().setParallelism(1)

    // execute program
    env.execute("Windowed Join Example")
  }


  def joinStreams(
      grades: DataStream[Grade],
      salaries: DataStream[Salary],
      windowSize: Long) : DataStream[Person] = {

    grades.join(salaries)
      .where(_.name)
      .equalTo(_.name)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
      .apply { (g, s) => Person(g.name, g.grade, s.salary) }
  }
}
