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

import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.Stream._
import scala.language.postfixOps
import scala.util.Random

object WindowJoin {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************

  case class Grade(time: Long, name: String, grade: Int)
  case class Salary(time: Long, name: String, salary: Int)
  case class Person(name: String, grade: Int, salary: Int)

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)
    println("Usage: WindowJoin --grades <path> --salaries <path> --output <path>")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.setGlobalJobParameters(params)

    // Create streams for grades and salaries by mapping the inputs to the corresponding objects
    val grades = setGradesDataStream(env, params)
    val salaries = setSalariesDataStream(env, params)

    //Join the two input streams by name on the last 2 seconds every second and create new
    //Person objects containing both grade and salary
    val joined = grades.join(salaries)
        .where(_.name)
        .equalTo(_.name)
        .window(SlidingTimeWindows.of(Time.of(2, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS)))
        .apply { (g, s) => Person(g.name, g.grade, s.salary) }

    if (params.has("output")) {
      joined.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      joined.print()
    }

    env.execute("WindowJoin")
  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************

  val names = Array("tom", "jerry", "alice", "bob", "john", "grace")
  val gradeCount = 5
  val salaryMax = 10000
  val sleepInterval = 100
  
  def gradeStream: Stream[(Long, String, Int)] = {
    def gradeMapper(names: Array[String])(x: Int): (Long, String, Int) =
      {
        if (x % sleepInterval == 0) Thread.sleep(sleepInterval)
        (System.currentTimeMillis(),names(Random.nextInt(names.length)),Random.nextInt(gradeCount))
      }
    range(1, 100).map(gradeMapper(names))
  }

  def salaryStream: Stream[(Long, String, Int)] = {
    def salaryMapper(x: Int): (Long, String, Int) =
      {
        if (x % sleepInterval == 0) Thread.sleep(sleepInterval)
        (System.currentTimeMillis(), names(Random.nextInt(names.length)), Random.nextInt(salaryMax))
      }
    range(1, 100).map(salaryMapper)
  }

  def parseMap(line : String): (Long, String, Int) = {
    val record = line.substring(1, line.length - 1).split(",")
    (record(0).toLong, record(1), record(2).toInt)
  }

  // *************************************************************************
  // UTIL METHODS
  // *************************************************************************

  private def setGradesDataStream(env: StreamExecutionEnvironment, params: ParameterTool) :
                       DataStream[Grade] = {
    if (params.has("grades")) {
      env.readTextFile(params.get("grades")).map(parseMap _ ).map(x => Grade(x._1, x._2, x._3))
    } else {
      println("Executing WindowJoin example with default grades data set.")
      println("Use --grades to specify file input.")
      env.fromCollection(gradeStream).map(x => Grade(x._1, x._2, x._3))
    }
  }

  private def setSalariesDataStream(env: StreamExecutionEnvironment, params: ParameterTool) :
                         DataStream[Salary] = {
    if (params.has("salaries")) {
      env.readTextFile(params.get("salaries")).map(parseMap _).map(x => Salary(x._1, x._2, x._3))
    } else {
      println("Executing WindowJoin example with default salaries data set.")
      println("Use --salaries to specify file input.")
      env.fromCollection(salaryStream).map(x => Salary(x._1, x._2, x._3))
    }
  }
}
