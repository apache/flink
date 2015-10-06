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

    if (!parseParameters(args)) {
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableTimestamps()

    //Create streams for grades and salaries by mapping the inputs to the corresponding objects
    val grades = setGradesInput(env)
    val salaries = setSalariesInput(env)

    //Join the two input streams by name on the last 2 seconds every second and create new
    //Person objects containing both grade and salary
    val joined = grades.join(salaries)
        .where(_.name)
        .equalTo(_.name)
        .window(SlidingTimeWindows.of(Time.of(2, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS)))
        .apply { (g, s) => Person(g.name, g.grade, s.salary) }

    if (fileOutput) {
      joined.writeAsText(outputPath)
    } else {
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

  private var fileInput: Boolean = false
  private var fileOutput: Boolean = false

  private var gradesPath: String = null
  private var salariesPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      if (args.length == 1) {
        fileOutput = true
        outputPath = args(0)
      }
      else if (args.length == 3) {
        fileInput = true
        fileOutput = true
        gradesPath = args(0)
        salariesPath = args(1)
        outputPath = args(2)
      } else {
        System.err.println("Usage: WindowJoin <result path> or WindowJoin <input path 1> " +
          "<input path 2> <result path>")
        return false
      }
    } else {
      System.out.println("Executing WindowJoin with generated data.")
      System.out.println("  Provide parameter to write to file.")
      System.out.println("  Usage: WindowJoin <result path>")
    }
    true
  }

  private def setGradesInput(env: StreamExecutionEnvironment) : DataStream[Grade] = {
    if (fileInput) {
      env.readTextFile(gradesPath).map(parseMap _ ).map(x => Grade(x._1, x._2, x._3))
    } else {
      env.fromCollection(gradeStream).map(x => Grade(x._1, x._2, x._3))
    }
  }

  private def setSalariesInput(env: StreamExecutionEnvironment) : DataStream[Salary] = {
    if (fileInput) {
      env.readTextFile(salariesPath).map(parseMap _).map(x => Salary(x._1, x._2, x._3))
    }
    else {
      env.fromCollection(salaryStream).map(x => Salary(x._1, x._2, x._3))
    }
  }
}
