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

import java.io.Serializable
import java.util.Random

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.examples.utils.ThrottledIterator
import org.apache.flink.streaming.scala.examples.join.WindowJoin.{Grade, Salary}

import scala.collection.JavaConverters._

/**
 * Sample data for the [[WindowJoin]] example.
 */
object WindowJoinSampleData {
  
  private[join] val NAMES = Array("tom", "jerry", "alice", "bob", "john", "grace")
  private[join] val GRADE_COUNT = 5
  private[join] val SALARY_MAX = 10000

  /**
   * Continuously generates (name, grade).
   */
  def getGradeSource(env: StreamExecutionEnvironment, rate: Long): DataStream[Grade] = {
      env.fromCollection(new ThrottledIterator(new GradeSource().asJava, rate).asScala)
  }

  /**
   * Continuously generates (name, salary).
   */
  def getSalarySource(env: StreamExecutionEnvironment, rate: Long): DataStream[Salary] = {
    env.fromCollection(new ThrottledIterator(new SalarySource().asJava, rate).asScala)
  }
  
  // --------------------------------------------------------------------------
  
  class GradeSource extends Iterator[Grade] with Serializable {
    
    private[this] val rnd = new Random(hashCode())

    def hasNext: Boolean = true

    def next: Grade = {
      Grade(NAMES(rnd.nextInt(NAMES.length)), rnd.nextInt(GRADE_COUNT) + 1)
    }
  }
  
  class SalarySource extends Iterator[Salary] with Serializable {

    private[this] val rnd = new Random(hashCode())

    def hasNext: Boolean = true

    def next: Salary = {
      Salary(NAMES(rnd.nextInt(NAMES.length)), rnd.nextInt(SALARY_MAX) + 1)
    }
  }
}
