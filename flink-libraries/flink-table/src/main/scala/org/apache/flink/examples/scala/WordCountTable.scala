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

package org.apache.flink.examples.scala

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._

/**
 * Simple example for demonstrating the use of the Table API for a Word Count.
 */
object WordCountTable {

  case class WC(word: String, count: Int)

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    val expr = input.toTable
    val result = expr
      .groupBy('word)
      .select('word, 'count.sum as 'count)
      .toDataSet[WC]

    result.print()
  }
}
