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

package org.apache.flink.table.runtime.utils

import org.apache.flink.api.java.tuple.{Tuple1, Tuple2}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.functions.AggregateFunction

import com.google.common.base.Charsets
import com.google.common.io.Files

import java.io.File

object UserDefinedFunctionTestUtils {

  /** Counts how often the first argument was larger than the second argument. */
  class LargerThanCount extends AggregateFunction[Long, Tuple1[Long]] {

    def accumulate(acc: Tuple1[Long], a: Long, b: Long): Unit = {
      if (a > b) acc.f0 += 1
    }

    def retract(acc: Tuple1[Long], a: Long, b: Long): Unit = {
      if (a > b) acc.f0 -= 1
    }

    override def createAccumulator(): Tuple1[Long] = Tuple1.of(0L)

    override def getValue(acc: Tuple1[Long]): Long = acc.f0
  }

  class CountNullNonNull extends AggregateFunction[String, Tuple2[Long, Long]] {

    override def createAccumulator(): Tuple2[Long, Long] = Tuple2.of(0L, 0L)

    override def getValue(acc: Tuple2[Long, Long]): String = s"${acc.f0}|${acc.f1}"

    def accumulate(acc: Tuple2[Long, Long], v: String): Unit = {
      if (v == null) {
        acc.f1 += 1
      } else {
        acc.f0 += 1
      }
    }

    def retract(acc: Tuple2[Long, Long], v: String): Unit = {
      if (v == null) {
        acc.f1 -= 1
      } else {
        acc.f0 -= 1
      }
    }
  }

  class CountPairs extends AggregateFunction[Long, Tuple1[Long]] {

    def accumulate(acc: Tuple1[Long], a: String, b: String): Unit = {
      acc.f0 += 1
    }

    def retract(acc: Tuple1[Long], a: String, b: String): Unit = {
      acc.f0 -= 1
    }

    override def createAccumulator(): Tuple1[Long] = Tuple1.of(0L)

    override def getValue(acc: Tuple1[Long]): Long = acc.f0
  }

  def setJobParameters(env: ExecutionEnvironment, parameters: Map[String, String]): Unit = {
    val conf = new Configuration()
    parameters.foreach {
      case (k, v) => conf.setString(k, v)
    }
    env.getConfig.setGlobalJobParameters(conf)
  }

  def setJobParameters(env: StreamExecutionEnvironment, parameters: Map[String, String]): Unit = {
    val conf = new Configuration()
    parameters.foreach {
      case (k, v) => conf.setString(k, v)
    }
    env.getConfig.setGlobalJobParameters(conf)
  }

  def setJobParameters(
      env: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment,
      parameters: Map[String, String]): Unit = {
    val conf = new Configuration()
    parameters.foreach {
      case (k, v) => conf.setString(k, v)
    }
    env.getConfig.setGlobalJobParameters(conf)
  }

  def writeCacheFile(fileName: String, contents: String): String = {
    val tempFile = File.createTempFile(this.getClass.getName + "-" + fileName, "tmp")
    tempFile.deleteOnExit()
    Files.write(contents, tempFile, Charsets.UTF_8)
    tempFile.getAbsolutePath
  }
}
