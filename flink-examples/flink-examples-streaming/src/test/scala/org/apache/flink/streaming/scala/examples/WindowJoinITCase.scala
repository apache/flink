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

package org.apache.flink.streaming.scala.examples

import java.io.File

import org.apache.commons.io.FileUtils

import org.apache.flink.streaming.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.scala.examples.join.WindowJoin
import org.apache.flink.streaming.scala.examples.join.WindowJoin.{Grade, Person, Salary}
import org.apache.flink.streaming.test.exampleJavaPrograms.join.WindowJoinData
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.test.util.TestBaseUtils

import org.junit.Test

class WindowJoinITCase extends StreamingMultipleProgramsTestBase {
  
  @Test
  def testProgram(): Unit = {
    
    val resultPath: String = File.createTempFile("result-path", "dir").toURI().toString()
    try {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
      
      val grades: DataStream[Grade] = env
        .fromCollection(WindowJoinData.GRADES_INPUT.split("\n"))
        .map( line => {
          val fields = line.split(",")
          Grade(fields(1), fields(2).toInt)
        })

      val salaries: DataStream[Salary] = env
        .fromCollection(WindowJoinData.SALARIES_INPUT.split("\n"))
        .map( line => {
          val fields = line.split(",")
          Salary(fields(1), fields(2).toInt)
        })
      
      WindowJoin.joinStreams(grades, salaries, 100)
        .writeAsText(resultPath, WriteMode.OVERWRITE)
      
      env.execute()

      TestBaseUtils.checkLinesAgainstRegexp(resultPath, "^Person\\([a-z]+,(\\d),(\\d)+\\)")
    }
    finally {
      try {
        FileUtils.deleteDirectory(new File(resultPath))
      }
      catch {
        case _ : Throwable => 
      }
    }
  }

}
