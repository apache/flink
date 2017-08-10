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

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object UserDefinedFunctionTestUtils {

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

  def writeCacheFile(fileName: String, contents: String): String = {
    val tempFile = File.createTempFile(this.getClass.getName + "-" + fileName, "tmp")
    tempFile.deleteOnExit()
    Files.write(contents, tempFile, Charsets.UTF_8)
    tempFile.getAbsolutePath
  }
}
