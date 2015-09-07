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

package eu.enx.flink.dfwssp.utils

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}

class ResultsLogger(subtaskID: Int, clock: Int, slack: Int, beta: Double) {
  /**
   * The path to the log file for each worker on HDFS looks like this:
   * /cluster_setting/beta_slack/sampleID/workerID.csv
   * TODO: getFilePath(workerID)
   * @return the path to the log file for this worker
   */

  var jobConf: Config = ConfigFactory.load("job.conf")

  /**
   * Writes the results to the disk
   */

  def writeToDisk(index: Int, residualNorm: Double, dualityGap: Double, t0: Long, t1: Long): Unit
  = {
    val dir: String = getLogFileDir
    val path: String = getLogFilePath
    val data: String = produceLogEntry(index, residualNorm, dualityGap, t1 - t0, t0)
    Files.createDirectories(Paths.get(dir))
    Files.write(Paths.get(path), (data + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption
      .APPEND, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
  }

  def getLogFilePath: String = {
    val res = getLogFileDir + subtaskID + ".csv"
    res
  }

  def getLogFileDir: String = {
    jobConf.getString("log.rootdir") + "/" + jobConf.getString("log.expedir") + "/"
  }

  /**
   * Produces one line of log in the form (workerID, clock, atomID, worktime, residual)
   * @return a CSV String with the log entry
   */
  def produceLogEntry(
    atomIndex: Int, residual: Double, dualityGap: Double, elapsedTime: Long,
    startTime: Long): String = {

    val res = List(subtaskID, clock, atomIndex, elapsedTime, residual, dualityGap, startTime)
      .mkString(",")
    println("log entry: " + res)
    res
  }

  def write(uri: String, filePath: String, data: List[String]): Unit = {
    def values = for (i <- data) yield i

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val path = new Path(filePath)
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)

    if (fs.exists(path)) {
      fs.delete(path, false)
    }

    val os = fs.create(path)
    data.foreach(a => os.write((a + "\n").getBytes()))

    fs.close()
  }
}
