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
package org.apache.flink.api.scala.hadoop.mapred

import org.apache.flink.annotation.Public
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormatBase
import org.apache.hadoop.mapred.{JobConf, OutputCommitter, OutputFormat}

@Public
class HadoopOutputFormat[K, V](mapredOutputFormat: OutputFormat[K, V], job: JobConf)
  extends HadoopOutputFormatBase[K, V, (K, V)](mapredOutputFormat, job) {

  def this(
      mapredOutputFormat: OutputFormat[K, V],
      outputCommitterClass: Class[OutputCommitter],
      job: JobConf) {

    this(mapredOutputFormat, job)
    this.getJobConf.setOutputCommitter(outputCommitterClass)
  }

  def writeRecord(record: (K, V)) {
    this.recordWriter.write(record._1, record._2)
  }
}
