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

import org.apache.flink.api.scala._

import org.apache.flink.test.testdata.WordCountData
import org.apache.flink.test.util.{TestBaseUtils, JavaProgramTestBase}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, TextOutputFormat, TextInputFormat}

class WordCountMapredITCase extends JavaProgramTestBase {
  protected var textPath: String = null
  protected var resultPath: String = null

  protected override def preSubmit() {
    textPath = createTempFile("text.txt", WordCountData.TEXT)
    resultPath = getTempDirPath("result")
  }

  protected override def postSubmit() {
    TestBaseUtils.compareResultsByLinesInMemory(WordCountData.COUNTS,
                                                resultPath, Array[String](".", "_"))
  }

  protected def testProgram() {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val input =
      env.readHadoopFile(new TextInputFormat, classOf[LongWritable], classOf[Text], textPath)

    val text = input map { _._2.toString }
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    val words = counts map { t => (new Text(t._1), new LongWritable(t._2)) }

    val hadoopOutputFormat = new HadoopOutputFormat[Text,LongWritable](
      new TextOutputFormat[Text, LongWritable],
      new JobConf)
    hadoopOutputFormat.getJobConf.set("mapred.textoutputformat.separator", " ")

    FileOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf, new Path(resultPath))

    words.output(hadoopOutputFormat)

    env.execute("Hadoop Compat WordCount")
  }
}

