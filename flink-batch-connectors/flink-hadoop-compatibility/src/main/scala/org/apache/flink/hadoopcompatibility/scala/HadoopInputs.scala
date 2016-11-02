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
package org.apache.flink.hadoopcompatibility.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.hadoop.mapreduce
import org.apache.flink.api.scala.hadoop.mapred
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.mapred.{JobConf, FileInputFormat => MapredFileInputFormat, InputFormat => MapredInputFormat}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => MapreduceFileInputFormat}
import org.apache.hadoop.mapreduce.{Job, InputFormat => MapreduceInputFormat}

/**
  * HadoopInputs is a utility class to use Apache Hadoop InputFormats with Apache Flink.
  *
  * It provides methods to create Flink InputFormat wrappers for Hadoop
  * [[org.apache.hadoop.mapred.InputFormat]] and [[org.apache.hadoop.mapreduce.InputFormat]].
  * Key value pairs produced by the Hadoop InputFormats are converted into tuple.
  *
  */
object HadoopInputs {

  /**
    * Creates a [[org.apache.flink.api.scala.hadoop.mapred.HadoopInputFormat]] from the given
    * [[org.apache.hadoop.mapred.FileInputFormat]]. The given inputName is set on the given job.
    */
  def readHadoopFile[K, V](
                            mapredInputFormat: MapredFileInputFormat[K, V],
                            key: Class[K],
                            value: Class[V],
                            inputPath: String,
                            job: JobConf)
                          (implicit tpe:
                          TypeInformation[(K, V)]): mapred.HadoopInputFormat[K, V] = {
    MapredFileInputFormat.addInputPath(job, new HadoopPath(inputPath))
    createHadoopInput(mapredInputFormat, key, value, job)
  }

  /**
    * Creates a [[org.apache.flink.api.scala.hadoop.mapred.HadoopInputFormat]]  from the given
    * [[org.apache.hadoop.mapred.FileInputFormat]]. A [[org.apache.hadoop.mapred.JobConf]] with
    * the given inputPath is created.
    */
  def readHadoopFile[K, V](
                            mapredInputFormat: MapredFileInputFormat[K, V],
                            key: Class[K],
                            value: Class[V],
                            inputPath: String)
                          (implicit tpe:
                          TypeInformation[(K, V)]): mapred.HadoopInputFormat[K, V] = {
    readHadoopFile(mapredInputFormat, key, value, inputPath, new JobConf)
  }

  /**
    * Creates a [[[org.apache.flink.api.scala.hadoop.mapred.HadoopInputFormat]] from key
    * [[java.lang.Class]] and value [[java.lang.Class]] A [[org.apache.hadoop.mapred.JobConf]]
    * with the given inputPath is created.
    */
  def readSequenceFile[K, V](
                              key: Class[K],
                              value: Class[V],
                              inputPath: String)
                            (implicit tpe:
                            TypeInformation[(K, V)]): mapred.HadoopInputFormat[K, V] = {
    readHadoopFile(new org.apache.hadoop.mapred.SequenceFileInputFormat[K, V],
      key, value, inputPath)
  }

  /**
    * Creates a [[org.apache.flink.api.scala.hadoop.mapred.HadoopInputFormat]] from the given
    * [[org.apache.hadoop.mapred.InputFormat]].
    */
  def createHadoopInput[K, V](
                               mapredInputFormat: MapredInputFormat[K, V],
                               key: Class[K],
                               value: Class[V],
                               job: JobConf)
                             (implicit tpe:
                             TypeInformation[(K, V)]): mapred.HadoopInputFormat[K, V] = {
    new mapred.HadoopInputFormat[K, V](mapredInputFormat, key, value, job)
  }

  /**
    * Creates a [[org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat]] from the given
    * [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]].
    * The given inputName is set on the given job.
    */
  def readHadoopFile[K, V](
                            mapreduceInputFormat: MapreduceFileInputFormat[K, V],
                            key: Class[K],
                            value: Class[V],
                            inputPath: String,
                            job: Job)
                          (implicit tpe:
                          TypeInformation[(K, V)]): mapreduce.HadoopInputFormat[K, V] = {
    MapreduceFileInputFormat.addInputPath(job, new HadoopPath(inputPath))
    createHadoopInput(mapreduceInputFormat, key, value, job)
  }

  /**
    * Creates a [[org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat]] from the given
    * [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]]. A
    * [[org.apache.hadoop.mapreduce.Job]] with the given inputPath will be created.
    */
  def readHadoopFile[K, V](
                            mapreduceInputFormat: MapreduceFileInputFormat[K, V],
                            key: Class[K],
                            value: Class[V],
                            inputPath: String)
                          (implicit tpe:
                          TypeInformation[(K, V)]): mapreduce.HadoopInputFormat[K, V]  = {
    readHadoopFile(mapreduceInputFormat, key, value, inputPath, Job.getInstance)
  }

  /**
    * Creates a [[org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat]] from the given
    * [[org.apache.hadoop.mapreduce.InputFormat]].
    */
  def createHadoopInput[K, V](
                               mapreduceInputFormat: MapreduceInputFormat[K, V],
                               key: Class[K],
                               value: Class[V],
                               job: Job)
                             (implicit tpe:
                             TypeInformation[(K, V)]): mapreduce.HadoopInputFormat[K, V] = {
    new mapreduce.HadoopInputFormat[K, V](mapreduceInputFormat, key, value, job)
  }
}

