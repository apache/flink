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
import org.apache.flink.api.scala.hadoop.{mapred, mapreduce}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.mapred.{JobConf, FileInputFormat => MapredFileInputFormat, InputFormat => MapredInputFormat}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => MapreduceFileInputFormat}
import org.apache.hadoop.mapreduce.{Job, InputFormat => MapreduceInputFormat}

/**
  * HadoopInputs is a utility class to use Apache Hadoop InputFormats with Apache Flink.
  *
  * It provides methods to create Flink InputFormat wrappers for Hadoop
  * [[org.apache.hadoop.mapred.InputFormat]] and [[org.apache.hadoop.mapreduce.InputFormat]].
  *
  * Key value pairs produced by the Hadoop InputFormats are converted into [[Tuple2]] where
  * the first field is the key and the second field is the value.
  *
  */
object HadoopInputs {

  /**
    * Creates a Flink [[org.apache.flink.api.common.io.InputFormat]] that wraps the given Hadoop
    * [[org.apache.hadoop.mapred.FileInputFormat]].
    */
  def readHadoopFile[K, V](
      mapredInputFormat: MapredFileInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      inputPath: String,
      job: JobConf)(implicit tpe: TypeInformation[(K, V)]): mapred.HadoopInputFormat[K, V] = {

    // set input path in JobConf
    MapredFileInputFormat.addInputPath(job, new HadoopPath(inputPath))
    // wrap mapredInputFormat
    createHadoopInput(mapredInputFormat, key, value, job)
  }

  /**
    * Creates a Flink [[org.apache.flink.api.common.io.InputFormat]] that wraps the given Hadoop
    * [[org.apache.hadoop.mapred.FileInputFormat]].
    */
  def readHadoopFile[K, V](
      mapredInputFormat: MapredFileInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      inputPath: String)(implicit tpe: TypeInformation[(K, V)]): mapred.HadoopInputFormat[K, V] = {

    readHadoopFile(mapredInputFormat, key, value, inputPath, new JobConf)
  }

  /**
    * Creates a Flink [[org.apache.flink.api.common.io.InputFormat]] that reads a Hadoop sequence
    * file with the given key and value classes.
    */
  def readSequenceFile[K, V](
      key: Class[K],
      value: Class[V],
      inputPath: String)(implicit tpe: TypeInformation[(K, V)]): mapred.HadoopInputFormat[K, V] = {

    readHadoopFile(
      new org.apache.hadoop.mapred.SequenceFileInputFormat[K, V],
      key,
      value,
      inputPath
   )
  }

  /**
    * Creates a Flink [[org.apache.flink.api.common.io.InputFormat]] that wraps the given Hadoop
    * [[org.apache.hadoop.mapred.InputFormat]].
    */
  def createHadoopInput[K, V](
      mapredInputFormat: MapredInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      job: JobConf)(implicit tpe: TypeInformation[(K, V)]): mapred.HadoopInputFormat[K, V] = {

    new mapred.HadoopInputFormat[K, V](mapredInputFormat, key, value, job)
  }

  /**
    * Creates a Flink [[org.apache.flink.api.common.io.InputFormat]] that wraps the given Hadoop
    * [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]].
    */
  def readHadoopFile[K, V](
      mapreduceInputFormat: MapreduceFileInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      inputPath: String,
      job: Job)(implicit tpe: TypeInformation[(K, V)]): mapreduce.HadoopInputFormat[K, V] = {

    // set input path in Job
    MapreduceFileInputFormat.addInputPath(job, new HadoopPath(inputPath))
    // wrap mapreduceInputFormat
    createHadoopInput(mapreduceInputFormat, key, value, job)
  }

  /**
    * Creates a Flink [[org.apache.flink.api.common.io.InputFormat]] that wraps the given Hadoop
    * [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]].
    */
  def readHadoopFile[K, V](
      mapreduceInputFormat: MapreduceFileInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      inputPath: String)(implicit tpe: TypeInformation[(K, V)]): mapreduce.HadoopInputFormat[K, V] =
  {
    readHadoopFile(mapreduceInputFormat, key, value, inputPath, Job.getInstance)
  }

  /**
    * Creates a Flink [[org.apache.flink.api.common.io.InputFormat]] that wraps the given Hadoop
    * [[org.apache.hadoop.mapreduce.InputFormat]].
    */
  def createHadoopInput[K, V](
      mapreduceInputFormat: MapreduceInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      job: Job)(implicit tpe: TypeInformation[(K, V)]): mapreduce.HadoopInputFormat[K, V] = {

    new mapreduce.HadoopInputFormat[K, V](mapreduceInputFormat, key, value, job)
  }
}

