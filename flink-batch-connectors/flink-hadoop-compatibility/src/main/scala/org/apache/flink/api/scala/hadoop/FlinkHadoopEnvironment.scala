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
package org.apache.flink.api.scala.hadoop

import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.hadoop.mapred.{HadoopInputFormat => MapredHadoopInputFormat}
import org.apache.flink.api.scala.hadoop.mapreduce.{HadoopInputFormat => MapreduceHadoopInputFormat}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.mapred.{JobConf, FileInputFormat => MapredFileInputFormat, InputFormat => MapredInputFormat}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => MapreduceFileInputFormat}
import org.apache.hadoop.mapreduce.{Job, InputFormat => MapreduceInputFormat}

/**
 * The FlinkHadoopEnvironment is the context in which a program is executed, connected with hadoop.
 *
 * The environment provides methods to interact with the hadoop cluster (data access).
 *
 *  Use [[FlinkHadoopEnvironment#getHadoopEnvironment]] to get the correct environment.
 */
@Public
class FlinkHadoopEnvironment(parentEnv: ExecutionEnvironment) {

  /**
   * @return the Execution environment.
   */
  def getParentEnv: ExecutionEnvironment = parentEnv

  /**
    * Creates a [[DataSet]] from the given [[org.apache.hadoop.mapred.FileInputFormat]]. The
    * given inputName is set on the given job.
    */
  @PublicEvolving
  def readHadoopFile[K, V](
                            mapredInputFormat: MapredFileInputFormat[K, V],
                            key: Class[K],
                            value: Class[V],
                            inputPath: String,
                            job: JobConf)
                          (implicit tpe: TypeInformation[(K, V)]): DataSet[(K, V)] = {
    val result = createHadoopInput(mapredInputFormat, key, value, job)
    MapredFileInputFormat.addInputPath(job, new HadoopPath(inputPath))
    result
  }

  /**
    * Creates a [[DataSet]] from the given [[org.apache.hadoop.mapred.FileInputFormat]]. A
    * [[org.apache.hadoop.mapred.JobConf]] with the given inputPath is created.
    */
  @PublicEvolving
  def readHadoopFile[K, V](
                            mapredInputFormat: MapredFileInputFormat[K, V],
                            key: Class[K],
                            value: Class[V],
                            inputPath: String)
                          (implicit tpe: TypeInformation[(K, V)]): DataSet[(K, V)] = {
    readHadoopFile(mapredInputFormat, key, value, inputPath, new JobConf)
  }

  /**
    * Creates a [[DataSet]] from [[org.apache.hadoop.mapred.SequenceFileInputFormat]]
    * A [[org.apache.hadoop.mapred.JobConf]] with the given inputPath is created.
    */
  @PublicEvolving
  def readSequenceFile[K, V](
                              key: Class[K],
                              value: Class[V],
                              inputPath: String)
                            (implicit tpe: TypeInformation[(K, V)]): DataSet[(K, V)] = {
    readHadoopFile(new org.apache.hadoop.mapred.SequenceFileInputFormat[K, V],
      key, value, inputPath)
  }

  /**
    * Creates a [[DataSet]] from the given [[org.apache.hadoop.mapred.InputFormat]].
    */
  @PublicEvolving
  def createHadoopInput[K, V](
                               mapredInputFormat: MapredInputFormat[K, V],
                               key: Class[K],
                               value: Class[V],
                               job: JobConf)
                             (implicit tpe: TypeInformation[(K, V)]): DataSet[(K, V)] = {
    val hadoopInputFormat = new mapred.HadoopInputFormat[K, V](mapredInputFormat, key, value, job)
    getParentEnv.createInput(hadoopInputFormat)
  }

  /**
    * Creates a [[DataSet]] from the given
    * [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]].
    * The given inputName is set on the given job.
    */
  @PublicEvolving
  def readHadoopFile[K, V](
                            mapreduceInputFormat: MapreduceFileInputFormat[K, V],
                            key: Class[K],
                            value: Class[V],
                            inputPath: String,
                            job: Job)
                          (implicit tpe: TypeInformation[(K, V)]): DataSet[(K, V)] = {
    val result = createHadoopInput(mapreduceInputFormat, key, value, job)
    MapreduceFileInputFormat.addInputPath(job, new HadoopPath(inputPath))
    result
  }

  /**
    * Creates a [[DataSet]] from the given
    * [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]]. A
    * [[org.apache.hadoop.mapreduce.Job]] with the given inputPath will be created.
    */
  @PublicEvolving
  def readHadoopFile[K, V](
                            mapreduceInputFormat: MapreduceFileInputFormat[K, V],
                            key: Class[K],
                            value: Class[V],
                            inputPath: String)
                          (implicit tpe: TypeInformation[(K, V)]): DataSet[Tuple2[K, V]] = {
    readHadoopFile(mapreduceInputFormat, key, value, inputPath, Job.getInstance)
  }

  /**
    * Creates a [[DataSet]] from the given [[org.apache.hadoop.mapreduce.InputFormat]].
    */
  @PublicEvolving
  def createHadoopInput[K, V](
                               mapreduceInputFormat: MapreduceInputFormat[K, V],
                               key: Class[K],
                               value: Class[V],
                               job: Job)
                             (implicit tpe: TypeInformation[(K, V)]): DataSet[Tuple2[K, V]] = {
    val hadoopInputFormat =
      new mapreduce.HadoopInputFormat[K, V](mapreduceInputFormat, key, value, job)
    getParentEnv.createInput(hadoopInputFormat)
  }
}

@Public
object FlinkHadoopEnvironment {

  /**
   * Creates a hadoop environment that represents the context in which the program is
   * currently executed.
   */
  def getHadoopEnvironment(env: ExecutionEnvironment): FlinkHadoopEnvironment = {
    new FlinkHadoopEnvironment(env)
  }

}

