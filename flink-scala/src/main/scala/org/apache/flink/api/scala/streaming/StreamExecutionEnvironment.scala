/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.scala.streaming

import org.apache.flink.streaming.api.environment.{ StreamExecutionEnvironment => JavaEnv }
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.commons.lang.Validate
import scala.reflect.ClassTag
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.invokable.SourceInvokable
import org.apache.flink.streaming.api.function.source.FromElementsFunction

class StreamExecutionEnvironment(javaEnv: JavaEnv) {

  /**
   * Sets the degree of parallelism (DOP) for operations executed through this environment.
   * Setting a DOP of x here will cause all operators (such as join, map, reduce) to run with
   * x parallel instances. This value can be overridden by specific operations using
   * [[DataStream.setParallelism]].
   */
  def setDegreeOfParallelism(degreeOfParallelism: Int): Unit = {
    javaEnv.setDegreeOfParallelism(degreeOfParallelism)
  }

  /**
   * Returns the default degree of parallelism for this execution environment. Note that this
   * value can be overridden by individual operations using [[DataStream.setParallelism]]
   */
  def getDegreeOfParallelism = javaEnv.getDegreeOfParallelism

  def generateSequence(from: Long, to: Long): DataStream[java.lang.Long] = new DataStream(javaEnv.generateSequence(from, to))

  /**
   * Creates a new data stream that contains the given elements. The elements must all be of the
   * same type and must be serializable.
   *
   * * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a degree of parallelism of one.
   */
  def fromElements[T: ClassTag: TypeInformation](data: T*): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    fromCollection(data)(implicitly[ClassTag[T]], typeInfo)
  }

  /**
   * Creates a DataStream from the given non-empty [[Seq]]. The elements need to be serializable
   * because the framework may move the elements into the cluster if needed.
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a degree of parallelism of one.
   */
  def fromCollection[T: ClassTag: TypeInformation](
    data: Seq[T]): DataStream[T] = {
    Validate.notNull(data, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]
    val returnStream = new DataStreamSource[T](javaEnv,
      "elements", typeInfo);

    javaEnv.getJobGraphBuilder.addStreamVertex(returnStream.getId(),
      new SourceInvokable[T](new FromElementsFunction[T](scala.collection.JavaConversions.asJavaCollection(data))), null, typeInfo,
      "source", 1);
    new DataStream(returnStream)
  }

  def execute() = javaEnv.execute()

}

object StreamExecutionEnvironment {

  /**
   * Creates an execution environment that represents the context in which the program is
   * currently executed. If the program is invoked standalone, this method returns a local
   * execution environment. If the program is invoked from within the command line client
   * to be submitted to a cluster, this method returns the execution environment of this cluster.
   */
  def getExecutionEnvironment: StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.getExecutionEnvironment)
  }

  /**
   * Creates a local execution environment. The local execution environment will run the program in
   * a multi-threaded fashion in the same JVM as the environment was created in. The default degree
   * of parallelism of the local environment is the number of hardware contexts (CPU cores/threads).
   */
  def createLocalEnvironment(
    degreeOfParallelism: Int = Runtime.getRuntime.availableProcessors()): StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironment(degreeOfParallelism))
  }

  /**
   * Creates a remote execution environment. The remote environment sends (parts of) the program to
   * a cluster for execution. Note that all file paths used in the program must be accessible from
   * the cluster. The execution will use the cluster's default degree of parallelism, unless the
   * parallelism is set explicitly via [[ExecutionEnvironment.setDegreeOfParallelism()]].
   *
   * @param host The host name or address of the master (JobManager),
   *             where the program should be executed.
   * @param port The port of the master (JobManager), where the program should be executed.
   * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the
   *                 program uses
   *                 user-defined functions, user-defined input formats, or any libraries,
   *                 those must be
   *                 provided in the JAR files.
   */
  def createRemoteEnvironment(host: String, port: Int, jarFiles: String*): StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*))
  }

  /**
   * Creates a remote execution environment. The remote environment sends (parts of) the program
   * to a cluster for execution. Note that all file paths used in the program must be accessible
   * from the cluster. The execution will use the specified degree of parallelism.
   *
   * @param host The host name or address of the master (JobManager),
   *             where the program should be executed.
   * @param port The port of the master (JobManager), where the program should be executed.
   * @param degreeOfParallelism The degree of parallelism to use during the execution.
   * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the
   *                 program uses
   *                 user-defined functions, user-defined input formats, or any libraries,
   *                 those must be
   *                 provided in the JAR files.
   */
  def createRemoteEnvironment(
    host: String,
    port: Int,
    degreeOfParallelism: Int,
    jarFiles: String*): StreamExecutionEnvironment = {
    val javaEnv = JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*)
    javaEnv.setDegreeOfParallelism(degreeOfParallelism)
    new StreamExecutionEnvironment(javaEnv)
  }
}