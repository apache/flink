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

package org.apache.flink.streaming.api.scala

import com.esotericsoftware.kryo.Serializer
import org.apache.flink.api.common.io.{FileInputFormat, InputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.scala.ClosureCleaner
import org.apache.flink.runtime.state.StateHandleProvider
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType
import org.apache.flink.streaming.api.functions.source.{FromElementsFunction, SourceFunction}
import org.apache.flink.types.StringValue
import org.apache.flink.util.SplittableIterator

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class StreamExecutionEnvironment(javaEnv: JavaEnv) {

  /**
   * Gets the config object.
   */
  def getConfig = javaEnv.getConfig

  /**
   * Sets the parallelism for operations executed through this environment.
   * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run
   * with x parallel instances. This value can be overridden by specific operations using
   * [[DataStream.setParallelism]].
   * @deprecated Please use [[setParallelism]]
   */
  @deprecated
  def setDegreeOfParallelism(degreeOfParallelism: Int): Unit = {
    javaEnv.setParallelism(degreeOfParallelism)
  }

  /**
   * Returns the default parallelism for this execution environment. Note that this
   * value can be overridden by individual operations using [[DataStream.setParallelism]]
   * @deprecated Please use [[getParallelism]]
   */
  @deprecated
  def getDegreeOfParallelism = javaEnv.getParallelism

  /**
   * Sets the parallelism for operations executed through this environment.
   * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run
   * with x parallel instances. This value can be overridden by specific operations using
   * [[DataStream.setParallelism]].
   */
  def setParallelism(parallelism: Int): Unit = {
    javaEnv.setParallelism(parallelism)
  }

  /**
   * Returns the default parallelism for this execution environment. Note that this
   * value can be overridden by individual operations using [[DataStream.setParallelism]]
   */
  def getParallelism = javaEnv.getParallelism

  /**
   * Sets the maximum time frequency (milliseconds) for the flushing of the
   * output buffers. By default the output buffers flush frequently to provide
   * low latency and to aid smooth developer experience. Setting the parameter
   * can result in three logical modes:
   *
   * <ul>
   * <li>
   * A positive integer triggers flushing periodically by that integer</li>
   * <li>
   * 0 triggers flushing after every record thus minimizing latency</li>
   * <li>
   * -1 triggers flushing only when the output buffer is full thus maximizing
   * throughput</li>
   * </ul>
   *
   */
  def setBufferTimeout(timeoutMillis: Long): StreamExecutionEnvironment = {
    javaEnv.setBufferTimeout(timeoutMillis)
    this
  }

  /**
   * Gets the default buffer timeout set for this environment
   */
  def getBufferTimeout = javaEnv.getBufferTimeout

  /**
   * Disables operator chaining for streaming operators. Operator chaining
   * allows non-shuffle operations to be co-located in the same thread fully
   * avoiding serialization and de-serialization.
   *
   */
  def disableOperatorChaining(): StreamExecutionEnvironment = {
    javaEnv.disableOperatorChaining()
    this
  }

  /**
   * Method for enabling fault-tolerance. Activates monitoring and backup of streaming
   * operator states. Time interval between state checkpoints is specified in in millis.
   *
   * Setting this option assumes that the job is used in production and thus if not stated
   * explicitly otherwise with calling with the
   * {@link #setNumberOfExecutionRetries(int numberOfExecutionRetries)} method in case of
   * failure the job will be resubmitted to the cluster indefinitely.
   */
  def enableCheckpointing(interval : Long) : StreamExecutionEnvironment = {
    javaEnv.enableCheckpointing(interval)
    this
  }

  /**
   * Method for enabling fault-tolerance. Activates monitoring and backup of streaming
   * operator states. Time interval between state checkpoints is specified in in millis.
   *
   * Setting this option assumes that the job is used in production and thus if not stated
   * explicitly otherwise with calling with the
   * {@link #setNumberOfExecutionRetries(int numberOfExecutionRetries)} method in case of
   * failure the job will be resubmitted to the cluster indefinitely.
   */
  def enableCheckpointing() : StreamExecutionEnvironment = {
    javaEnv.enableCheckpointing()
    this
  }

  /**
   * Sets the given StateHandleProvider to be used for storing operator state
   * checkpoints when checkpointing is enabled.
   */
  def setStateHandleProvider(provider: StateHandleProvider[_]): StreamExecutionEnvironment = {
    javaEnv.setStateHandleProvider(provider)
    this
  }

  /**
   * Sets the number of times that failed tasks are re-executed. A value of zero
   * effectively disables fault tolerance. A value of "-1" indicates that the system
   * default value (as defined in the configuration) should be used.
   */
  def setNumberOfExecutionRetries(numRetries: Int): Unit = {
    javaEnv.setNumberOfExecutionRetries(numRetries)
  }

  /**
   * Gets the number of times the system will try to re-execute failed tasks. A value
   * of "-1" indicates that the system default value (as defined in the configuration)
   * should be used.
   */
  def getNumberOfExecutionRetries = javaEnv.getNumberOfExecutionRetries

  // --------------------------------------------------------------------------------------------
  // Registry for types and serializers
  // --------------------------------------------------------------------------------------------
  /**
   * Adds a new Kryo default serializer to the Runtime.
   * <p/>
   * Note that the serializer instance must be serializable (as defined by
   * java.io.Serializable), because it may be distributed to the worker nodes
   * by java serialization.
   *
   * @param type
   * The class of the types serialized with the given serializer.
   * @param serializer
   * The serializer to use.
   */
  def addDefaultKryoSerializer(`type`: Class[_], serializer: Serializer[_]) {
    javaEnv.addDefaultKryoSerializer(`type`, serializer)
  }

  /**
   * Adds a new Kryo default serializer to the Runtime.
   *
   * @param type
   * The class of the types serialized with the given serializer.
   * @param serializerClass
   * The class of the serializer to use.
   */
  def addDefaultKryoSerializer(`type`: Class[_], serializerClass: Class[_ <: Serializer[_]]) {
    javaEnv.addDefaultKryoSerializer(`type`, serializerClass)
  }

  /**
   * Registers the given type with the serializer at the [[KryoSerializer]].
   *
   * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
   * because it may be distributed to the worker nodes by java serialization.
   */
  def registerTypeWithKryoSerializer(clazz: Class[_], serializer: Serializer[_]): Unit = {
    javaEnv.registerTypeWithKryoSerializer(clazz, serializer)
  }

  /**
   * Registers the given type with the serializer at the [[KryoSerializer]].
   */
  def registerTypeWithKryoSerializer(clazz: Class[_], serializer: Class[_ <: Serializer[_]]) {
    javaEnv.registerTypeWithKryoSerializer(clazz, serializer)
  }

  /**
   * Registers the given type with the serialization stack. If the type is eventually
   * serialized as a POJO, then the type is registered with the POJO serializer. If the
   * type ends up being serialized with Kryo, then it will be registered at Kryo to make
   * sure that only tags are written.
   *
   */
  def registerType(typeClass: Class[_]) {
    javaEnv.registerType(typeClass)
  }

  // --------------------------------------------------------------------------------------------
  // Data stream creations
  // --------------------------------------------------------------------------------------------

  /**
   * Creates a new DataStream that contains a sequence of numbers.
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
   */
  def generateSequence(from: Long, to: Long): DataStream[Long] = {
    new DataStream[java.lang.Long](javaEnv.generateSequence(from, to)).
      asInstanceOf[DataStream[Long]]
  }

  /**
   * Creates a new DataStream that contains a sequence of numbers in a parallel fashion.
   */
  def generateParallelSequence(from: Long, to: Long): DataStream[Long] = {
    new DataStream[java.lang.Long](javaEnv.generateParallelSequence(from, to)).
      asInstanceOf[DataStream[Long]]
  }

  /**
   * Creates a DataStream that contains the given elements. The elements must all be of the
   * same type.
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
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
   * a parallelism of one.
   */
  def fromCollection[T: ClassTag: TypeInformation](data: Seq[T]): DataStream[T] = {
    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]

    val sourceFunction = new FromElementsFunction[T](scala.collection.JavaConversions
      .asJavaCollection(data))

    javaEnv.addSource(sourceFunction).returns(typeInfo)
  }

  /**
   * Creates a DataStream from the given [[Iterator]].
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
   */
  def fromCollection[T: ClassTag : TypeInformation] (data: Iterator[T]): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    javaEnv.fromCollection(data.asJava, typeInfo)
  }

  /**
   * Creates a DataStream from the given [[SplittableIterator]].
   */
  def fromParallelCollection[T: ClassTag : TypeInformation] (data: SplittableIterator[T]):
  DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    javaEnv.fromParallelCollection(data, typeInfo)
  }

  /**
   * Creates a DataStream that represents the Strings produced by reading the
   * given file line wise. The file will be read with the system's default
   * character set.
   *
   */
  def readTextFile(filePath: String): DataStream[String] =
    javaEnv.readTextFile(filePath)

  /**
   * Creates a data stream that represents the Strings produced by reading the given file
   * line wise. The character set with the given name will be used to read the files.
   */
  def readTextFile(filePath: String, charsetName: String): DataStream[String] =
    javaEnv.readTextFile(filePath, charsetName)

  /**
   * Creates a data stream that represents the strings produced by reading the given file
   * line wise. This method is similar to the standard text file reader, but it produces
   * a data stream with mutable StringValue objects, rather than Java Strings.
   * StringValues can be used to tune implementations to be less object and garbage
   * collection heavy. The file will be read with the system's default character set.
   */
  def readTextFileWithValue(filePath: String): DataStream[StringValue] =
      javaEnv.readTextFileWithValue(filePath)

  /**
   * Creates a data stream that represents the strings produced by reading the given file
   * line wise. This method is similar to the standard text file reader, but it produces
   * a data stream with mutable StringValue objects, rather than Java Strings.
   * StringValues can be used to tune implementations to be less object and garbage
   * collection heavy. The boolean flag indicates whether to skip lines that cannot
   * be read with the given character set.
   */
  def readTextFileWithValue(filePath: String, charsetName : String, skipInvalidLines : Boolean):
    DataStream[StringValue] =
    javaEnv.readTextFileWithValue(filePath, charsetName, skipInvalidLines)

  /**
   * Reads the given file with the given input format. The file path should be passed
   * as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
   */
  def readFile[T: ClassTag : TypeInformation](inputFormat: FileInputFormat[T], filePath: String):
    DataStream[T] =
    javaEnv.readFile(inputFormat, filePath)

  /**
   * Creates a data stream that represents the primitive type produced by reading the given file
   * line wise. The file path should be passed as a URI (e.g., "file:///some/local/file" or
   * "hdfs://host:port/file/path").
   */
  def readFileOfPrimitives[T: ClassTag : TypeInformation](filePath: String,
    delimiter: String = "\n", typeClass: Class[T]): DataStream[T] =
    javaEnv.readFileOfPrimitives(filePath, delimiter, typeClass)

  /**
   * Creates a DataStream that contains the contents of file created while
   * system watches the given path. The file will be read with the system's
   * default character set. The user can check the monitoring interval in milliseconds,
   * and the way file modifications are handled. By default it checks for only new files
   * every 100 milliseconds.
   *
   */
  def readFileStream(StreamPath: String, intervalMillis: Long = 100, watchType: WatchType = 
    WatchType.ONLY_NEW_FILES): DataStream[String] =
    javaEnv.readFileStream(StreamPath, intervalMillis, watchType)

  /**
   * Creates a new DataStream that contains the strings received infinitely
   * from socket. Received strings are decoded by the system's default
   * character set. The maximum retry interval is specified in seconds, in case
   * of temporary service outage reconnection is initiated every second.
   */
  def socketTextStream(hostname: String, port: Int, delimiter: Char = '\n', maxRetry: Long = 0):
    DataStream[String] =
    javaEnv.socketTextStream(hostname, port)

  /**
   * Generic method to create an input data stream with a specific input format.
   * Since all data streams need specific information about their types, this method needs to
   * determine the type of the data produced by the input format. It will attempt to determine the
   * data type by reflection, unless the input format implements the ResultTypeQueryable interface.
   */
  def createInput[T: ClassTag : TypeInformation](inputFormat: InputFormat[T, _]): DataStream[T] =
    javaEnv.createInput(inputFormat)

  /**
   * Create a DataStream using a user defined source function for arbitrary
   * source functionality. By default sources have a parallelism of 1. 
   * To enable parallel execution, the user defined source should implement 
   * ParallelSourceFunction or extend RichParallelSourceFunction. 
   * In these cases the resulting source will have the parallelism of the environment. 
   * To change this afterwards call DataStreamSource.setParallelism(int)
   *
   */
  def addSource[T: ClassTag: TypeInformation](function: SourceFunction[T]): DataStream[T] = {
    require(function != null, "Function must not be null.")
    val cleanFun = StreamExecutionEnvironment.clean(function)
    val typeInfo = implicitly[TypeInformation[T]]
    javaEnv.addSource(cleanFun).returns(typeInfo)
  }
  
   /**
   * Create a DataStream using a user defined source function for arbitrary
   * source functionality.
   *
   */
  def addSource[T: ClassTag: TypeInformation](function: () => T): DataStream[T] = {
    require(function != null, "Function must not be null.")
    val sourceFunction = new SourceFunction[T] {
      val cleanFun = StreamExecutionEnvironment.clean(function)

      override def reachedEnd(): Boolean = false

      override def next(): T = cleanFun()
    }
    addSource(sourceFunction)
  }

  /**
   * Triggers the program execution. The environment will execute all parts of
   * the program that have resulted in a "sink" operation. Sink operations are
   * for example printing results or forwarding them to a message queue.
   * <p>
   * The program execution will be logged and displayed with a generated
   * default name.
   *
   */
  def execute() = javaEnv.execute()

  /**
   * Triggers the program execution. The environment will execute all parts of
   * the program that have resulted in a "sink" operation. Sink operations are
   * for example printing results or forwarding them to a message queue.
   * <p>
   * The program execution will be logged and displayed with the provided name
   *
   */
  def execute(jobName: String) = javaEnv.execute(jobName)

  /**
   * Creates the plan with which the system will execute the program, and
   * returns it as a String using a JSON representation of the execution data
   * flow graph. Note that this needs to be called, before the plan is
   * executed.
   *
   */
  def getExecutionPlan = javaEnv.getExecutionPlan

  /**
   * Getter of the {@link org.apache.flink.streaming.api.graph.StreamGraph} of the streaming job.
   *
   * @return The StreamGraph representing the transformations
   */
  def getStreamGraph = javaEnv.getStreamGraph

}

object StreamExecutionEnvironment {
  
  private[flink] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  /**
   * Sets the default parallelism that will be used for the local execution
   * environment created by {@link #createLocalEnvironment()}.
   *
   * @param parallelism
   * The parallelism to use as the default local parallelism.
   */
  def setDefaultLocalParallelism(parallelism: Int) : Unit =
    StreamExecutionEnvironment.setDefaultLocalParallelism(parallelism)

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
    parallelism: Int =  Runtime.getRuntime.availableProcessors()):
  StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironment(parallelism))
  }

  /**
   * Creates a remote execution environment. The remote environment sends (parts of) the program to
   * a cluster for execution. Note that all file paths used in the program must be accessible from
   * the cluster. The execution will use the cluster's default parallelism, unless the
   * parallelism is set explicitly via [[StreamExecutionEnvironment.setParallelism()]].
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
  def createRemoteEnvironment(host: String, port: Int, jarFiles: String*):
  StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*))
  }

  /**
   * Creates a remote execution environment. The remote environment sends (parts of) the program
   * to a cluster for execution. Note that all file paths used in the program must be accessible
   * from the cluster. The execution will use the specified parallelism.
   *
   * @param host The host name or address of the master (JobManager),
   *             where the program should be executed.
   * @param port The port of the master (JobManager), where the program should be executed.
   * @param parallelism The parallelism to use during the execution.
   * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the
   *                 program uses
   *                 user-defined functions, user-defined input formats, or any libraries,
   *                 those must be
   *                 provided in the JAR files.
   */
  def createRemoteEnvironment(
    host: String,
    port: Int,
    parallelism: Int,
    jarFiles: String*): StreamExecutionEnvironment = {
    val javaEnv = JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*)
    javaEnv.setParallelism(parallelism)
    new StreamExecutionEnvironment(javaEnv)
  }
}
