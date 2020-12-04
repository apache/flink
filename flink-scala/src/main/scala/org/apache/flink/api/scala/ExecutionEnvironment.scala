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
package org.apache.flink.api.scala

import com.esotericsoftware.kryo.Serializer
import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.io.{FileInputFormat, InputFormat}
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.{ExecutionConfig, JobExecutionResult}
import org.apache.flink.api.java.io._
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfoBase, ValueTypeInfo}
import org.apache.flink.api.java.{CollectionEnvironment, ExecutionEnvironment => JavaEnv}
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.core.execution.{JobClient, JobListener, PipelineExecutor}
import org.apache.flink.core.fs.Path
import org.apache.flink.types.StringValue
import org.apache.flink.util.{NumberSequenceIterator, Preconditions, SplittableIterator}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * The ExecutionEnvironment is the context in which a program is executed. A local environment will
 * cause execution in the current JVM, a remote environment will cause execution on a remote
 * cluster installation.
 *
 * The environment provides methods to control the job execution (such as setting the parallelism)
 * and to interact with the outside world (data access).
 *
 * To get an execution environment use the methods on the companion object:
 *
 *  - [[ExecutionEnvironment#getExecutionEnvironment]]
 *  - [[ExecutionEnvironment#createLocalEnvironment]]
 *  - [[ExecutionEnvironment#createRemoteEnvironment]]
 *
 *  Use [[ExecutionEnvironment#getExecutionEnvironment]] to get the correct environment depending
 *  on where the program is executed. If it is run inside an IDE a local environment will be
 *  created. If the program is submitted to a cluster a remote execution environment will
 *  be created.
 */
@Public
class ExecutionEnvironment(javaEnv: JavaEnv) {

  /**
   * @return the Java Execution environment.
   */
  def getJavaEnv: JavaEnv = javaEnv

  /**
   * Gets the config object.
   */
  def getConfig: ExecutionConfig = {
    javaEnv.getConfig
  }

  /**
   * Sets the parallelism (parallelism) for operations executed through this environment.
   * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run
   * with x parallel instances. This value can be overridden by specific operations using
   * [[DataSet.setParallelism]].
   */
  def setParallelism(parallelism: Int): Unit = {
    javaEnv.setParallelism(parallelism)
  }

  /**
   * Returns the default parallelism for this execution environment. Note that this
   * value can be overridden by individual operations using [[DataSet.setParallelism]]
   */
  def getParallelism = javaEnv.getParallelism

  /**
    * Sets the restart strategy configuration. The configuration specifies which restart strategy
    * will be used for the execution graph in case of a restart.
    *
    * @param restartStrategyConfiguration Restart strategy configuration to be set
    */
  @PublicEvolving
  def setRestartStrategy(restartStrategyConfiguration: RestartStrategyConfiguration): Unit = {
    javaEnv.setRestartStrategy(restartStrategyConfiguration)
  }

  /**
    * Returns the specified restart strategy configuration.
    *
    * @return The restart strategy configuration to be used
    */
  @PublicEvolving
  def getRestartStrategy: RestartStrategyConfiguration = {
    javaEnv.getRestartStrategy
  }

  /**
    * Sets the number of times that failed tasks are re-executed. A value of zero
    * effectively disables fault tolerance. A value of "-1" indicates that the system
    * default value (as defined in the configuration) should be used.
    *
    * @deprecated This method will be replaced by [[setRestartStrategy()]]. The
    *            FixedDelayRestartStrategyConfiguration contains the number of execution retries.
    */
  @Deprecated
  @PublicEvolving
  def setNumberOfExecutionRetries(numRetries: Int): Unit = {
    javaEnv.setNumberOfExecutionRetries(numRetries)
  }

  /**
    * Gets the number of times the system will try to re-execute failed tasks. A value
    * of "-1" indicates that the system default value (as defined in the configuration)
    * should be used.
    *
    * @deprecated This method will be replaced by [[getRestartStrategy]]. The
    *            FixedDelayRestartStrategyConfiguration contains the number of execution retries.
    */
  @Deprecated
  @PublicEvolving
  def getNumberOfExecutionRetries = javaEnv.getNumberOfExecutionRetries

  /**
   * Gets the JobExecutionResult of the last executed job.
   */
  def getLastJobExecutionResult = javaEnv.getLastJobExecutionResult

  /**
   * Registers the given type with the serializer at the [[KryoSerializer]].
   *
   * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
   * because it may be distributed to the worker nodes by java serialization.
   */
  def registerTypeWithKryoSerializer[T <: Serializer[_] with Serializable](
      clazz: Class[_],
      serializer: T)
    : Unit = {
    javaEnv.registerTypeWithKryoSerializer(clazz, serializer)
  }

  /**
   * Registers the given type with the serializer at the [[KryoSerializer]].
   */
  def registerTypeWithKryoSerializer(clazz: Class[_], serializer: Class[_ <: Serializer[_]]) {
    javaEnv.registerTypeWithKryoSerializer(clazz, serializer)
  }


  /**
   * Registers a default serializer for the given class and its sub-classes at Kryo.
   */
  def addDefaultKryoSerializer(clazz: Class[_], serializer: Class[_ <: Serializer[_]]) {
    javaEnv.addDefaultKryoSerializer(clazz, serializer)
  }

  /**
   * Registers a default serializer for the given class and its sub-classes at Kryo.
   *
   * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
   * because it may be distributed to the worker nodes by java serialization.
   */
  def addDefaultKryoSerializer[T <: Serializer[_] with Serializable](
      clazz: Class[_],
      serializer: T)
    : Unit = {
    javaEnv.addDefaultKryoSerializer(clazz, serializer)
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

  /**
   * Sets all relevant options contained in the [[ReadableConfig]] such as e.g.
   * [[org.apache.flink.configuration.PipelineOptions#CACHED_FILES]]. It will reconfigure
   * [[ExecutionEnvironment]] and [[ExecutionConfig]].
   *
   * It will change the value of a setting only if a corresponding option was set in the
   * `configuration`. If a key is not present, the current value of a field will remain
   * untouched.
   *
   * @param configuration a configuration to read the values from
   * @param classLoader   a class loader to use when loading classes
   */
  @PublicEvolving
  def configure(configuration: ReadableConfig, classLoader: ClassLoader): Unit = {
    javaEnv.configure(configuration, classLoader)
  }

  /**
   * Creates a DataSet of Strings produced by reading the given file line wise.
   *
   * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or
   *                 "hdfs://host:port/file/path").
   * @param charsetName The name of the character set used to read the file. Default is UTF-8
   */
  def readTextFile(filePath: String, charsetName: String = "UTF-8"): DataSet[String] = {
    require(filePath != null, "The file path may not be null.")
    val format = new TextInputFormat(new Path(filePath))
    format.setCharsetName(charsetName)
    val source = new DataSource[String](javaEnv, format, BasicTypeInfo.STRING_TYPE_INFO,
      getCallLocationName())
    wrap(source)
  }

  /**
   * Creates a DataSet of Strings produced by reading the given file line wise.
   * This method is similar to [[readTextFile]], but it produces a DataSet with mutable
   * [[StringValue]] objects, rather than Java Strings. StringValues can be used to tune
   * implementations to be less object and garbage collection heavy.
   *
   * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or
   *                 "hdfs://host:port/file/path").
   * @param charsetName The name of the character set used to read the file. Default is UTF-8
   */
  def readTextFileWithValue(
      filePath: String,
      charsetName: String = "UTF-8"): DataSet[StringValue] = {
    require(filePath != null, "The file path may not be null.")
    val format = new TextValueInputFormat(new Path(filePath))
    format.setCharsetName(charsetName)
    val source = new DataSource[StringValue](
      javaEnv, format, new ValueTypeInfo[StringValue](classOf[StringValue]), getCallLocationName())
    wrap(source)
  }

  /**
   * Creates a DataSet by reading the given CSV file. The type parameter must be used to specify
   * a Tuple type that has the same number of fields as there are fields in the CSV file. If the
   * number of fields in the CSV file is not the same, the `includedFields` parameter can be used
   * to only read specific fields.
   *
   * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or
   *                 "hdfs://host:port/file/path").
   * @param lineDelimiter The string that separates lines, defaults to newline.
   * @param fieldDelimiter The string that separates individual fields, defaults to ",".
   * @param quoteCharacter The character to use for quoted String parsing, disabled by default.
   * @param ignoreFirstLine Whether the first line in the file should be ignored.
   * @param ignoreComments Lines that start with the given String are ignored, disabled by default.
   * @param lenient Whether the parser should silently ignore malformed lines.
   * @param includedFields The fields in the file that should be read. Per default all fields
   *                       are read.
   * @param pojoFields The fields of the POJO which are mapped to CSV fields.
   */
  def readCsvFile[T : ClassTag : TypeInformation](
      filePath: String,
      lineDelimiter: String = "\n",
      fieldDelimiter: String = ",",
      quoteCharacter: Character = null,
      ignoreFirstLine: Boolean = false,
      ignoreComments: String = null,
      lenient: Boolean = false,
      includedFields: Array[Int] = null,
      pojoFields: Array[String] = null): DataSet[T] = {

    val typeInfo = implicitly[TypeInformation[T]]

    Preconditions.checkArgument(
      typeInfo.isInstanceOf[CompositeType[T]],
      s"The type $typeInfo has to be a tuple or pojo type.",
      null)

    var inputFormat: CsvInputFormat[T] = null

    typeInfo match {
      case info: TupleTypeInfoBase[T] =>
        inputFormat = new TupleCsvInputFormat[T](
          new Path(filePath),
          typeInfo.asInstanceOf[TupleTypeInfoBase[T]],
          includedFields)
      case info: PojoTypeInfo[T] =>
        if (pojoFields == null) {
          throw new IllegalArgumentException(
            "POJO fields must be specified (not null) if output type is a POJO.")
        }
        inputFormat = new PojoCsvInputFormat[T](
          new Path(filePath),
          typeInfo.asInstanceOf[PojoTypeInfo[T]],
          pojoFields,
          includedFields)
      case _ => throw new IllegalArgumentException("Type information is not valid.")
    }
    if (quoteCharacter != null) {
      inputFormat.enableQuotedStringParsing(quoteCharacter)
    }
    inputFormat.setDelimiter(lineDelimiter)
    inputFormat.setFieldDelimiter(fieldDelimiter)
    inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine)
    inputFormat.setLenient(lenient)
    inputFormat.setCommentPrefix(ignoreComments)
    wrap(new DataSource[T](javaEnv, inputFormat, typeInfo, getCallLocationName()))
  }

  /**
   * Creates a DataSet that represents the primitive type produced by reading the
   * given file in delimited way.This method is similar to [[readCsvFile]] with
   * single field, but it produces a DataSet not through Tuple.
   * The type parameter must be used to specify the primitive type.
   *
   * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or
   *                 "hdfs://host:port/file/path").
   * @param delimiter The string that separates primitives , defaults to newline.
   */
  def readFileOfPrimitives[T : ClassTag : TypeInformation](
      filePath : String,
      delimiter : String = "\n") : DataSet[T] = {
    require(filePath != null, "File path must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]
    val datasource = new DataSource[T](
      javaEnv,
      new PrimitiveInputFormat(new Path(filePath), delimiter, typeInfo.getTypeClass),
      typeInfo,
      getCallLocationName())
    wrap(datasource)
  }

  /**
   * Creates a new DataSource by reading the specified file using the custom
   * [[org.apache.flink.api.common.io.FileInputFormat]].
   */
  def readFile[T : ClassTag : TypeInformation](
      inputFormat: FileInputFormat[T],
      filePath: String): DataSet[T] = {
    require(inputFormat != null, "InputFormat must not be null.")
    require(filePath != null, "File path must not be null.")
    inputFormat.setFilePath(new Path(filePath))
    createInput(inputFormat, explicitFirst(inputFormat, implicitly[TypeInformation[T]]))
  }

  /**
   * Generic method to create an input DataSet with an
   * [[org.apache.flink.api.common.io.InputFormat]].
   */
  def createInput[T : ClassTag : TypeInformation](inputFormat: InputFormat[T, _]): DataSet[T] = {
    if (inputFormat == null) {
      throw new IllegalArgumentException("InputFormat must not be null.")
    }
    createInput(inputFormat, explicitFirst(inputFormat, implicitly[TypeInformation[T]]))
  }

  /**
   * Generic method to create an input DataSet with an
   * [[org.apache.flink.api.common.io.InputFormat]].
   */
  private def createInput[T: ClassTag](
      inputFormat: InputFormat[T, _],
      producedType: TypeInformation[T]): DataSet[T] = {
    if (inputFormat == null) {
      throw new IllegalArgumentException("InputFormat must not be null.")
    }
    require(producedType != null, "Produced type must not be null")
    wrap(new DataSource[T](javaEnv, inputFormat, producedType, getCallLocationName()))
  }

  /**
   * Creates a DataSet from the given non-empty [[Iterable]].
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
   */
  def fromCollection[T: ClassTag : TypeInformation](
      data: Iterable[T]): DataSet[T] = {
    require(data != null, "Data must not be null.")

    val typeInfo = implicitly[TypeInformation[T]]
    CollectionInputFormat.checkCollection(data.asJavaCollection, typeInfo.getTypeClass)
    val dataSource = new DataSource[T](
      javaEnv,
      new CollectionInputFormat[T](data.asJavaCollection, typeInfo.createSerializer(getConfig)),
      typeInfo,
      getCallLocationName())
    wrap(dataSource)
  }

  /**
   * Creates a DataSet from the given [[Iterator]].
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
   */
  def fromCollection[T: ClassTag : TypeInformation] (
    data: Iterator[T]): DataSet[T] = {
    require(data != null, "Data must not be null.")

    val typeInfo = implicitly[TypeInformation[T]]
    val dataSource = new DataSource[T](
      javaEnv,
      new IteratorInputFormat[T](data.asJava),
      typeInfo,
      getCallLocationName())
    wrap(dataSource)
  }

  /**
   * Creates a new data set that contains the given elements.
   *
   * * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
   */
  def fromElements[T: ClassTag : TypeInformation](data: T*): DataSet[T] = {
    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]
    fromCollection(data)(implicitly[ClassTag[T]], typeInfo)
  }

  /**
   * Creates a new data set that contains elements in the iterator. The iterator is splittable,
   * allowing the framework to create a parallel data source that returns the elements in the
   * iterator.
   */
  def fromParallelCollection[T: ClassTag : TypeInformation](
      iterator: SplittableIterator[T]): DataSet[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    wrap(new DataSource[T](javaEnv,
      new ParallelIteratorInputFormat[T](iterator),
      typeInfo,
      getCallLocationName()))
  }

  /**
   * Creates a new data set that contains a sequence of numbers. The data set will be created in
   * parallel, so there is no guarantee about the oder of the elements.
   *
   * @param from The number to start at (inclusive).
   * @param to The number to stop at (inclusive).
   */
  def generateSequence(from: Long, to: Long): DataSet[Long] = {
    val iterator = new NumberSequenceIterator(from, to)
    val source = new DataSource(
      javaEnv,
      new ParallelIteratorInputFormat[java.lang.Long](iterator),
      BasicTypeInfo.LONG_TYPE_INFO,
      getCallLocationName())
    wrap(source).asInstanceOf[DataSet[Long]]
  }

  def union[T](sets: Seq[DataSet[T]]): DataSet[T] = {
    sets.reduce( (l, r) => l.union(r) )
  }

  /**
   * Registers a file at the distributed cache under the given name. The file will be accessible
   * from any user-defined function in the (distributed) runtime under a local path. Files
   * may be local files (which will be distributed via BlobServer), or files in a distributed file
   * system. The runtime will copy the files temporarily to a local cache, if needed.
   *
   * The [[org.apache.flink.api.common.functions.RuntimeContext]] can be obtained inside UDFs
   * via
   * [[org.apache.flink.api.common.functions.RichFunction#getRuntimeContext]] and provides
   * access via
   * [[org.apache.flink.api.common.functions.RuntimeContext#getDistributedCache]]
   *
   * @param filePath The path of the file, as a URI (e.g. "file:///some/path" or
   *                 "hdfs://host:port/and/path")
   * @param name The name under which the file is registered.
   * @param executable Flag indicating whether the file should be executable
   */
  def registerCachedFile(filePath: String, name: String, executable: Boolean = false): Unit = {
    javaEnv.registerCachedFile(filePath, name, executable)
  }

  /**
   * Triggers the program execution. The environment will execute all parts of the program that have
   * resulted in a "sink" operation. Sink operations are for example printing results
   * [[DataSet.print]], writing results (e.g. [[DataSet.writeAsText]], [[DataSet.write]], or other
   * generic data sinks created with [[DataSet.output]].
   *
   * The program execution will be logged and displayed with a generated default name.
   *
   * @return The result of the job execution, containing elapsed time and accumulators.
   */
  def execute(): JobExecutionResult = {
    javaEnv.execute()
  }

  /**
   * Triggers the program execution. The environment will execute all parts of the program that have
   * resulted in a "sink" operation. Sink operations are for example printing results
   * [[DataSet.print]], writing results (e.g. [[DataSet.writeAsText]], [[DataSet.write]], or other
   * generic data sinks created with [[DataSet.output]].
   *
   * The program execution will be logged and displayed with the given name.
   *
   * @return The result of the job execution, containing elapsed time and accumulators.
   */
  def execute(jobName: String): JobExecutionResult = {
    javaEnv.execute(jobName)
  }

  /**
   * Register a [[JobListener]] in this environment. The [[JobListener]] will be
   * notified on specific job status changed.
   */
  @PublicEvolving
  def registerJobListener(jobListener: JobListener): Unit = {
    javaEnv.registerJobListener(jobListener)
  }

  /**
   * Clear all registered [[JobListener]]s.
   */
  @PublicEvolving def clearJobListeners(): Unit = {
    javaEnv.clearJobListeners()
  }

  /**
   * Triggers the program execution asynchronously.
   * The environment will execute all parts of the program that have
   * resulted in a "sink" operation. Sink operations are for example printing results
   * [[DataSet.print]], writing results (e.g. [[DataSet.writeAsText]], [[DataSet.write]], or other
   * generic data sinks created with [[DataSet.output]].
   *
   * The program execution will be logged and displayed with a generated default name.
   *
   * <b>ATTENTION:</b> The caller of this method is responsible for managing the lifecycle
   * of the returned [[JobClient]]. This means calling [[JobClient#close()]] at the end of
   * its usage. In other case, there may be resource leaks depending on the JobClient
   * implementation.
   *
   * @return A [[JobClient]] that can be used to communicate with the submitted job,
   *         completed on submission succeeded.
   */
  @PublicEvolving
  def executeAsync(): JobClient = javaEnv.executeAsync()

  /**
   * Triggers the program execution asynchronously.
   * The environment will execute all parts of the program that have
   * resulted in a "sink" operation. Sink operations are for example printing results
   * [[DataSet.print]], writing results (e.g. [[DataSet.writeAsText]], [[DataSet.write]], or other
   * generic data sinks created with [[DataSet.output]].
   *
   * The program execution will be logged and displayed with the given name.
   *
   * <b>ATTENTION:</b> The caller of this method is responsible for managing the lifecycle
   * of the returned [[JobClient]]. This means calling [[JobClient#close()]] at the end of
   * its usage. In other case, there may be resource leaks depending on the JobClient
   * implementation.
   *
   * @return A [[JobClient]] that can be used to communicate with the submitted job,
   *         completed on submission succeeded.
   */
  @PublicEvolving
  def executeAsync(jobName: String): JobClient = javaEnv.executeAsync(jobName)

  /**
   * Creates the plan with which the system will execute the program, and returns it as  a String
   * using a JSON representation of the execution data flow graph.
   */
  def getExecutionPlan() = {
    javaEnv.getExecutionPlan
  }

  /**
   * Creates the program's [[org.apache.flink.api.common.Plan]].
   * The plan is a description of all data sources, data sinks,
   * and operations and how they interact, as an isolated unit that can be executed with an
   * [[PipelineExecutor]]. Obtaining a plan and starting it with an
   * executor is an alternative way to run a program and is only possible if the program only
   * consists of distributed operations.
   */
  def createProgramPlan(jobName: String = "") = {
    if (jobName.isEmpty) {
      javaEnv.createProgramPlan()
    } else {
      javaEnv.createProgramPlan(jobName)
    }
  }
}

@Public
object ExecutionEnvironment {

  /**
   * Sets the default parallelism that will be used for the local execution
   * environment created by [[createLocalEnvironment()]].
   *
   * @param parallelism The default parallelism to use for local execution.
   */
  @PublicEvolving
  def setDefaultLocalParallelism(parallelism: Int) : Unit =
    JavaEnv.setDefaultLocalParallelism(parallelism)

  /**
   * Gets the default parallelism that will be used for the local execution environment created by
   * [[createLocalEnvironment()]].
   */
  @PublicEvolving
  def getDefaultLocalParallelism: Int = JavaEnv.getDefaultLocalParallelism

  // --------------------------------------------------------------------------
  //  context environment
  // --------------------------------------------------------------------------

  /**
   * Creates an execution environment that represents the context in which the program is
   * currently executed. If the program is invoked standalone, this method returns a local
   * execution environment. If the program is invoked from within the command line client
   * to be submitted to a cluster, this method returns the execution environment of this cluster.
   */
  def getExecutionEnvironment: ExecutionEnvironment = {
    new ExecutionEnvironment(JavaEnv.getExecutionEnvironment)
  }

  // --------------------------------------------------------------------------
  //  local environment
  // --------------------------------------------------------------------------

  /**
   * Creates a local execution environment. The local execution environment will run the
   * program in a multi-threaded fashion in the same JVM as the environment was created in.
   *
   * This method sets the environment's default parallelism to given parameter, which
   * defaults to the value set via [[setDefaultLocalParallelism(Int)]].
   */
  def createLocalEnvironment(parallelism: Int = JavaEnv.getDefaultLocalParallelism): 
      ExecutionEnvironment = {
    new ExecutionEnvironment(JavaEnv.createLocalEnvironment(parallelism))
  }

  /**
   * Creates a local execution environment. The local execution environment will run the program in
   * a multi-threaded fashion in the same JVM as the environment was created in.
   * This method allows to pass a custom Configuration to the local environment.
   */
  def createLocalEnvironment(customConfiguration: Configuration): ExecutionEnvironment = {
    val javaEnv = JavaEnv.createLocalEnvironment(customConfiguration)
    new ExecutionEnvironment(javaEnv)
  }

  /**
   * Creates a [[ExecutionEnvironment]] for local program execution that also starts the
   * web monitoring UI.
   *
   * The local execution environment will run the program in a multi-threaded fashion in
   * the same JVM as the environment was created in. It will use the parallelism specified in the
   * parameter.
   *
   * If the configuration key 'rest.port' was set in the configuration, that particular
   * port will be used for the web UI. Otherwise, the default port (8081) will be used.
   *
   * @param config optional config for the local execution
   * @return The created StreamExecutionEnvironment
   */
  @PublicEvolving
  def createLocalEnvironmentWithWebUI(config: Configuration = null): ExecutionEnvironment = {
    val conf: Configuration = if (config == null) new Configuration() else config
    new ExecutionEnvironment(JavaEnv.createLocalEnvironmentWithWebUI(conf))
  }

  /**
   * Creates an execution environment that uses Java Collections underneath. This will execute in a
   * single thread in the current JVM. It is very fast but will fail if the data does not fit into
   * memory. This is useful during implementation and for debugging.
   *
   * @return
   */
  @PublicEvolving
  def createCollectionsEnvironment: ExecutionEnvironment = {
    new ExecutionEnvironment(new CollectionEnvironment)
  }

  // --------------------------------------------------------------------------
  //  remote environment
  // --------------------------------------------------------------------------

  /**
   * Creates a remote execution environment. The remote environment sends (parts of) the program to
   * a cluster for execution. Note that all file paths used in the program must be accessible from
   * the cluster. The execution will use the cluster's default parallelism, unless the
   * parallelism is set explicitly via [[ExecutionEnvironment.setParallelism()]].
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
  def createRemoteEnvironment(host: String, port: Int, jarFiles: String*): ExecutionEnvironment = {
    new ExecutionEnvironment(JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*))
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
      jarFiles: String*): ExecutionEnvironment = {
    val javaEnv = JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*)
    javaEnv.setParallelism(parallelism)
    new ExecutionEnvironment(javaEnv)
  }

  /**
   * Creates a remote execution environment. The remote environment sends (parts of) the program
   * to a cluster for execution. Note that all file paths used in the program must be accessible
   * from the cluster. The custom configuration file is used to configure Akka specific
   * configuration parameters for the Client only; Program parallelism can be set via
   * [[ExecutionEnvironment.setParallelism]].
   *
   * ClusterClient configuration has to be done in the remotely running Flink instance.
   *
   * @param host The host name or address of the master (JobManager), where the program should be
   *             executed.
   * @param port The port of the master (JobManager), where the program should be executed.
   * @param clientConfiguration Pass a custom configuration to the Client.
   * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the
   *                 program uses user-defined functions, user-defined input formats, or any
   *                 libraries, those must be provided in the JAR files.
   * @return A remote environment that executes the program on a cluster.
   */
  def createRemoteEnvironment(
      host: String,
      port: Int,
      clientConfiguration: Configuration,
      jarFiles: String*): ExecutionEnvironment = {
    val javaEnv = JavaEnv.createRemoteEnvironment(host, port, clientConfiguration, jarFiles: _*)
    new ExecutionEnvironment(javaEnv)
  }
}

