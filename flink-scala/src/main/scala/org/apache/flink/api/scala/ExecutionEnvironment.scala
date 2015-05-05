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

import java.util.UUID

import com.esotericsoftware.kryo.Serializer
import org.apache.flink.api.common.io.{FileInputFormat, InputFormat}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.{ExecutionConfig, JobExecutionResult}
import org.apache.flink.api.java.io._
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfoBase, ValueTypeInfo}
import org.apache.flink.api.java.{CollectionEnvironment, ExecutionEnvironment => JavaEnv}
import org.apache.flink.api.scala.hadoop.{mapred, mapreduce}
import org.apache.flink.api.scala.operators.ScalaCsvInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.types.StringValue
import org.apache.flink.util.{NumberSequenceIterator, SplittableIterator}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.mapred.{FileInputFormat => MapredFileInputFormat, InputFormat => MapredInputFormat, JobConf}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => MapreduceFileInputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => MapreduceInputFormat, Job}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * The ExecutionEnviroment is the context in which a program is executed. A local environment will
 * cause execution in the current JVM, a remote environment will cause execution on a remote
 * cluster installation.
 *
 * The environment provides methods to control the job execution (such as setting the parallelism)
 * and to interact with the outside world (data access).
 *
 * To get an execution environment use the methods on the companion object:
 *
 *  - [[ExecutionEnvironment.getExecutionEnvironment]]
 *  - [[ExecutionEnvironment.createLocalEnvironment]]
 *  - [[ExecutionEnvironment.createRemoteEnvironment]]
 *
 *  Use [[ExecutionEnvironment.getExecutionEnvironment]] to get the correct environment depending
 *  on where the program is executed. If it is run inside an IDE a loca environment will be
 *  created. If the program is submitted to a cluster a remote execution environment will
 *  be created.
 */
class ExecutionEnvironment(javaEnv: JavaEnv) {

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
   * @deprecated Please use [[setParallelism]]
   */
  @deprecated
  def setDegreeOfParallelism(parallelism: Int): Unit = {
    setParallelism(parallelism)
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
   * @deprecated Please use [[getParallelism]]
   */
  @deprecated
  def getDegreeOfParallelism = javaEnv.getParallelism

  /**
   * Returns the default parallelism for this execution environment. Note that this
   * value can be overridden by individual operations using [[DataSet.setParallelism]]
   */
  def getParallelism = javaEnv.getParallelism

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

  /**
   * Gets the UUID by which this environment is identified. The UUID sets the execution context
   * in the cluster or local environment.
   */
  def getId: UUID = {
    javaEnv.getId
  }


  /**
   * retrieves JobExecutionResult from last job execution (for "eager" print)
   * @return JobExecutionResult form last job execution
   */
  def getLastJobExecutionResult = javaEnv.getLastJobExecutionResult

  /**
   * Gets the UUID by which this environment is identified, as a string.
   */
  def getIdString: String = {
    javaEnv.getIdString
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
  def addDefaultKryoSerializer(clazz: Class[_], serializer: Serializer[_]): Unit = {
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
   * Creates a DataSet of Strings produced by reading the given file line wise.
   *
   * @param filePath The path of the file, as a URI (e.g., "file:///some/local/file" or
   *                 "hdfs://host:port/file/path").
   * @param charsetName The name of the character set used to read the file. Default is UTF-0
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
   * @param charsetName The name of the character set used to read the file. Default is UTF-0
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
   *                 "hdfs://host:port/file/path").   * @param lineDelimiter
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

    val inputFormat = new ScalaCsvInputFormat[T](new Path(filePath), typeInfo)
    inputFormat.setDelimiter(lineDelimiter)
    inputFormat.setFieldDelimiter(fieldDelimiter)
    inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine)
    inputFormat.setLenient(lenient)
    inputFormat.setCommentPrefix(ignoreComments)

    if (quoteCharacter != null) {
      inputFormat.enableQuotedStringParsing(quoteCharacter);
    }

    val classesBuf: ArrayBuffer[Class[_]] = new ArrayBuffer[Class[_]]
    typeInfo match {
      case info: TupleTypeInfoBase[T] =>
        for (i <- 0 until info.getArity) {
          classesBuf += info.getTypeAt(i).getTypeClass()
        }
      case info: PojoTypeInfo[T] =>
        if (pojoFields == null) {
          throw new IllegalArgumentException(
            "POJO fields must be specified (not null) if output type is a POJO.")
        } else {
          for (i <- 0 until pojoFields.length) {
            val pos = info.getFieldIndex(pojoFields(i))
            if (pos < 0) {
              throw new IllegalArgumentException(
                "Field \"" + pojoFields(i) + "\" not part of POJO type " +
                  info.getTypeClass.getCanonicalName);
            }
            classesBuf += info.getPojoFieldAt(pos).`type`.getTypeClass
          }
        }
      case _ => throw new IllegalArgumentException("Type information is not valid.")
    }

    if (includedFields != null) {
      require(classesBuf.size == includedFields.length, "Number of tuple fields and" +
        " included fields must match.")
      inputFormat.setFields(includedFields, classesBuf.toArray)
    } else {
      inputFormat.setFieldTypes(classesBuf.toArray)
    }

    if (pojoFields != null) {
      inputFormat.setOrderOfPOJOFields(pojoFields)
    }

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
    createInput(inputFormat, implicitly[TypeInformation[T]])
  }

  /**
   * Generic method to create an input DataSet with an
   * [[org.apache.flink.api.common.io.InputFormat]].
   */
  def createInput[T : ClassTag : TypeInformation](inputFormat: InputFormat[T, _]): DataSet[T] = {
    if (inputFormat == null) {
      throw new IllegalArgumentException("InputFormat must not be null.")
    }
    createInput(inputFormat, implicitly[TypeInformation[T]])
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
   * Creates a [[DataSet]] from the given [[org.apache.hadoop.mapred.FileInputFormat]]. The
   * given inputName is set on the given job.
   */
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
  def readHadoopFile[K, V](
      mapredInputFormat: MapredFileInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      inputPath: String)
      (implicit tpe: TypeInformation[(K, V)]): DataSet[(K, V)] = {
    readHadoopFile(mapredInputFormat, key, value, inputPath, new JobConf)
  }

  /**
   * Creates a [[DataSet]] from the given [[org.apache.hadoop.mapred.InputFormat]].
   */
  def createHadoopInput[K, V](
      mapredInputFormat: MapredInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      job: JobConf)
      (implicit tpe: TypeInformation[(K, V)]): DataSet[(K, V)] = {
    val hadoopInputFormat = new mapred.HadoopInputFormat[K, V](mapredInputFormat, key, value, job)
    createInput(hadoopInputFormat)
  }

  /**
   * Creates a [[DataSet]] from the given [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]].
   * The given inputName is set on the given job.
   */
  def readHadoopFile[K, V](
      mapredInputFormat: MapreduceFileInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      inputPath: String,
      job: Job)
      (implicit tpe: TypeInformation[(K, V)]): DataSet[(K, V)] = {
    val result = createHadoopInput(mapredInputFormat, key, value, job)
    MapreduceFileInputFormat.addInputPath(job, new HadoopPath(inputPath))
    result
  }

  /**
   * Creates a [[DataSet]] from the given
   * [[org.apache.hadoop.mapreduce.lib.input.FileInputFormat]]. A
   * [[org.apache.hadoop.mapreduce.Job]] with the given inputPath will be created.
   */
  def readHadoopFile[K, V](
      mapredInputFormat: MapreduceFileInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      inputPath: String)
      (implicit tpe: TypeInformation[(K, V)]): DataSet[Tuple2[K, V]] = {
    readHadoopFile(mapredInputFormat, key, value, inputPath, Job.getInstance)
  }

  /**
   * Creates a [[DataSet]] from the given [[org.apache.hadoop.mapreduce.InputFormat]].
   */
  def createHadoopInput[K, V](
      mapredInputFormat: MapreduceInputFormat[K, V],
      key: Class[K],
      value: Class[V],
      job: Job)
      (implicit tpe: TypeInformation[(K, V)]): DataSet[Tuple2[K, V]] = {
    val hadoopInputFormat =
      new mapreduce.HadoopInputFormat[K, V](mapredInputFormat, key, value, job)
    createInput(hadoopInputFormat)
  }

  /**
   * Creates a DataSet from the given non-empty [[Seq]]. The elements need to be serializable
   * because the framework may move the elements into the cluster if needed.
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
   */
  def fromCollection[T: ClassTag : TypeInformation](
      data: Seq[T]): DataSet[T] = {
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
   * Creates a DataSet from the given [[Iterator]]. The iterator must be serializable because the
   * framework might move into the cluster if needed.
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
   * Creates a new data set that contains the given elements. The elements must all be of the
   * same type and must be serializable.
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
   * iterator. The iterator must be serializable because the execution environment may ship the
   * elements into the cluster.
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
   * may be local files (as long as all relevant workers have access to it),
   * or files in a distributed file system.
   * The runtime will copy the files temporarily to a local cache, if needed.
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
   * Creates the plan with which the system will execute the program, and returns it as  a String
   * using a JSON representation of the execution data flow graph.
   */
  def getExecutionPlan() = {
    javaEnv.getExecutionPlan
  }

  /**
   * Creates the program's [[org.apache.flink.api.common.Plan]].
   * The plan is a description of all data sources, data sinks,
   * and operations and how they interact, as an isolated unit that can be executed with a
   * [[org.apache.flink.api.common.PlanExecutor]]. Obtaining a plan and starting it with an
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

object ExecutionEnvironment {

  /**
   * Creates an execution environment that represents the context in which the program is
   * currently executed. If the program is invoked standalone, this method returns a local
   * execution environment. If the program is invoked from within the command line client
   * to be submitted to a cluster, this method returns the execution environment of this cluster.
   */
  def getExecutionEnvironment: ExecutionEnvironment = {
    new ExecutionEnvironment(JavaEnv.getExecutionEnvironment)
  }

  /**
   * Creates a local execution environment. The local execution environment will run the program in
   * a multi-threaded fashion in the same JVM as the environment was created in. The parallelism of
   * the local environment is the number of hardware contexts (CPU cores/threads).
   */
  def createLocalEnvironment(
      parallelism: Int = Runtime.getRuntime.availableProcessors())
      : ExecutionEnvironment = {
    val javaEnv = JavaEnv.createLocalEnvironment()
    javaEnv.setParallelism(parallelism)
    new ExecutionEnvironment(javaEnv)
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
   * Creates an execution environment that uses Java Collections underneath. This will execute in a
   * single thread in the current JVM. It is very fast but will fail if the data does not fit into
   * memory. This is useful during implementation and for debugging.
   * @return
   */
  def createCollectionsEnvironment: ExecutionEnvironment = {
    new ExecutionEnvironment(new CollectionEnvironment)
  }

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
}

