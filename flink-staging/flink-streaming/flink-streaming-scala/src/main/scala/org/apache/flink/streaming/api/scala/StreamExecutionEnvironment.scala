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
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.scala.ClosureCleaner
import org.apache.flink.runtime.state.StateHandleProvider
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.types.StringValue
import org.apache.flink.util.SplittableIterator

import scala.reflect.ClassTag

class StreamExecutionEnvironment(javaEnv: JavaEnv) {

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
	 * @deprecated Please use [[getParallelism]]
	 */
	@deprecated
	def getDegreeOfParallelism = javaEnv.getParallelism

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
	def getBufferTimout: Long = javaEnv.getBufferTimeout()

	/**
	 * Method for enabling fault-tolerance. Activates monitoring and backup of streaming
	 * operator states. Time interval between state checkpoints is specified in in millis.
	 *
	 * Setting this option assumes that the job is used in production and thus if not stated
	 * explicitly otherwise with calling with the
	 * {@link #setNumberOfExecutionRetries(int numberOfExecutionRetries)} method in case of
	 * failure the job will be resubmitted to the cluster indefinitely.
	 */
	def enableCheckpointing(interval: Long): StreamExecutionEnvironment = {
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
	def enableCheckpointing(): StreamExecutionEnvironment = {
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
	 * Disables operator chaining for streaming operators. Operator chaining
	 * allows non-shuffle operations to be co-located in the same thread fully
	 * avoiding serialization and de-serialization.
	 *
	 */
	def disableOperatorChaning(): StreamExecutionEnvironment = {
		javaEnv.disableOperatorChaning()
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
	def registerDefaultKryoSerializer(clazz: Class[_], serializer: Class[_ <: Serializer[_]]) {
		javaEnv.addDefaultKryoSerializer(clazz, serializer)
	}

	/**
	 * Registers a default serializer for the given class and its sub-classes at Kryo.
	 *
	 * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 */
	def registerDefaultKryoSerializer(clazz: Class[_], serializer: Serializer[_]): Unit = {
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
	 * Creates a data stream that represents the Strings produced by reading the given file line wise. The file will be
	 * read with the system's default character set.
	 *
	 * @param filePath
	 * The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
	 * @return The data stream that represents the data read from the given file as text lines
	 */
	def readTextFile(filePath: String): DataStream[String] =
		javaEnv.readTextFile(filePath)

	/**
	 * Creates a data stream that represents the Strings produced by reading the given file line wise. The {@link
	 * java.nio.charset.Charset} with the given name will be used to read the files.
	 *
	 * @param filePath
	 * The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param charsetName
	 * The name of the character set used to read the file
	 * @return The data stream that represents the data read from the given file as text lines
	 */
	def readTextFile(filePath: String, charsetName: String): DataStream[String] =
		javaEnv.readTextFile(filePath, charsetName)

	/**
	 * Creates a data stream that contains the contents of file created while system watches the given path. The file
	 * will be read with the system's default character set.
	 *
	 * @param streamPath
	 * The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path/")
	 * @param intervalMillis
	 * The interval of file watching in milliseconds
	 * @param watchType
	 * The watch type of file stream. When watchType is
	 * { @link org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType#ONLY_NEW_FILES},
	 * the system processes only new files.
	 * { @link org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType#REPROCESS_WITH_APPENDED}
	 * means that the system re-processes all contents of appended file.
	 * { @link org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType#PROCESS_ONLY_APPENDED}
	 * means that the system processes only appended contents of files.
	 * @return The DataStream containing the given directory.
	 */
	def readFileStream(streamPath: String, intervalMillis: Long = 100, watchType: WatchType =
	WatchType.ONLY_NEW_FILES): DataStream[String] =
		javaEnv.readFileStream(streamPath, intervalMillis, watchType)

	/**
	 * Creates a data stream that represents the strings produced by reading the given file line wise. This method is
	 * similar to {@link #readTextFile(String)}, but it produces a data stream with mutable {@link StringValue}
	 * objects,
	 * rather than Java Strings. StringValues can be used to tune implementations to be less object and garbage
	 * collection heavy.
	 * <p/>
	 * The file will be read with the system's default character set.
	 *
	 * @param filePath
	 * The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @return A data stream that represents the data read from the given file as text lines
	 */
	def readTextFileWithValue(filePath: String): DataStream[StringValue] =
		javaEnv.readTextFileWithValue(filePath)

	/**
	 * Creates a data stream that represents the Strings produced by reading the given file line wise. This method is
	 * similar to {@link #readTextFile(String, String)}, but it produces a data stream with mutable {@link StringValue}
	 * objects, rather than Java Strings. StringValues can be used to tune implementations to be less object and
	 * garbage
	 * collection heavy.
	 * <p/>
	 * The {@link java.nio.charset.Charset} with the given name will be used to read the files.
	 *
	 * @param filePath
	 * The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param charsetName
	 * The name of the character set used to read the file
	 * @param skipInvalidLines
	 * A flag to indicate whether to skip lines that cannot be read with the given character set
	 * @return A data stream that represents the data read from the given file as text lines
	 */
	def readTextFileWithValue(filePath: String, charsetName: String,
		skipInvalidLines: Boolean): DataStream[StringValue] =
		javaEnv.readTextFileWithValue(filePath, charsetName, skipInvalidLines)

	/**
	 * Reads the given file with the given imput format.
	 *
	 * @param filePath
	 * The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param inputFormat
	 * The input format used to create the data stream
	 * @return The data stream that represents the data read from the given file
	 */
	def readFile[T: ClassTag : TypeInformation](inputFormat: FileInputFormat[T],
		filePath: String): DataStream[T] = {
		javaEnv.readFile(inputFormat, filePath)
	}

	/**
	 * Creates a data stream that represents the primitive type produced by reading the given file line wise.
	 *
	 * @param filePath
	 * The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param typeClass
	 * The primitive type class to be read
	 * @return A data stream that represents the data read from the given file as primitive type
	 */
	def readFileOfPrimitives[T: ClassTag : TypeInformation](filePath: String, typeClass: Class[T]): DataStream[T] = {
		javaEnv.readFileOfPrimitives(filePath, typeClass)
	}

	/**
	 * Creates a data stream that represents the primitive type produced by reading the given file in delimited way.
	 *
	 * @param filePath
	 * The path of the file, as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path")
	 * @param delimiter
	 * The delimiter of the given file
	 * @param typeClass
	 * The primitive type class to be read
	 * @return A data stream that represents the data read from the given file as primitive type.
	 */
	def readFileOfPrimitives[T: ClassTag : TypeInformation](filePath: String, delimiter: String, typeClass: Class[T]): DataStream[T] = {
		javaEnv.readFileOfPrimitives(filePath, delimiter, typeClass)
	}

	/**
	 * Creates a data stream from the given {@link org.apache.hadoop.mapred.FileInputFormat}. A {@link
	 * org.apache.hadoop.mapreduce.Job} with the given inputPath is created.
	 */
	def readHadoopFile[K, V: ClassTag : TypeInformation](mapredInputFormat: org.apache.hadoop.mapred.FileInputFormat[K, V],
		key: Class[K], value: Class[V], inputPath: String): DataStream[org.apache.flink.api.java.tuple.Tuple2[K, V]] = {
		javaEnv.readHadoopFile(mapredInputFormat, key, value, inputPath)
	}

	/**
	 * Creates a data stream from the given {@link org.apache.hadoop.mapred.FileInputFormat}. A {@link
	 * org.apache.hadoop.mapred.JobConf} with the given inputPath is created.
	 */
	def readHadoopFile[K, V: ClassTag : TypeInformation](mapredInputFormat: org.apache.hadoop.mapred.FileInputFormat[K, V],
		key: Class[K], value: Class[V], inputPath: String, job: org.apache.hadoop.mapred.JobConf): DataStream[org.apache.flink.api.java.tuple.Tuple2[K, V]] = {
		javaEnv.readHadoopFile(mapredInputFormat, key, value, inputPath)
	}

	/**
	 * Creates a data stream from the given {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}. A {@link
	 * org.apache.hadoop.mapreduce.Job} with the given inputPath is created.
	 */
	def readHadoopFile[K, V: ClassTag : TypeInformation](mapredInputFormat: org.apache.hadoop.mapreduce.lib.input.FileInputFormat[K, V],
		key: Class[K], value: Class[V], inputPath: String, job: org.apache.hadoop.mapreduce.Job): DataStream[org.apache.flink.api.java.tuple.Tuple2[K, V]] = {
		javaEnv.readHadoopFile(mapredInputFormat, key, value, inputPath, job)
	}

	/**
	 * Creates a data stream from the given {@link org.apache.hadoop.mapreduce.InputFormat}.
	 */
	def readHadoopFile[K, V: ClassTag : TypeInformation](mapredInputFormat: org.apache.hadoop.mapreduce.lib.input.FileInputFormat[K, V],
		key: Class[K], value: Class[V], inputPath: String): DataStream[org.apache.flink.api.java.tuple.Tuple2[K, V]] = {
		javaEnv.readHadoopFile(mapredInputFormat, key, value, inputPath)
	}

	/**
	 * Creates a new data stream that contains the strings received infinitely from a socket. Received strings are
	 * decoded by the system's default character set. On the termination of the socket server connection retries can be
	 * initiated.
	 * <p/>
	 * Let us note that the socket itself does not report on abort and as a consequence retries are only initiated when
	 * the socket was gracefully terminated.
	 *
	 * @param hostname
	 * The host name which a server socket binds
	 * @param port
	 * The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * allocated.
	 * @param delimiter
	 * A character which splits received strings into records
	 * @param maxRetry
	 * The maximal retry interval in seconds while the program waits for a socket that is temporarily down.
	 * Reconnection is initiated every second. A number of 0 means that the reader is immediately terminated,
	 * while
	 * a	negative value ensures retrying forever.
	 * @return A data stream containing the strings received from the socket
	 */
	def socketTextStream(hostname: String, port: Int, delimiter: Char, maxRetry: Long): DataStream[String] =
		javaEnv.socketTextStream(hostname, port, delimiter, maxRetry)

	/**
	 * Creates a new data stream that contains the strings received infinitely from a socket. Received strings are
	 * decoded by the system's default character set. The reader is terminated immediately when the socket is down.
	 *
	 * @param hostname
	 * The host name which a server socket binds
	 * @param port
	 * The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * allocated.
	 * @param delimiter
	 * A character which splits received strings into records
	 * @return A data stream containing the strings received from the socket
	 */
	def socketTextStream(hostname: String, port: Int, delimiter: Char): DataStream[String] =
		javaEnv.socketTextStream(hostname, port, delimiter)

	/**
	 * Creates a new data stream that contains the strings received infinitely from a socket. Received strings are
	 * decoded by the system's default character set, using'\n' as delimiter. The reader is terminated immediately when
	 * the socket is down.
	 *
	 * @param hostname
	 * The host name which a server socket binds
	 * @param port
	 * The port number which a server socket binds. A port number of 0 means that the port number is automatically
	 * allocated.
	 * @return A data stream containing the strings received from the socket
	 */
	def socketTextStream(hostname: String, port: Int): DataStream[String] =
		javaEnv.socketTextStream(hostname, port)

	//  <K, V > DataStreamSource[Tuple2[K, V]] createHadoopInput(mapredInputFormat: InputFormat[K, V], key: Class[K], value: Class[V], job: JobConf) {
	//    val hadoopInputFormat: HadoopInputFormat[K, V] = new HadoopInputFormat[K, V](mapredInputFormat, key, value, job)
	//    return createInput(hadoopInputFormat, TypeExtractor.getInputFormatTypes(hadoopInputFormat), "Hadoop " + "Input source")
	//  }

	/**
	 * Creates a data stream from the given {@link org.apache.hadoop.mapred.InputFormat}.
	 */
	def createHadoopInput[K, V: ClassTag : TypeInformation](mapredInputFormat: org.apache.hadoop.mapred.InputFormat[K, V],
		key: Class[K], value: Class[V],
		job: org.apache.hadoop.mapred.JobConf): DataStream[org.apache.flink.api.java.tuple.Tuple2[K, V]] =
		javaEnv.createHadoopInput(mapredInputFormat, key, value, job)

	/**
	 * Creates a data stream from the given {@link org.apache.hadoop.mapred.InputFormat}.
	 */
	def createHadoopInput[K, V: ClassTag : TypeInformation](mapredInputFormat: org.apache.hadoop.mapreduce.InputFormat[K, V],
		key: Class[K], value: Class[V], inputPath: String, job: org.apache.hadoop.mapreduce.Job) =
		javaEnv.createHadoopInput(mapredInputFormat, key, value, job)

	/**
	 * Generic method to create an input data stream with {@link org.apache.flink.api.common.io.InputFormat}. The data stream will not be immediately
	 * created - instead, this method returns a data stream that will be lazily created from the input format once the
	 * program is executed.
	 * <p/>
	 * Since all data streams need specific information about their types, this method needs to determine the type of
	 * the data produced by the input format. It will attempt to determine the data type by reflection, unless the
	 * input
	 * format implements the {@link org.apache.flink.api.java.typeutils .ResultTypeQueryable} interface. In the latter
	 * case, this method will invoke the {@link org.apache.flink.api.java.typeutils
	 * .ResultTypeQueryable#getProducedType()} method to determine data type produced by the input format.
	 *
	 * @param inputFormat
	 * The input format used to create the data stream
	 * @return The data stream that represents the data created by the input format
	 */
	def createInput[T: ClassTag : TypeInformation](inputFormat: InputFormat[T, _]): DataStream[T] =
		javaEnv.createInput(inputFormat)

	/**
	 * Generic method to create an input data stream with {@link org.apache.flink.api.common.io.InputFormat}. The data stream will not be immediately
	 * created - instead, this method returns a data stream that will be lazily created from the input format once the
	 * program is executed.
	 * <p/>
	 * The data stream is typed to the given TypeInformation. This method is intended for input formats where the
	 * return
	 * type cannot be determined by reflection analysis, and that do not implement the {@link
	 * org.apache.flink.api.java.typeutils.ResultTypeQueryable} interface.
	 *
	 * @param inputFormat
	 * The input format used to create the data stream
	 * @return The data stream that represents the data created by the input format
	 */
	def createInput[T: ClassTag : TypeInformation](inputFormat: InputFormat[T, _],
		typeInfo: TypeInformation[T]): DataStream[T] =
		javaEnv.createInput(inputFormat, typeInfo)

	//  private[flink] def createInput[T: ClassTag: TypeInformation](inputFormat: InputFormat[T,_],
	//    typeInfo: TypeInformation[T], sourceName: String): DataStream[T] = {
	//    val function = new FileSourceFunction[T](inputFormat, typeInfo)
	//    val returnStream = javaEnv.addSource(function, sourceName).returns(typeInfo)
	//    javaEnv.getStreamGraph.setInputFormat(returnStream.getId, inputFormat)
	//    returnStream
	//  }

	/**
	 * Creates a new data stream that contains a sequence of numbers. The data stream will be created in parallel, so
	 * there is no guarantee about the oder of the elements.
	 *
	 * @param from
	 * The number to start at (inclusive)
	 * @param to
	 * The number to stop at (inclusive)
	 * @return A data stream, containing all number in the [from, to] interval
	 */
	def generateSequence(from: Long, to: Long): DataStream[Long] = {
		new DataStream[java.lang.Long](javaEnv.generateSequence(from, to)).
			asInstanceOf[DataStream[Long]]
	}

	/**
	 * Creates a new data stream that contains the given elements. The elements must all be of the same type, for
	 * example, all of the {@link String} or {@link Integer}. The sequence of elements must not be empty. Furthermore,
	 * the elements must be serializable (as defined in {@link java.io.Serializable}), because the execution
	 * environment
	 * may ship the elements into the cluster.
	 * <p/>
	 * The framework will try and determine the exact type from the elements. In case of generic elements, it may be
	 * necessary to manually supply the type information via {@link #fromCollection(java.util.Collection,
	 * org.apache.flink.api.common.typeinfo.TypeInformation)}.
	 * <p/>
	 * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * degree of parallelism one.
	 *
	 * @param data
	 * The array of elements to create the data stream from.
	 * @return The data stream representing the given array of elements
	 */
	def fromElements[T: ClassTag : TypeInformation](data: T*): DataStream[T] = {
		val typeInfo = implicitly[TypeInformation[T]]
		fromCollection(data)(implicitly[ClassTag[T]], typeInfo)
	}

	/**
	 * Creates a data stream from the given non-empty collection. The type of the data stream is that of the
	 * elements in
	 * the collection. The elements need to be serializable (as defined by {@link java.io.Serializable}), because the
	 * framework may move the elements into the cluster if needed.
	 * <p/>
	 * The framework will try and determine the exact type from the collection elements. In case of generic
	 * elements, it
	 * may be necessary to manually supply the type information via {@link #fromCollection(java.util.Collection,
	 * org.apache.flink.api.common.typeinfo.TypeInformation)}.
	 * <p/>
	 * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * degree of parallelism one.
	 *
	 * @param data
	 * The collection of elements to create the data stream from
	 * @return The data stream representing the given collection
	 */
	def fromCollection[T: ClassTag : TypeInformation](
		data: Seq[T]): DataStream[T] = {
		require(data != null, "Data must not be null.")
		val typeInfo = implicitly[TypeInformation[T]]

		val sourceFunction = new FromElementsFunction[T](scala.collection.JavaConversions
			.asJavaCollection(data))

		javaEnv.addSource(sourceFunction, "Collection source").returns(typeInfo)
	}

	/**
	 * Creates a data stream from the given non-empty collection. The type of the data stream is the type given by
	 * typeInfo. The elements need to be serializable (as defined by {@link java.io.Serializable}), because the
	 * framework may move the elements into the cluster if needed.
	 * <p/>
	 * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * degree of parallelism one.
	 *
	 * @param data
	 * The collection of elements to create the data stream from
	 * @param typeInfo
	 * The TypeInformation for the produced data stream
	 * @return The data stream representing the given collection
	 */
	def fromCollection[T: ClassTag : TypeInformation](
		data: Seq[T], typeInfo: TypeInformation[T]): DataStream[T] = {
		require(data != null, "Data must not be null.")

		val sourceFunction = new FromElementsFunction[T](scala.collection.JavaConversions
			.asJavaCollection(data))

		javaEnv.addSource(sourceFunction, "Collection source").returns(typeInfo)
	}

	/**
	 * Creates a data stream from the given iterator. Because the iterator will remain unmodified until the actual
	 * execution happens, the type of data returned by the iterator must be given explicitly in the form of the type
	 * class (this is due to the fact that the Java compiler erases the generic type information).
	 * <p/>
	 * The iterator must be serializable (as defined in {@link java.io.Serializable}), because the framework may
	 * move it
	 * to a remote environment, if needed.
	 * <p/>
	 * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * degree of parallelism of one.
	 *
	 * @param data
	 * The iterator of elements to create the data stream from
	 * @param typeClass
	 * The class of the data produced by the iterator. Must not be a generic class.
	 * @return The data stream representing the elements in the iterator
	 * @see #fromCollection(java.util.Iterator, org.apache.flink.api.common.typeinfo.TypeInformation)
	 */
	def fromCollection[T: ClassTag : TypeInformation](
		data: Iterator[T], typeClass: Class[T]): DataStream[T] = {
		fromCollection(data, TypeExtractor.getForClass(typeClass))
	}

	/**
	 * Creates a data stream from the given iterator. Because the iterator will remain unmodified until the actual
	 * execution happens, the type of data returned by the iterator must be given explicitly in the form of the type
	 * information. This method is useful for cases where the type is generic. In that case, the type class (as
	 * given in
	 * {@link #fromCollection(java.util.Iterator, Class)} does not supply all type information.
	 * <p/>
	 * The iterator must be serializable (as defined in {@link java.io.Serializable}), because the framework may
	 * move it
	 * to a remote environment, if needed.
	 * <p/>
	 * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * degree of parallelism one.
	 *
	 * @param data
	 * The iterator of elements to create the data stream from
	 * @param typeInfo
	 * The TypeInformation for the produced data stream
	 * @return The data stream representing the elements in the iterator
	 */
	def fromCollection[T: ClassTag : TypeInformation](
		data: Iterator[T], typeInfo: TypeInformation[T]): DataStream[T] = {
		require(data != null, "Data must not be null.")
		if (!data.isInstanceOf[java.io.Serializable]) {
			throw new IllegalArgumentException("The iterator must be serializable.")
		}

		val sourceFunction = new FromIteratorFunction[T](scala.collection.JavaConversions
			.asJavaIterator(data))

		javaEnv.addSource(sourceFunction, "Collection source").returns(typeInfo)
	}

	/**
	 * Creates a new data stream that contains elements in the iterator. The iterator is splittable, allowing the
	 * framework to create a parallel data stream source that returns the elements in the iterator. The iterator
	 * must be
	 * serializable (as defined in {@link java.io.Serializable}, because the execution environment may ship the
	 * elements
	 * into the cluster.
	 * <p/>
	 * Because the iterator will remain unmodified until the actual execution happens, the type of data returned by the
	 * iterator must be given explicitly in the form of the type class (this is due to the fact that the Java compiler
	 * erases the generic type information).
	 *
	 * @param iterator
	 * The iterator that produces the elements of the data stream
	 * @param typeClass
	 * The class of the data produced by the iterator. Must not be a generic class.
	 * @return A data stream representing the elements in the iterator
	 */
	def fromParallelCollection[T: ClassTag : TypeInformation](iterator: SplittableIterator[T],
		typeClass: Class[T]): DataStream[T] = {
		javaEnv.fromParallelCollection(iterator, typeClass)
	}

	/**
	 * Creates a new data stream that contains elements in the iterator. The iterator is splittable, allowing the
	 * framework to create a parallel data stream source that returns the elements in the iterator. The iterator
	 * must be
	 * serializable (as defined in {@link java.io.Serializable}, because the execution environment may ship the
	 * elements
	 * into the cluster.
	 * <p/>
	 * Because the iterator will remain unmodified until the actual execution happens, the type of data returned by the
	 * iterator must be given explicitly in the form of the type information. This method is useful for cases where the
	 * type is generic. In that case, the type class (as given in {@link #fromParallelCollection(SplittableIterator,
	 * Class)} does not supply all type information.
	 *
	 * @param iterator
	 * The iterator that produces the elements of the data stream
	 * @param typeInfo
	 * The TypeInformation for the produced data stream.
	 * @return A data stream representing the elements in the iterator
	 */
	def fromParallelCollection[T: ClassTag : TypeInformation](iterator: SplittableIterator[T],
		typeInfo: TypeInformation[T]): DataStream[T] = {
		javaEnv.fromParallelCollection(iterator, typeInfo)
	}

	// private helper for passing different names
	private[flink] def fromParallelCollection[T: ClassTag : TypeInformation](iterator: SplittableIterator[T],
		typeInfo: TypeInformation[T], operatorName: String): DataStream[T] = {
		javaEnv.addSource(new FromIteratorFunction[T](iterator), operatorName).returns(typeInfo)
	}


	/**
	 * Create a DataStream using a user defined source function for arbitrary
	 * source functionality. By default sources have a parallelism of 1.
	 * To enable parallel execution, the user defined source should implement
	 * ParallelSourceFunction or extend RichParallelSourceFunction.
	 * In these cases the resulting source will have the parallelism of the environment.
	 * To change this afterwards call DataStreamSource.setParallelism(int)
	 *
	 */
	def addSource[T: ClassTag : TypeInformation](function: SourceFunction[T]): DataStream[T] = {
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
	def addSource[T: ClassTag : TypeInformation](function: () => T): DataStream[T] = {
		require(function != null, "Function must not be null.")
		val sourceFunction = new SourceFunction[T] {
			val cleanFun = StreamExecutionEnvironment.clean(function)

			override def reachedEnd(): Boolean = false

			override def next(): T = cleanFun()
		}
		addSource(sourceFunction)
	}


	/**
	 * Ads a data source with a custom type information thus opening a {@link org.apache.flink.streaming.api
	 * .datastream.DataStream}. Only in very special cases does the user need to support type information. Otherwise
	 * use
	 * {@link #addSource(org.apache.flink.streaming.api.function.source.SourceFunction)}
	 *
	 * @param function
	 * the user defined function
	 * @param sourceName
	 * Name of the data source
	 * @return the data stream constructed
	 */
	def addSource[T: ClassTag : TypeInformation](function: SourceFunction[T],
		sourceName: String): DataStream[T] = {
		require(function != null, "Function must not be null.")
		val cleanFun = StreamExecutionEnvironment.clean(function)
		val typeInfo = implicitly[TypeInformation[T]]
		javaEnv.addSource(cleanFun, sourceName).returns(typeInfo)
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
	def getExecutionPlan() = javaEnv.getStreamGraph.getStreamingPlanAsJSON

}

object StreamExecutionEnvironment {

	private[flink] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
		ClosureCleaner.clean(f, checkSerializable)
		f
	}

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
		parallelism: Int = Runtime.getRuntime.availableProcessors()):
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