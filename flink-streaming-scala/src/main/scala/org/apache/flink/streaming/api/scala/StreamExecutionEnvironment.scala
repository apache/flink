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
import org.apache.flink.annotation.{Internal, Public, PublicEvolving}
import org.apache.flink.api.common.io.{FileInputFormat, FilePathFilter, InputFormat}
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.scala.ClosureCleaner
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.AbstractStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.SplittableIterator

import scala.collection.JavaConverters._
import _root_.scala.language.implicitConversions

@Public
class StreamExecutionEnvironment(javaEnv: JavaEnv) {

  /**
    * @return the wrapped Java environment
    */
  def getJavaEnv: JavaEnv = javaEnv

  /**
   * Gets the config object.
   */
  def getConfig = javaEnv.getConfig

  /**
    * Gets cache files.
    */
  def getCachedFiles = javaEnv.getCachedFiles

  /**
   * Sets the parallelism for operations executed through this environment.
   * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run
   * with x parallel instances. This value can be overridden by specific operations using
   * [[DataStream#setParallelism(int)]].
   */
  def setParallelism(parallelism: Int): Unit = {
    javaEnv.setParallelism(parallelism)
  }

  /**
    * Sets the maximum degree of parallelism defined for the program.
    * The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
    * defines the number of key groups used for partitioned state.
    **/
  def setMaxParallelism(maxParallelism: Int): Unit = {
    javaEnv.setMaxParallelism(maxParallelism)
  }

  /**
   * Returns the default parallelism for this execution environment. Note that this
   * value can be overridden by individual operations using [[DataStream#setParallelism(int)]]
   */
  def getParallelism = javaEnv.getParallelism

  /**
    * Returns the maximum degree of parallelism defined for the program.
    *
    * The maximum degree of parallelism specifies the upper limit for dynamic scaling. It also
    * defines the number of key groups used for partitioned state.
    *
    */
  def getMaxParallelism = javaEnv.getMaxParallelism

  /**
   * Sets the maximum time frequency (milliseconds) for the flushing of the
   * output buffers. By default the output buffers flush frequently to provide
   * low latency and to aid smooth developer experience. Setting the parameter
   * can result in three logical modes:
   *
   * <ul>
   *   <li>A positive integer triggers flushing periodically by that integer</li>
   *   <li>0 triggers flushing after every record thus minimizing latency</li>
   *   <li>-1 triggers flushing only when the output buffer is full thus maximizing throughput</li>
   * </ul>
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
  @PublicEvolving
  def disableOperatorChaining(): StreamExecutionEnvironment = {
    javaEnv.disableOperatorChaining()
    this
  }

  // ------------------------------------------------------------------------
  //  Checkpointing Settings
  // ------------------------------------------------------------------------
  
  /**
   * Gets the checkpoint config, which defines values like checkpoint interval, delay between
   * checkpoints, etc.
   */
  def getCheckpointConfig = javaEnv.getCheckpointConfig()
  
  /**
   * Enables checkpointing for the streaming job. The distributed state of the streaming
   * dataflow will be periodically snapshotted. In case of a failure, the streaming
   * dataflow will be restarted from the latest completed checkpoint.
   *
   * The job draws checkpoints periodically, in the given interval. The state will be
   * stored in the configured state backend.
   *
   * NOTE: Checkpointing iterative streaming dataflows in not properly supported at
   * the moment. If the "force" parameter is set to true, the system will execute the
   * job nonetheless.
   *
   * @param interval
   *     Time interval between state checkpoints in millis.
   * @param mode
   *     The checkpointing mode, selecting between "exactly once" and "at least once" guarantees.
   * @param force
   *           If true checkpointing will be enabled for iterative jobs as well.
   */
  @deprecated
  @PublicEvolving
  def enableCheckpointing(interval : Long,
                          mode: CheckpointingMode,
                          force: Boolean) : StreamExecutionEnvironment = {
    javaEnv.enableCheckpointing(interval, mode, force)
    this
  }

  /**
   * Enables checkpointing for the streaming job. The distributed state of the streaming
   * dataflow will be periodically snapshotted. In case of a failure, the streaming
   * dataflow will be restarted from the latest completed checkpoint.
   *
   * The job draws checkpoints periodically, in the given interval. The system uses the
   * given [[CheckpointingMode]] for the checkpointing ("exactly once" vs "at least once").
   * The state will be stored in the configured state backend.
   *
   * NOTE: Checkpointing iterative streaming dataflows in not properly supported at
   * the moment. For that reason, iterative jobs will not be started if used
   * with enabled checkpointing. To override this mechanism, use the 
   * [[enableCheckpointing(long, CheckpointingMode, boolean)]] method.
   *
   * @param interval 
   *     Time interval between state checkpoints in milliseconds.
   * @param mode 
   *     The checkpointing mode, selecting between "exactly once" and "at least once" guarantees.
   */
  def enableCheckpointing(interval : Long,
                          mode: CheckpointingMode) : StreamExecutionEnvironment = {
    javaEnv.enableCheckpointing(interval, mode)
    this
  }

  /**
   * Enables checkpointing for the streaming job. The distributed state of the streaming
   * dataflow will be periodically snapshotted. In case of a failure, the streaming
   * dataflow will be restarted from the latest completed checkpoint.
   *
   * The job draws checkpoints periodically, in the given interval. The program will use
   * [[CheckpointingMode.EXACTLY_ONCE]] mode. The state will be stored in the
   * configured state backend.
   *
   * NOTE: Checkpointing iterative streaming dataflows in not properly supported at
   * the moment. For that reason, iterative jobs will not be started if used
   * with enabled checkpointing. To override this mechanism, use the 
   * [[enableCheckpointing(long, CheckpointingMode, boolean)]] method.
   *
   * @param interval 
   *           Time interval between state checkpoints in milliseconds.
   */
  def enableCheckpointing(interval : Long) : StreamExecutionEnvironment = {
    enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE)
  }

  /**
   * Method for enabling fault-tolerance. Activates monitoring and backup of streaming
   * operator states. Time interval between state checkpoints is specified in in millis.
   *
   * Setting this option assumes that the job is used in production and thus if not stated
   * explicitly otherwise with calling the [[setRestartStrategy]] method in case of
   * failure the job will be resubmitted to the cluster indefinitely.
   */
  @deprecated
  @PublicEvolving
  def enableCheckpointing() : StreamExecutionEnvironment = {
    javaEnv.enableCheckpointing()
    this
  }
  
  def getCheckpointingMode = javaEnv.getCheckpointingMode()

  /**
   * Sets the state backend that describes how to store and checkpoint operator state. It defines
   * both which data structures hold state during execution (for example hash tables, RockDB,
   * or other data stores) as well as where checkpointed data will be persisted.
   *
   * State managed by the state backend includes both keyed state that is accessible on
   * [[org.apache.flink.streaming.api.datastream.KeyedStream keyed streams]], as well as
   * state maintained directly by the user code that implements
   * [[org.apache.flink.streaming.api.checkpoint.CheckpointedFunction CheckpointedFunction]].
   *
   * The [[org.apache.flink.runtime.state.memory.MemoryStateBackend]], for example,
   * maintains the state in heap memory, as objects. It is lightweight without extra dependencies,
   * but can checkpoint only small states (some counters).
   *
   * In contrast, the [[org.apache.flink.runtime.state.filesystem.FsStateBackend]]
   * stores checkpoints of the state (also maintained as heap objects) in files.
   * When using a replicated file system (like HDFS, S3, MapR FS, Tachyon, etc) this will guarantee
   * that state is not lost upon failures of individual nodes and that streaming program can be
   * executed highly available and strongly consistent.
   */
  @PublicEvolving
  def setStateBackend(backend: StateBackend): StreamExecutionEnvironment = {
    javaEnv.setStateBackend(backend)
    this
  }

  /**
   * @deprecated Use [[StreamExecutionEnvironment.setStateBackend(StateBackend)]] instead.
   */
  @Deprecated
  @PublicEvolving
  def setStateBackend(backend: AbstractStateBackend): StreamExecutionEnvironment = {
    setStateBackend(backend.asInstanceOf[StateBackend])
  }

  /**
   * Returns the state backend that defines how to store and checkpoint state.
   */
  @PublicEvolving
  def getStateBackend: StateBackend = javaEnv.getStateBackend()

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
    javaEnv.getRestartStrategy()
  }

  /**
    * Sets the number of times that failed tasks are re-executed. A value of zero
    * effectively disables fault tolerance. A value of "-1" indicates that the system
    * default value (as defined in the configuration) should be used.
    *
    * @deprecated This method will be replaced by [[setRestartStrategy()]]. The
    *            FixedDelayRestartStrategyConfiguration contains the number of execution retries.
    */
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
  @PublicEvolving
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
  def addDefaultKryoSerializer[T <: Serializer[_] with Serializable](
      `type`: Class[_],
      serializer: T)
    : Unit = {
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
  //  Time characteristic
  // --------------------------------------------------------------------------------------------
  /**
   * Sets the time characteristic for all streams create from this environment, e.g., processing
   * time, event time, or ingestion time.
   *
   * If you set the characteristic to IngestionTime of EventTime this will set a default
   * watermark update interval of 200 ms. If this is not applicable for your application
   * you should change it using
   * [[org.apache.flink.api.common.ExecutionConfig#setAutoWatermarkInterval(long)]]
   *
   * @param characteristic The time characteristic.
   */
  @PublicEvolving
  def setStreamTimeCharacteristic(characteristic: TimeCharacteristic) : Unit = {
    javaEnv.setStreamTimeCharacteristic(characteristic)
  }

  /**
   * Gets the time characteristic/
   *
   * @see #setStreamTimeCharacteristic
   * @return The time characteristic.
   */
  @PublicEvolving
  def getStreamTimeCharacteristic = javaEnv.getStreamTimeCharacteristic()

  // --------------------------------------------------------------------------------------------
  // Data stream creations
  // --------------------------------------------------------------------------------------------

  /**
   * Creates a new DataStream that contains a sequence of numbers. This source is a parallel source.
   * If you manually set the parallelism to `1` the emitted elements are in order.
   */
  def generateSequence(from: Long, to: Long): DataStream[Long] = {
    new DataStream[java.lang.Long](javaEnv.generateSequence(from, to))
      .asInstanceOf[DataStream[Long]]
  }

  /**
   * Creates a DataStream that contains the given elements. The elements must all be of the
   * same type.
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
   */
  def fromElements[T: TypeInformation](data: T*): DataStream[T] = {
    fromCollection(data)
  }

  /**
   * Creates a DataStream from the given non-empty [[Seq]]. The elements need to be serializable
   * because the framework may move the elements into the cluster if needed.
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
   */
  def fromCollection[T: TypeInformation](data: Seq[T]): DataStream[T] = {
    require(data != null, "Data must not be null.")
    val typeInfo = implicitly[TypeInformation[T]]

    val collection = scala.collection.JavaConversions.asJavaCollection(data)
    asScalaStream(javaEnv.fromCollection(collection, typeInfo))
  }

  /**
   * Creates a DataStream from the given [[Iterator]].
   *
   * Note that this operation will result in a non-parallel data source, i.e. a data source with
   * a parallelism of one.
   */
  def fromCollection[T: TypeInformation] (data: Iterator[T]): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    asScalaStream(javaEnv.fromCollection(data.asJava, typeInfo))
  }

  /**
   * Creates a DataStream from the given [[SplittableIterator]].
   */
  def fromParallelCollection[T: TypeInformation] (data: SplittableIterator[T]):
      DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    asScalaStream(javaEnv.fromParallelCollection(data, typeInfo))
  }

  /**
   * Creates a DataStream that represents the Strings produced by reading the
   * given file line wise. The file will be read with the system's default
   * character set.
   */
  def readTextFile(filePath: String): DataStream[String] =
    asScalaStream(javaEnv.readTextFile(filePath))

  /**
   * Creates a data stream that represents the Strings produced by reading the given file
   * line wise. The character set with the given name will be used to read the files.
   */
  def readTextFile(filePath: String, charsetName: String): DataStream[String] =
    asScalaStream(javaEnv.readTextFile(filePath, charsetName))


  /**
   * Reads the given file with the given input format. The file path should be passed
   * as a URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
   */
  def readFile[T: TypeInformation](inputFormat: FileInputFormat[T], filePath: String):
        DataStream[T] =
    asScalaStream(javaEnv.readFile(inputFormat, filePath))

  /**
    * Creates a DataStream that contains the contents of file created while
    * system watches the given path. The file will be read with the system's
    * default character set. The user can check the monitoring interval in milliseconds,
    * and the way file modifications are handled. By default it checks for only new files
    * every 100 milliseconds.
    *
    */
  @Deprecated
  def readFileStream(StreamPath: String, intervalMillis: Long = 100,
                     watchType: FileMonitoringFunction.WatchType =
                     FileMonitoringFunction.WatchType.ONLY_NEW_FILES): DataStream[String] =
    asScalaStream(javaEnv.readFileStream(StreamPath, intervalMillis, watchType))

  /**
    * Reads the contents of the user-specified path based on the given [[FileInputFormat]].
    * Depending on the provided [[FileProcessingMode]].
    *
    * @param inputFormat
    *          The input format used to create the data stream
    * @param filePath
    *          The path of the file, as a URI (e.g., "file:///some/local/file" or
    *          "hdfs://host:port/file/path")
    * @param watchType
    *          The mode in which the source should operate, i.e. monitor path and react
    *          to new data, or process once and exit
    * @param interval
    *          In the case of periodic path monitoring, this specifies the interval (in millis)
    *          between consecutive path scans
    * @param filter
    *          The files to be excluded from the processing
    * @return The data stream that represents the data read from the given file
   * @deprecated Use [[FileInputFormat#setFilesFilter(FilePathFilter)]] to set a filter and
    * [[StreamExecutionEnvironment#readFile(FileInputFormat, String, FileProcessingMode, long)]]
    */
  @PublicEvolving
  @Deprecated
  def readFile[T: TypeInformation](
                                    inputFormat: FileInputFormat[T],
                                    filePath: String,
                                    watchType: FileProcessingMode,
                                    interval: Long,
                                    filter: FilePathFilter): DataStream[T] = {
    asScalaStream(javaEnv.readFile(inputFormat, filePath, watchType, interval, filter))
  }

  /**
    * Reads the contents of the user-specified path based on the given [[FileInputFormat]].
    * Depending on the provided [[FileProcessingMode]], the source
    * may periodically monitor (every `interval` ms) the path for new data
    * ([[FileProcessingMode.PROCESS_CONTINUOUSLY]]), or process
    * once the data currently in the path and exit
    * ([[FileProcessingMode.PROCESS_ONCE]]). In addition,
    * if the path contains files not to be processed, the user can specify a custom
    * [[FilePathFilter]]. As a default implementation you can use
    * [[FilePathFilter.createDefaultFilter()]].
    *
    * ** NOTES ON CHECKPOINTING: ** If the `watchType` is set to
    * [[FileProcessingMode#PROCESS_ONCE]], the source monitors the path ** once **,
    * creates the [[org.apache.flink.core.fs.FileInputSplit FileInputSplits]]
    * to be processed, forwards them to the downstream
    * [[ContinuousFileReaderOperator readers]] to read the actual data,
    * and exits, without waiting for the readers to finish reading. This
    * implies that no more checkpoint barriers are going to be forwarded
    * after the source exits, thus having no checkpoints after that point.
    *
    * @param inputFormat
    *          The input format used to create the data stream
    * @param filePath
    *          The path of the file, as a URI (e.g., "file:///some/local/file" or
    *          "hdfs://host:port/file/path")
    * @param watchType
    *          The mode in which the source should operate, i.e. monitor path and react
    *          to new data, or process once and exit
    * @param interval
    *          In the case of periodic path monitoring, this specifies the interval (in millis)
    *          between consecutive path scans
    * @return The data stream that represents the data read from the given file
    */
  @PublicEvolving
  def readFile[T: TypeInformation](
      inputFormat: FileInputFormat[T],
      filePath: String,
      watchType: FileProcessingMode,
      interval: Long): DataStream[T] = {
    val typeInfo = implicitly[TypeInformation[T]]
    asScalaStream(javaEnv.readFile(inputFormat, filePath, watchType, interval, typeInfo))
  }

  /**
   * Creates a new DataStream that contains the strings received infinitely
   * from socket. Received strings are decoded by the system's default
   * character set. The maximum retry interval is specified in seconds, in case
   * of temporary service outage reconnection is initiated every second.
   */
  @PublicEvolving
  def socketTextStream(hostname: String, port: Int, delimiter: Char = '\n', maxRetry: Long = 0):
        DataStream[String] =
    asScalaStream(javaEnv.socketTextStream(hostname, port))

  /**
   * Generic method to create an input data stream with a specific input format.
   * Since all data streams need specific information about their types, this method needs to
   * determine the type of the data produced by the input format. It will attempt to determine the
   * data type by reflection, unless the input format implements the ResultTypeQueryable interface.
   */
  @PublicEvolving
  def createInput[T: TypeInformation](inputFormat: InputFormat[T, _]): DataStream[T] =
    if (inputFormat.isInstanceOf[ResultTypeQueryable[_]]) {
      asScalaStream(javaEnv.createInput(inputFormat))
    } else {
      asScalaStream(javaEnv.createInput(inputFormat, implicitly[TypeInformation[T]]))
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
  def addSource[T: TypeInformation](function: SourceFunction[T]): DataStream[T] = {
    require(function != null, "Function must not be null.")
    
    val cleanFun = scalaClean(function)
    val typeInfo = implicitly[TypeInformation[T]]
    asScalaStream(javaEnv.addSource(cleanFun).returns(typeInfo))
  }

  /**
   * Create a DataStream using a user defined source function for arbitrary
   * source functionality.
   */
  def addSource[T: TypeInformation](function: SourceContext[T] => Unit): DataStream[T] = {
    require(function != null, "Function must not be null.")
    val sourceFunction = new SourceFunction[T] {
      val cleanFun = scalaClean(function)
      override def run(ctx: SourceContext[T]) {
        cleanFun(ctx)
      }
      override def cancel() = {}
    }
    addSource(sourceFunction)
  }

  /**
   * Triggers the program execution. The environment will execute all parts of
   * the program that have resulted in a "sink" operation. Sink operations are
   * for example printing results or forwarding them to a message queue.
   * 
   * The program execution will be logged and displayed with a generated
   * default name.
   */
  def execute() = javaEnv.execute()

  /**
   * Triggers the program execution. The environment will execute all parts of
   * the program that have resulted in a "sink" operation. Sink operations are
   * for example printing results or forwarding them to a message queue.
   * 
   * The program execution will be logged and displayed with the provided name.
   */
  def execute(jobName: String) = javaEnv.execute(jobName)

  /**
   * Creates the plan with which the system will execute the program, and
   * returns it as a String using a JSON representation of the execution data
   * flow graph. Note that this needs to be called, before the plan is
   * executed.
   */
  def getExecutionPlan = javaEnv.getExecutionPlan

  /**
   * Getter of the [[org.apache.flink.streaming.api.graph.StreamGraph]] of the streaming job.
   *
   * @return The StreamGraph representing the transformations
   */
  @Internal
  def getStreamGraph = javaEnv.getStreamGraph

  /**
   * Getter of the wrapped [[org.apache.flink.streaming.api.environment.StreamExecutionEnvironment]]
 *
   * @return The encased ExecutionEnvironment
   */
  @Internal
  def getWrappedStreamExecutionEnvironment = javaEnv

  /**
   * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
   * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]]
   */
  private[flink] def scalaClean[F <: AnyRef](f: F): F = {
    if (getConfig.isClosureCleanerEnabled) {
      ClosureCleaner.clean(f, true)
    } else {
      ClosureCleaner.ensureSerializable(f)
    }
    f
  }

  /**
    * Registers a file at the distributed cache under the given name. The file will be accessible
    * from any user-defined function in the (distributed) runtime under a local path. Files
    * may be local files (which will be distributed via BlobServer), or files in a distributed file
    * system. The runtime will copy the files temporarily to a local cache, if needed.
    *
    * The {@link org.apache.flink.api.common.functions.RuntimeContext} can be obtained inside UDFs
    * via {@link org.apache.flink.api.common.functions.RichFunction#getRuntimeContext()} and
    * provides access {@link org.apache.flink.api.common.cache.DistributedCache} via
    * {@link org.apache.flink.api.common.functions.RuntimeContext#getDistributedCache()}.
    *
    * @param filePath The path of the file, as a URI (e.g. "file:///some/path" or
    *                 "hdfs://host:port/and/path")
    * @param name     The name under which the file is registered.
    */
  def registerCachedFile(filePath: String, name: String): Unit = {
    javaEnv.registerCachedFile(filePath, name)
  }

  /**
    * Registers a file at the distributed cache under the given name. The file will be accessible
    * from any user-defined function in the (distributed) runtime under a local path. Files
    * may be local files (which will be distributed via BlobServer), or files in a distributed file
    * system. The runtime will copy the files temporarily to a local cache, if needed.
    *
    * The {@link org.apache.flink.api.common.functions.RuntimeContext} can be obtained inside UDFs
    * via {@link org.apache.flink.api.common.functions.RichFunction#getRuntimeContext()} and
    * provides access {@link org.apache.flink.api.common.cache.DistributedCache} via
    * {@link org.apache.flink.api.common.functions.RuntimeContext#getDistributedCache()}.
    *
    * @param filePath   The path of the file, as a URI (e.g. "file:///some/path" or
    *                   "hdfs://host:port/and/path")
    * @param name       The name under which the file is registered.
    * @param executable flag indicating whether the file should be executable
    */
  def registerCachedFile(filePath: String, name: String, executable: Boolean): Unit = {
    javaEnv.registerCachedFile(filePath, name, executable)
  }
}

object StreamExecutionEnvironment {

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
  def getExecutionEnvironment: StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.getExecutionEnvironment)
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
      StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironment(parallelism))
  }

  /**
   * Creates a local execution environment. The local execution environment will run the
   * program in a multi-threaded fashion in the same JVM as the environment was created in.
   *
   * @param parallelism   The parallelism for the local environment.
   * @param configuration Pass a custom configuration into the cluster.
   */
  def createLocalEnvironment(parallelism: Int, configuration: Configuration):
  StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironment(parallelism, configuration))
  }

  /**
   * Creates a [[StreamExecutionEnvironment]] for local program execution that also starts the
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
  def createLocalEnvironmentWithWebUI(config: Configuration = null): StreamExecutionEnvironment = {
    val conf: Configuration = if (config == null) new Configuration() else config
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironmentWithWebUI(conf))
  }

  // --------------------------------------------------------------------------
  //  remote environment
  // --------------------------------------------------------------------------

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
