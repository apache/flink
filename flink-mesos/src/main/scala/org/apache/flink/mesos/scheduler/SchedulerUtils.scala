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

package org.apache.flink.mesos.scheduler

import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.google.common.base.Splitter
import com.google.protobuf.{ByteString, GeneratedMessage}
import org.apache.flink.configuration.{ConfigConstants, Configuration, GlobalConfiguration}
import org.apache.flink.configuration.ConfigConstants._
import org.apache.flink.mesos._
import org.apache.flink.mesos.scheduler.FlinkScheduler._
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.jobmanager.{JobManager, JobManagerMode}
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.mesos.{MesosSchedulerDriver, Scheduler, SchedulerDriver}
import org.apache.mesos.Protos._
import org.apache.mesos.Protos.CommandInfo.URI
import org.apache.mesos.Protos.Value.Ranges
import org.apache.mesos.Protos.Value.Type._

/**
 * This code is borrowed and inspired from Apache Spark Project:
 *   core/src/main/scala/org/apache/spark/scheduler/cluster/mesos/MesosSchedulerUtils.scala
 */
trait SchedulerUtils {

  /**
   * Converts the attributes from the resource offer into a Map of name -> Attribute Value
   * The attribute values are the mesos attribute types and they are
   * @param offerAttributes List of attributes sent with an offer
   * @return
   */
  def toAttributeMap(offerAttributes: JList[Attribute]): Map[String, GeneratedMessage] = {
    offerAttributes.map(attr => {
      val attrValue = attr.getType match {
        case SCALAR => attr.getScalar
        case Value.Type.RANGES => attr.getRanges
        case Value.Type.SET => attr.getSet
        case Value.Type.TEXT => attr.getText
      }
      (attr.getName, attrValue)
    }).toMap
  }

  def createJavaExecCommand(jvmArgs: String = "", classPath: String = "flink-*.jar",
                            classToExecute: String, args: String = ""): String = {
    s"env; java $jvmArgs -cp $classPath $classToExecute $args"
  }

  def createExecutorInfo(id: String, role: String, artifactURIs: Set[String], command: String,
                         nativeLibPath: String): ExecutorInfo = {
    val uris = artifactURIs.map(uri => URI.newBuilder().setValue(uri).build())
    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID
        .newBuilder()
        .setValue(s"executor_$id"))
      .setName(s"Apache Flink Mesos Executor - $id")
      .setCommand(CommandInfo.newBuilder()
        .setValue(s"env; $command")
        .addAllUris(uris)
        .setEnvironment(Environment.newBuilder()
          .addVariables(Environment.Variable.newBuilder()
            .setName("MESOS_NATIVE_JAVA_LIBRARY").setValue(nativeLibPath)))
        .setValue(command))
      .build()
  }

  def createTaskInfo(taskName: String, taskId: TaskID, slaveID: SlaveID, role: String, mem: Double,
                     cpus: Double, disk: Double, ports: Set[Int], executorInfo: ExecutorInfo,
                     conf: Configuration): TaskInfo = {

    val portRanges = Ranges.newBuilder().addAllRange(
      ports.map(port => Value.Range.newBuilder().setBegin(port).setEnd(port).build())).build()

    val taskConf = conf.clone()
    val portsSeq = ports.toSeq
    // set task manager ports
    taskConf.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, portsSeq.get(0))
    taskConf.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, portsSeq.get(1))

    TaskInfo.newBuilder()
      .setData(ByteString.copyFrom(Utils.serialize(taskConf)))
      .setExecutor(executorInfo)
      .setName(taskId.getValue)
      .setTaskId(taskId)
      .setSlaveId(slaveID)
      .addResources(Resource.newBuilder()
        .setName("ports").setType(RANGES)
        .setRole(role)
        .setRanges(portRanges))
      .addResources(Resource.newBuilder()
        .setName("mem").setType(SCALAR)
        .setRole(role)
        .setScalar(Value.Scalar.newBuilder().setValue(mem)))
      .addResources(Resource.newBuilder()
        .setName("cpus").setType(SCALAR)
        .setRole(role)
        .setScalar(Value.Scalar.newBuilder().setValue(cpus)))
      .addResources(Resource.newBuilder()
        .setName("disk").setType(SCALAR)
        .setRole(role)
        .setScalar(Value.Scalar.newBuilder().setValue(disk)))
      .build()
  }


  def getResource(res: JList[Resource], name: String): Double = {
    // A resource can have multiple values in the offer since it can
    // either be from a specific role or wildcard.
    res.filter(_.getName == name).map(_.getScalar.getValue).sum
  }

  /**
   * Match the requirements (if any) to the offer attributes.
   * if attribute requirements are not specified - return true
   * else if attribute is defined and no values are given, simple attribute presence is performed
   * else if attribute name and value is specified, subset match is performed on slave attributes
   *
   * This code is borrowed and inspired from: Apache Spark:
   *  core/src/main/scala/org/apache/spark/scheduler/cluster/mesos/MesosSchedulerUtils.scala
   */
  def matchesAttributeRequirements(
                                    slaveOfferConstraints: Map[String, Set[String]],
                                    offerAttributes: Map[String, GeneratedMessage]): Boolean = {
    slaveOfferConstraints.forall {
      // offer has the required attribute and subsumes the required values for that attribute
      case (name, requiredValues) =>
        offerAttributes.get(name) match {
          case None => false
          case Some(_) if requiredValues.isEmpty => true // empty value matches presence
          case Some(scalarValue: Value.Scalar) =>
            // check if provided values is less than equal to the offered values
            requiredValues.map(_.toDouble).exists(_ <= scalarValue.getValue)
          case Some(rangeValue: Value.Range) =>
            val offerRange = rangeValue.getBegin to rangeValue.getEnd
            // Check if there is some required value that is between the ranges specified
            // Note: We only support the ability to specify discrete values, in the future
            // we may expand it to subsume ranges specified with a XX..YY value or something
            // similar to that.
            requiredValues.map(_.toLong).exists(offerRange.contains(_))
          case Some(offeredValue: Value.Set) =>
            // check if the specified required values is a subset of offered set
            requiredValues.subsetOf(offeredValue.getItemList.toSet)
          case Some(textValue: Value.Text) =>
            // check if the specified value is equal, if multiple values are specified
            // we succeed if any of them match.
            requiredValues.contains(textValue.getValue)
        }
    }
  }

  /**
   * Parses the attributes constraints provided to spark and build a matching data struct:
   * Map[<attribute-name>, Set[values-to-match]]
   * The constraints are specified as ';' separated key-value pairs where keys and values
   * are separated by ':'. The ':' implies equality (for singular values) and "is one of" for
   * multiple values (comma separated). For example:
   * {{{
   *  parseConstraintString("tachyon:true;zone:us-east-1a,us-east-1b")
   *  // would result in
   * <code>
   * Map(
   * "tachyon" -> Set("true"),
   * "zone":   -> Set("us-east-1a", "us-east-1b")
   * )
   * }}}
   *
   * Mesos documentation: http://mesos.apache.org/documentation/attributes-resources/
   * https://github.com/apache/mesos/blob/master/src/common/values.cpp
   * https://github.com/apache/mesos/blob/master/src/common/attributes.cpp
   *
   * This code is borrowed and inspired from: Apache Spark:
   *  core/src/main/scala/org/apache/spark/scheduler/cluster/mesos/MesosSchedulerUtils.scala
   *
   * @param constraintsVal constaints string consisting of ';' separated key-value pairs (separated
   * by ':')
   * @return  Map of constraints to match resources offers.
   */
  def parseConstraintString(constraintsVal: String): Map[String, Set[String]] = {
    /*
      Based on mesos docs:
      attributes : attribute ( ";" attribute )*
      attribute : labelString ":" ( labelString | "," )+
      labelString : [a-zA-Z0-9_/.-]
    */
    val splitter = Splitter.on(';').trimResults().withKeyValueSeparator(':')
    // kv splitter
    if (constraintsVal.isEmpty) {
      Map()
    } else {
      try {
        Map() ++ mapAsScalaMap(splitter.split(constraintsVal)).map {
          case (k, v) =>
            if (v == null || v.isEmpty) {
              (k, Set[String]())
            } else {
              (k, v.split(',').toSet)
            }
        }
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"Bad constraint string: $constraintsVal", e)
      }
    }
  }

  def createFrameworkInfoAndCredentials(cliConf: Conf): (FrameworkInfo, Option[Credential]) = {
    // start scheduler
    val frameworkBuilder = FrameworkInfo.newBuilder()

    frameworkBuilder.setUser(GlobalConfiguration.getString(
      MESOS_FRAMEWORK_USER_KEY, DEFAULT_MESOS_FRAMEWORK_USER))
    val frameworkId = GlobalConfiguration.getString(
      MESOS_FRAMEWORK_ID_KEY, DEFAULT_MESOS_FRAMEWORK_ID)
    if (frameworkId != null) {
      frameworkBuilder.setId(FrameworkID.newBuilder().setValue(frameworkId))
    }

    frameworkBuilder.setRole(GlobalConfiguration.getString(
      MESOS_FRAMEWORK_ROLE_KEY, DEFAULT_MESOS_FRAMEWORK_ROLE))
    frameworkBuilder.setName(GlobalConfiguration.getString(
      MESOS_FRAMEWORK_NAME_KEY, DEFAULT_MESOS_FRAMEWORK_NAME))
    val webUIPort = GlobalConfiguration.getInteger(JOB_MANAGER_WEB_PORT_KEY, -1)
    if (webUIPort > 0) {
      val webUIHost = GlobalConfiguration.getString(
        JOB_MANAGER_IPC_ADDRESS_KEY, cliConf.host)
      frameworkBuilder.setWebuiUrl(s"http://$webUIHost:$webUIPort")
    }

    var credsBuilder: Credential.Builder = null
    val principal = GlobalConfiguration.getString(
      MESOS_FRAMEWORK_PRINCIPAL_KEY, DEFAULT_MESOS_FRAMEWORK_PRINCIPAL)
    if (principal != null) {
      frameworkBuilder.setPrincipal(principal)

      credsBuilder = Credential.newBuilder()
      credsBuilder.setPrincipal(principal)
      val secret = GlobalConfiguration.getString(
        MESOS_FRAMEWORK_SECRET_KEY, DEFAULT_MESOS_FRAMEWORK_SECRET)
      if (secret != null) {
        credsBuilder.setSecret(ByteString.copyFromUtf8(secret))
      }
    }
    val credential = if (credsBuilder != null) {
      Some(credsBuilder.build)
    } else {
      None
    }

    (frameworkBuilder.build(), credential)
  }


  def createTaskManagerCommand(mem: Int): String = {
    val log4jArgs = "-Dlog4j.configuration=log4j-mesos.properties"
    val tmJVMHeap = math.round(mem / (1 + JVM_MEM_OVERHEAD_PERCENT_DEFAULT))
    val tmJVMArgs = GlobalConfiguration.getString(
      TASK_MANAGER_JVM_ARGS_KEY, DEFAULT_TASK_MANAGER_JVM_ARGS)
    createJavaExecCommand(
      jvmArgs = s"$tmJVMArgs -Xmx${tmJVMHeap}m $log4jArgs",
      classToExecute = "org.apache.flink.mesos.executor.TaskManagerExecutor")
  }

  def getNPortsFromPortRanges(n: Int, portRanges: Seq[Value.Range]): Set[Int] = {
    var ports: Set[Int] = Set()

    // find some ports
    for (x <- portRanges) {
      var begin = x.getBegin
      val end = x.getEnd

      while (begin < end && ports.size < n) {
        ports += begin.toInt
        begin += 1
      }

      if (ports.size == n) {
        return ports
      }
    }

    ports
  }

  def checkEnvironment(args: Array[String]): Unit = {
    EnvironmentInformation.logEnvironmentInfo(LOG, "JobManager", args)
    EnvironmentInformation.checkJavaVersion()
    val maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit
    if (maxOpenFileHandles != -1) {
      LOG.info(s"Maximum number of open file descriptors is $maxOpenFileHandles")
    } else {
      LOG.info("Cannot determine the maximum number of open file descriptors")
    }
  }

  def createDriver(scheduler: Scheduler, fwInfo: FrameworkInfo,
                   creds: Option[Credential]): SchedulerDriver = {
    val master = GlobalConfiguration.getString(MESOS_MASTER_KEY, DEFAULT_MESOS_MASTER)
    creds match {
      case None => new MesosSchedulerDriver(scheduler, fwInfo, master)
      case Some(c) => new MesosSchedulerDriver(scheduler, fwInfo, master, c)
    }
  }

  def createJobManagerThread(host: String): Thread = {
    new Thread {
      override def run(): Unit = {
        // start the job
        startJobManager(host) match {
          case Success(_) =>
            LOG.info("JobManager finished")
            sys.exit(0)
          case Failure(throwable) =>
            LOG.error("Caught exception, committing suicide.", throwable)
            sys.exit(1)
        }
      }
    }
  }

  def startJobManager(hostName: String): Try[Unit] = {
    val conf = GlobalConfiguration.getConfiguration
    val executionMode = JobManagerMode.CLUSTER
    val listeningHost = conf.getString(JOB_MANAGER_IPC_ADDRESS_KEY, hostName)
    val listeningPort = conf.getInteger(JOB_MANAGER_IPC_PORT_KEY, DEFAULT_JOB_MANAGER_IPC_PORT)
    val streamingMode =
      conf.getString(STREAMING_MODE_KEY, DEFAULT_STREAMING_MODE) match {
        case "streaming" => StreamingMode.STREAMING
        case _ => StreamingMode.BATCH_ONLY
      }

    // add jobmanager configuration
    conf.setString(JOB_MANAGER_IPC_ADDRESS_KEY, listeningHost)
    conf.setInteger(JOB_MANAGER_IPC_PORT_KEY, listeningPort)

    currentConfiguration = Some(conf)

    // start jobManager
    Try(JobManager.runJobManager(conf, executionMode, streamingMode, listeningHost, listeningPort))
  }
}
