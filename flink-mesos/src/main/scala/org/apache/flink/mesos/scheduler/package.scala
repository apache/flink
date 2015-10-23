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

package org.apache.flink.mesos

import java.nio.file.Paths

import org.apache.mesos.Protos.{SlaveID, TaskID}

package object scheduler {

  // configuration for streamingMode
  val STREAMING_MODE_KEY = "streamingMode"
  val DEFAULT_STREAMING_MODE = "batch"

  // maximum number of task managers, by default we will greedily consume whole cluster
  val TASK_MANAGER_COUNT_KEY = "flink.mesos.taskmanagers.maxcount"
  val DEFAULT_TASK_MANAGER_COUNT = Integer.MAX_VALUE

  // number of CPU cores to be allocated per task manager, default = 1 core
  val TASK_MANAGER_CPU_KEY = "flink.mesos.taskmanagers.cpu"
  val DEFAULT_TASK_MANAGER_CPU = 1.0F // 1 core

  // Amount of memory (in MiB) to be allocated per task manager, default = 512MiB
  val TASK_MANAGER_MEM_KEY = "flink.mesos.taskmanagers.mem"
  val DEFAULT_TASK_MANAGER_MEM = 512.0F // 1GB

  // Amount of disk (in MiB) to be allocated per task manager, default = 1GB
  val TASK_MANAGER_DISK_KEY = "flink.mesos.taskmanager.disk"
  val DEFAULT_TASK_MANGER_DISK = 1024.0F // 1GB

  // offer attribute constraints to be checked while accepting offers, default = no constraints
  val TASK_MANAGER_OFFER_ATTRIBUTES_KEY = "flink.mesos.taskmanager.attributes"
  val DEFAULT_TASK_MANAGER_OFFER_ATTRIBUTES = ""

  // offer role to use for resource offers
  val MESOS_FRAMEWORK_ROLE_KEY = "flink.mesos.framework.role"
  val DEFAULT_MESOS_FRAMEWORK_ROLE = "*"

  // additional JVM arguments for task managers
  val TASK_MANAGER_JVM_ARGS_KEY = "flink.mesos.taskmanager.jvmArgs"
  val DEFAULT_TASK_MANAGER_JVM_ARGS = "-server " +
                                      "-XX:+UseConcMarkSweepGC " +
                                      "-XX:+CMSParallelRemarkEnabled " +
                                      "-XX:+CMSClassUnloadingEnabled " +
                                      "-XX:+UseParNewGC " +
                                      "-XX:+UseCompressedOops " +
                                      "-XX:+UseFastEmptyMethods " +
                                      "-XX:+UseFastAccessorMethods " +
                                      "-XX:+AlwaysPreTouch"

  // mesos-slave visible url to the flink uberjar (that contains the TaskManagerExecutor)
  val FLINK_UBERJAR_LOCATION_KEY = "flink.uberjar.location"
  val DEFAULT_FLINK_UBERJAR_LOCATION = "file:///vagrant/flink-dist-0.10-SNAPSHOT.jar"

  // path to the native library of the slave
  val MESOS_NATIVE_JAVA_LIBRARY_KEY = "mesos.native.lib"
  val DEFAULT_MESOS_NATIVE_JAVA_LIBRARY = "/usr/local/lib/libmesos.so"

  // This is the memory overhead for a jvm process. This needs to be added
  // to a jvm process's resource requirement, in addition to its heap size.
  val JVM_MEM_OVERHEAD_PERCENT_DEFAULT = 0.20

  // The user to use to spawn the taskManager, default = same as the scheduler
  val MESOS_FRAMEWORK_USER_KEY = "flink.mesos.framework.user"
  val DEFAULT_MESOS_FRAMEWORK_USER = ""

  // Framework Id to use for the mesos scheduler, default = mesos-assigned id is used
  val MESOS_FRAMEWORK_ID_KEY = "flink.mesos.framework.id"
  val DEFAULT_MESOS_FRAMEWORK_ID = null

  // Human friendly framework name (to be used in the mesos UI)
  val MESOS_FRAMEWORK_NAME_KEY = "flink.mesos.framework.name"
  val DEFAULT_MESOS_FRAMEWORK_NAME = "Apache Flink on Mesos"

  // Mesos authentication principal to use, default = no authentication
  val MESOS_FRAMEWORK_PRINCIPAL_KEY = "flink.mesos.framework.principal"
  val DEFAULT_MESOS_FRAMEWORK_PRINCIPAL = null

  // Mesos authentication secret to use, default = no authentication
  val MESOS_FRAMEWORK_SECRET_KEY = "flink.mesos.framework.secret"
  val DEFAULT_MESOS_FRAMEWORK_SECRET = null

  // Mesos master url (generally the zookeeper path at witch mesos master is registered)
  val MESOS_MASTER_KEY = "flink.mesos.master"
  val DEFAULT_MESOS_MASTER = "zk://127.0.0.1:2181/mesos"

  case class Conf(confDir: String = Paths.get(".").toAbsolutePath.toString,
                  host: String = "",
                  port: Int = -1)
  sealed case class RunningTaskManager(taskId: TaskID,
                                       slaveId: SlaveID)

}
