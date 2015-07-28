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

package object scheduler {

  // configuration key and default value for streamingMode
  val JOB_MANAGER_STREAMING_MODE_KEY = "jobmanager.streamingMode"
  val DEFAULT_JOB_MANAGER_STREAMING_MODE = "batch"

  val TASK_MANAGER_COUNT_KEY = "flink.mesos.taskmanagers.maxcount"
  val DEFAULT_TASK_MANAGER_COUNT = Integer.MAX_VALUE // consume whole cluster

  val TASK_MANAGER_CPU_KEY = "flink.mesos.taskmanagers.cpu"
  val DEFAULT_TASK_MANAGER_CPU = 1.0 // 1 core

  val TASK_MANAGER_MEM_KEY = "flink.mesos.taskmanagers.mem"
  val DEFAULT_TASK_MANAGER_MEM = 1024 // 1GB

  val TASK_MANAGER_DISK_KEY = "flink.mesos.taskmanager.disk"
  val DEFAULT_TASK_MANGER_DISK = 1024 // 1GB

  val TASK_MANAGER_OFFER_ATTRIBUTES_KEY = "flink.mesos.taskmanager.attributes"

  val MESOS_FRAMEWORK_ROLE_KEY = "flink.mesos.framework.role"
  val DEFAULT_MESOS_FRAMEWORK_ROLE = "*"

  val TASK_MANAGER_JVM_ARGS_KEY = "flink.mesos.taskmanager.jvmArgs"
  val DEFAULT_TASK_MANAGER_JVM_ARGS = "-server " +
                                      "-XX:+UseConcMarkSweepGC " +
                                      "-XX:+CMSParallelRemarkEnabled " +
                                      "-XX:+CMSClassUnloadingEnabled " +
                                      "-XX:+UseParNewGC " +
                                      "-XX:+UseCompressedOops " +
                                      "-XX:+UseFastEmptyMethods " +
                                      "-XX:+UseFastAccessorMethods " +
                                      "-XX:+AlwaysPreTouch " +
                                      "-Dlog4j.configuration=file:log4j-console.properties"

  val FLINK_UBERJAR_LOCATION_KEY = "flink.uberjar.location"
  val MESOS_NATIVE_JAVA_LIBRARY_KEY = "mesos.native.lib"
  val DEFAULT_MESOS_NATIVE_JAVA_LIBRARY = "/usr/local/lib/libmesos.so"

  // This is the memory overhead for a jvm process. This needs to be added
  // to a jvm process's resource requirement, in addition to its heap size.
  val JVM_MEM_OVERHEAD_PERCENT_DEFAULT = 0.20

  val MESOS_FRAMEWORK_USER_KEY = "flink.mesos.framework.user"
  val DEFAULT_MESOS_FRAMEWORK_USER = ""

  val MESOS_FRAMEWORK_ID_KEY = "flink.mesos.framework.id"
  val DEFAULT_MESOS_FRAMEWORK_ID = null

  val MESOS_FRAMEWORK_NAME_KEY = "flink.mesos.framework.name"
  val DEFAULT_MESOS_FRAMEWORK_NAME = "Apache Flink on Mesos"

  val MESOS_FRAMEWORK_FAILOVER_TIMEOUT_KEY = "flink.mesos.framework.failoverTimeout"
  val DEFAULT_MESOS_FRAMEWORK_FAILOVER_TIMEOUT = 300

  val MESOS_FRAMEWORK_PRINCIPAL_KEY = "flink.mesos.framework.principal"
  val DEFAULT_MESOS_FRAMEWORK_PRINCIPAL = null

  val MESOS_FRAMEWORK_SECRET_KEY = "flink.mesos.framework.secret"
  val DEFAULT_MESOS_FRAMEWORK_SECRET = null

  val MESOS_MASTER_KEY = "flink.mesos.master"
  val DEFAULT_MESOS_MASTER = "localhost:5050"
}
