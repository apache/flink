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

import org.apache.flink.configuration.GlobalConfiguration
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {

  // job manager conf
  val jobManagerMem = opt[Int](short='m', name = "jobManager-mem", default = Some(256), descr = "Memory for JobManager Container [in MB]")
  val jobManagerCores = opt[Int](short='c', name = "jobManager-cpus", default = Some(1), descr = "Number of JobManager Cores")
  val useWeb = toggle(short = 'w', name = "web", descrYes = "Launch the web frontend on the JobManager node")

  // task manager conf
  val taskManagerMem = opt[Int](short='t', name = "taskManger-mem", default = Some(256), descr = "Memory for JobManager Container [in MB]")
  val taskManagerCores = opt[Int](short='p', name = "taskManger-cpus", default = Some(1), descr = "Maximum CPU cores per TaskManager")
  val maxTaskManagers = opt[Int](short = 'n', name = "taskManger-num", default= Some(10),  descr = "Number of Task Managers, greedy behaviour if not specified")
  val slotsPerTM = opt[Int](short='s', name = "slots", descr = "Number of slots per TaskManager")

  // general conf
  val slaveOfferConstraints = opt[String](short = 'a', name = "attribute-constraints", descr = "Resource offer constraints", default = Some(""))
  val memoryOverhead = opt[Int](short = 'o', name = "memoryOverhead", descr = "memory to add to each container for runtime overheads")
  val mesosLib = opt[String](name = "lib", descr = "Path to libmesos", required = true)
  val jarUrl = opt[String](name = "jar", descr = "Path to Flink uber jar", required = true)

  // get confDir parsed and loaded
  val confDir = opt[String](short = 'f', name = "confDir", descr = "Path to Flink configuration directory", required = true)
  val masterUrl = opt[String](short = 'u', name = "master", descr = "mesos master URL (mesos://localhost:5050 or mesos://zk://localhost:2181/mesos)", required = true)

  // validate and load configuration
  GlobalConfiguration.loadConfiguration(confDir())
  val flinkConfiguration = GlobalConfiguration.getConfiguration

}