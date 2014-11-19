/**
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

package org.apache.flink.mesos;

public class MesosConstants {
	/**
	 * The config Parameter defining the address of the Mesos master node.
	 */
	public static final String MESOS_MASTER = "flink.mesos.master";

	/**
	 * The path to the Mesos native libraries.
	 */
	public static final String MESOS_LIB = "flink.mesos.library";

	/**
	 * The maximum TaskManager instances that should be launched.
	 */
	public static final String MESOS_MAX_TM_INSTANCES = "flink.mesos.maxtminstances";

	/**
	 * The parameter that describes whether Mesos should open the webclient interface of the Jobmanager.
	 */
	public static final String MESOS_USE_WEB = "flink.mesos.usewebclient";

	/**
	 * The location of the uberjar required on all Mesos nodes.
	 */
	public static final String MESOS_UBERJAR_LOCATION = "flink.mesos.uberjar";

	/**
	 * Configuration directory
	 */
	public static final String MESOS_CONF_DIR = "flink.mesos.conf";

	/**
	 * The cores that each TaskManager
	 */
	public static final String MESOS_TASK_MANAGER_CORES = "flink.mesos.tmcores";

	/**
	 * JobManager memory on each Mesos node
	 */
	public static final String MESOS_JOB_MANAGER_MEMORY = "flink.mesos.jmmemory";

	/**
	 * TaskManager memory on each Mesos node
	 */
	public static final String MESOS_TASK_MANAGER_MEMORY = "flink.mesos.tmmemory";

	/**
	 * Dynamic Properties
	 */
	public static final String MESOS_DYNAMIC_PROPERTIES = "flink.mesos.dynprops";

	//------------------DEFAULT VALUES----------------
	/**
	 * The cores that each TaskManager
	 */
	public static final Integer DEFAULT_MESOS_TASK_MANAGER_CORES = 1;

	/**
	 * JobManager memory on each Mesos node
	 */
	public static final Double DEFAULT_MESOS_JOB_MANAGER_MEMORY = 256.0;

	/**
	 * TaskManager memory on each Mesos node
	 */
	public static final Double DEFAULT_MESOS_TASK_MANAGER_MEMORY = 256.0;
}
