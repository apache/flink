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

package org.apache.flink.kubernetes.utils;

/**
 * Constants for kubernetes.
 */
public class Constants {

	// Kubernetes api version
	public static final String API_VERSION = "v1";
	public static final String APPS_API_VERSION = "apps/v1";

	public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

	public static final String FLINK_CONF_VOLUME = "flink-config-volume";
	public static final String CONFIG_MAP_PREFIX = "flink-config-";

	public static final String HADOOP_CONF_VOLUME = "hadoop-config-volume";
	public static final String HADOOP_CONF_CONFIG_MAP_PREFIX = "hadoop-config-";
	public static final String HADOOP_CONF_DIR_IN_POD = "/opt/hadoop/conf";
	public static final String ENV_HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
	public static final String ENV_HADOOP_HOME = "HADOOP_HOME";

	public static final String FLINK_REST_SERVICE_SUFFIX = "-rest";

	public static final String NAME_SEPARATOR = "-";

	// Constants for label builder
	public static final String LABEL_TYPE_KEY = "type";
	public static final String LABEL_TYPE_NATIVE_TYPE = "flink-native-kubernetes";
	public static final String LABEL_APP_KEY = "app";
	public static final String LABEL_COMPONENT_KEY = "component";
	public static final String LABEL_COMPONENT_JOB_MANAGER = "jobmanager";
	public static final String LABEL_COMPONENT_TASK_MANAGER = "taskmanager";

	// Use fixed port in kubernetes, it needs to be exposed.
	public static final int BLOB_SERVER_PORT = 6124;
	public static final int TASK_MANAGER_RPC_PORT = 6122;

	public static final String JOB_MANAGER_RPC_PORT_NAME = "jobmanager-rpc";
	public static final String BLOB_SERVER_PORT_NAME = "blobserver";
	public static final String REST_PORT_NAME = "rest";
	public static final String TASK_MANAGER_RPC_PORT_NAME = "taskmanager-rpc";

	public static final String RESOURCE_NAME_MEMORY = "memory";

	public static final String RESOURCE_NAME_CPU = "cpu";

	public static final String RESOURCE_UNIT_MB = "Mi";

	public static final String ENV_FLINK_CLASSPATH = "FLINK_CLASSPATH";

	public static final String ENV_FLINK_POD_NAME = "_FLINK_POD_NAME";

	public static final String ENV_FLINK_POD_IP_ADDRESS = "_POD_IP_ADDRESS";

	public static final String POD_IP_FIELD_PATH = "status.podIP";

	public static final String HEADLESS_SERVICE_CLUSTER_IP = "None";
}
