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

/** Constants for kubernetes. */
public class Constants {

    // Kubernetes api version
    public static final String API_VERSION = "v1";
    public static final String APPS_API_VERSION = "apps/v1";

    public static final String DNS_POLICY_DEFAULT = "ClusterFirst";
    public static final String DNS_POLICY_HOSTNETWORK = "ClusterFirstWithHostNet";

    public static final String CONFIG_FILE_LOGBACK_NAME = "logback-console.xml";
    public static final String CONFIG_FILE_LOG4J_NAME = "log4j-console.properties";
    public static final String ENV_FLINK_LOG_DIR = "FLINK_LOG_DIR";

    public static final String MAIN_CONTAINER_NAME = "flink-main-container";

    public static final String FLINK_CONF_VOLUME = "flink-config-volume";
    public static final String CONFIG_MAP_PREFIX = "flink-config-";

    public static final String HADOOP_CONF_VOLUME = "hadoop-config-volume";
    public static final String HADOOP_CONF_CONFIG_MAP_PREFIX = "hadoop-config-";
    public static final String HADOOP_CONF_DIR_IN_POD = "/opt/hadoop/conf";
    public static final String ENV_HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    public static final String ENV_HADOOP_HOME = "HADOOP_HOME";

    public static final String KERBEROS_KEYTAB_VOLUME = "kerberos-keytab-volume";
    public static final String KERBEROS_KEYTAB_SECRET_PREFIX = "kerberos-keytab-";
    public static final String KERBEROS_KEYTAB_MOUNT_POINT = "/opt/kerberos/kerberos-keytab";
    public static final String KERBEROS_KRB5CONF_VOLUME = "kerberos-krb5conf-volume";
    public static final String KERBEROS_KRB5CONF_CONFIG_MAP_PREFIX = "kerberos-krb5conf-";
    public static final String KERBEROS_KRB5CONF_MOUNT_DIR = "/etc";
    public static final String KERBEROS_KRB5CONF_FILE = "krb5.conf";

    public static final String FLINK_REST_SERVICE_SUFFIX = "-rest";

    public static final String NAME_SEPARATOR = "-";

    // Constants for label builder
    public static final String LABEL_TYPE_KEY = "type";
    public static final String LABEL_TYPE_NATIVE_TYPE = "flink-native-kubernetes";
    public static final String LABEL_APP_KEY = "app";
    public static final String LABEL_COMPONENT_KEY = "component";
    public static final String LABEL_COMPONENT_JOB_MANAGER = "jobmanager";
    public static final String LABEL_COMPONENT_TASK_MANAGER = "taskmanager";
    public static final String LABEL_CONFIGMAP_TYPE_KEY = "configmap-type";
    public static final String LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY = "high-availability";

    // Use fixed port in kubernetes, it needs to be exposed.
    public static final int REST_PORT = 8081;
    public static final int BLOB_SERVER_PORT = 6124;
    public static final int TASK_MANAGER_RPC_PORT = 6122;

    public static final String JOB_MANAGER_RPC_PORT_NAME = "jobmanager-rpc";
    public static final String BLOB_SERVER_PORT_NAME = "blobserver";
    public static final String REST_PORT_NAME = "rest";
    public static final String TASK_MANAGER_RPC_PORT_NAME = "taskmanager-rpc";

    public static final String RESOURCE_NAME_MEMORY = "memory";

    public static final String RESOURCE_NAME_CPU = "cpu";

    public static final String RESOURCE_UNIT_MB = "Mi";

    public static final String ENV_FLINK_POD_IP_ADDRESS = "_POD_IP_ADDRESS";

    public static final String POD_IP_FIELD_PATH = "status.podIP";

    public static final String ENV_FLINK_POD_NODE_ID = "_POD_NODE_ID";

    public static final String POD_NODE_ID_FIELD_PATH = "spec.nodeName";

    public static final int MAXIMUM_CHARACTERS_OF_CLUSTER_ID = 45;

    public static final String RESTART_POLICY_OF_NEVER = "Never";

    // Constants for Kubernetes high availability
    public static final String LEADER_ADDRESS_KEY = "address";
    public static final String LEADER_SESSION_ID_KEY = "sessionId";
    public static final String JOB_GRAPH_STORE_KEY_PREFIX = "jobGraph-";
    public static final String SUBMITTED_JOBGRAPH_FILE_PREFIX = "submittedJobGraph";
    public static final String CHECKPOINT_COUNTER_KEY = "counter";
    public static final String CHECKPOINT_ID_KEY_PREFIX = "checkpointID-";
    public static final String COMPLETED_CHECKPOINT_FILE_SUFFIX = "completedCheckpoint";

    // The file name of the mounted task manager pod template in the JobManager pod if there was any
    // specified.
    public static final String TASK_MANAGER_POD_TEMPLATE_FILE_NAME =
            "taskmanager-pod-template.yaml";
    public static final String POD_TEMPLATE_DIR_IN_POD = "/opt/flink/pod-template";
    public static final String POD_TEMPLATE_CONFIG_MAP_PREFIX = "pod-template-";
    public static final String POD_TEMPLATE_VOLUME = "pod-template-volume";

    // Kubernetes start scripts
    public static final String KUBERNETES_JOB_MANAGER_SCRIPT_PATH = "kubernetes-jobmanager.sh";
    public static final String KUBERNETES_TASK_MANAGER_SCRIPT_PATH = "kubernetes-taskmanager.sh";

    public static final String ENV_TM_JVM_MEM_OPTS = "FLINK_TM_JVM_MEM_OPTS";

    // "resourceVersion="0" is any resource version.It saves time to access etcd and improves
    // performance.
    // https://kubernetes.io/docs/reference/using-api/api-concepts/#the-resourceversion-parameter
    public static final String KUBERNETES_ZERO_RESOURCE_VERSION = "0";
}
