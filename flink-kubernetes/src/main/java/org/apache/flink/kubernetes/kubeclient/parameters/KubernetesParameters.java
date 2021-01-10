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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;

import io.fabric8.kubernetes.api.model.LocalObjectReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A common collection of parameters that is used to construct the JobManager/TaskManager Pods,
 * including the accompanying Kubernetes resources that together represent a Flink application.
 */
public interface KubernetesParameters {

    String getConfigDirectory();

    String getClusterId();

    String getNamespace();

    String getImage();

    KubernetesConfigOptions.ImagePullPolicy getImagePullPolicy();

    LocalObjectReference[] getImagePullSecrets();

    /**
     * A common collection of labels that are attached to every created Kubernetes resources. This
     * can include the Deployment, the Pod(s), the ConfigMap(s), and the Service(s), etc.
     */
    Map<String, String> getCommonLabels();

    /** A collection of labels that are attached to the JobManager and TaskManager Pod(s). */
    Map<String, String> getLabels();

    /**
     * A collection of node selector to constrain a pod to only be able to run on particular
     * node(s).
     */
    Map<String, String> getNodeSelector();

    /**
     * A collection of customized environments that are attached to the JobManager and TaskManager
     * Container(s).
     */
    Map<String, String> getEnvironments();

    /** A map of user-specified annotations that are set to the JobManager and TaskManager pods. */
    Map<String, String> getAnnotations();

    /**
     * A collection of tolerations that are set to the JobManager and TaskManager Pod(s). Kubernetes
     * taints and tolerations work together to ensure that pods are not scheduled onto inappropriate
     * nodes.
     */
    List<Map<String, String>> getTolerations();

    /** Directory in Pod that stores the flink-conf.yaml, log4j.properties, and the logback.xml. */
    String getFlinkConfDirInPod();

    /** Directory in Pod that saves the log files. */
    String getFlinkLogDirInPod();

    /** The docker entrypoint that starts processes in the container. */
    String getContainerEntrypoint();

    /** Whether the logback.xml is located. */
    boolean hasLogback();

    /** Whether the log4j.properties is located. */
    boolean hasLog4j();

    /** The existing ConfigMap containing custom Hadoop configuration. */
    Optional<String> getExistingHadoopConfigurationConfigMap();

    /** The local directory to locate the custom Hadoop configuration. */
    Optional<String> getLocalHadoopConfigurationDirectory();

    /**
     * A collection of secret and path pairs that are mounted to the JobManager and TaskManager
     * container(s).
     */
    Map<String, String> getSecretNamesToMountPaths();

    /**
     * A collection of customized environments that are attached to the JobManager and TaskManager
     * container(s).
     */
    List<Map<String, String>> getEnvironmentsFromSecrets();
}
