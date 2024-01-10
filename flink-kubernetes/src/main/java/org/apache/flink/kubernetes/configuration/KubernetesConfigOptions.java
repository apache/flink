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

package org.apache.flink.kubernetes.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.kubernetes.kubeclient.services.ClusterIPService;
import org.apache.flink.kubernetes.kubeclient.services.HeadlessClusterIPService;
import org.apache.flink.kubernetes.kubeclient.services.LoadBalancerService;
import org.apache.flink.kubernetes.kubeclient.services.NodePortService;
import org.apache.flink.kubernetes.kubeclient.services.ServiceType;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.util.EnvironmentInformation;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** This class holds configuration constants used by Flink's kubernetes runners. */
@PublicEvolving
public class KubernetesConfigOptions {

    private static final String KUBERNETES_SERVICE_ACCOUNT_KEY = "kubernetes.service-account";
    private static final String KUBERNETES_POD_TEMPLATE_FILE_KEY = "kubernetes.pod-template-file";

    public static final ConfigOption<String> CONTEXT =
            key("kubernetes.context")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The desired context from your Kubernetes config file used to configure the Kubernetes client "
                                    + "for interacting with the cluster. This could be helpful if one has multiple contexts configured and "
                                    + "wants to administrate different Flink clusters on different Kubernetes clusters/contexts.");

    public static final ConfigOption<ServiceExposedType> REST_SERVICE_EXPOSED_TYPE =
            key("kubernetes.rest-service.exposed.type")
                    .enumType(ServiceExposedType.class)
                    .defaultValue(ServiceExposedType.ClusterIP)
                    .withDescription(
                            "The exposed type of the rest service. "
                                    + "The exposed rest service could be used to access the Flink’s Web UI and REST endpoint.");

    public static final ConfigOption<NodePortAddressType>
            REST_SERVICE_EXPOSED_NODE_PORT_ADDRESS_TYPE =
                    key("kubernetes.rest-service.exposed.node-port-address-type")
                            .enumType(NodePortAddressType.class)
                            .defaultValue(NodePortAddressType.InternalIP)
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "The user-specified %s that is used for filtering node IPs when constructing a %s connection string. This option is only considered when '%s' is set to '%s'.",
                                                    link(
                                                            "https://kubernetes.io/docs/concepts/architecture/nodes/#addresses",
                                                            "address type"),
                                                    link(
                                                            "https://kubernetes.io/docs/concepts/services-networking/service/#nodeport",
                                                            "node port"),
                                                    text(REST_SERVICE_EXPOSED_TYPE.key()),
                                                    text(ServiceExposedType.NodePort.name()))
                                            .build());

    public static final ConfigOption<String> JOB_MANAGER_SERVICE_ACCOUNT =
            key("kubernetes.jobmanager.service-account")
                    .stringType()
                    .defaultValue("default")
                    .withFallbackKeys(KUBERNETES_SERVICE_ACCOUNT_KEY)
                    .withDescription(
                            "Service account that is used by jobmanager within kubernetes cluster. "
                                    + "The job manager uses this service account when requesting taskmanager pods from the API server. "
                                    + "If not explicitly configured, config option '"
                                    + KUBERNETES_SERVICE_ACCOUNT_KEY
                                    + "' will be used.");

    public static final ConfigOption<String> TASK_MANAGER_SERVICE_ACCOUNT =
            key("kubernetes.taskmanager.service-account")
                    .stringType()
                    .defaultValue("default")
                    .withFallbackKeys(KUBERNETES_SERVICE_ACCOUNT_KEY)
                    .withDescription(
                            "Service account that is used by taskmanager within kubernetes cluster. "
                                    + "The task manager uses this service account when watching config maps on the API server to retrieve "
                                    + "leader address of jobmanager and resourcemanager. If not explicitly configured, config option '"
                                    + KUBERNETES_SERVICE_ACCOUNT_KEY
                                    + "' will be used.");

    public static final ConfigOption<String> KUBERNETES_SERVICE_ACCOUNT =
            key(KUBERNETES_SERVICE_ACCOUNT_KEY)
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Service account that is used by jobmanager and taskmanager within kubernetes cluster. "
                                    + "Notice that this can be overwritten by config options '"
                                    + JOB_MANAGER_SERVICE_ACCOUNT.key()
                                    + "' and '"
                                    + TASK_MANAGER_SERVICE_ACCOUNT.key()
                                    + "' for jobmanager and taskmanager respectively.");

    public static final ConfigOption<List<Map<String, String>>> JOB_MANAGER_OWNER_REFERENCE =
            key("kubernetes.jobmanager.owner.reference")
                    .mapType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The user-specified %s to be set to the JobManager Deployment. "
                                                    + "When all the owner resources are deleted, the JobManager Deployment "
                                                    + "will be deleted automatically, which also deletes all the resources "
                                                    + "created by this Flink cluster. The value should be formatted as a "
                                                    + "semicolon-separated list of owner references, where each owner "
                                                    + "reference is a comma-separated list of `key:value` pairs. E.g., "
                                                    + "apiVersion:v1,blockOwnerDeletion:true,controller:true,kind:FlinkApplication,name:flink-app-name,uid:flink-app-uid;"
                                                    + "apiVersion:v1,kind:Deployment,name:deploy-name,uid:deploy-uid",
                                            link(
                                                    "https://nightlies.apache.org/flink/flink-docs-master/deployment/resource-providers/native_kubernetes.html#manual-resource-cleanup",
                                                    "Owner References"))
                                    .build());
    public static final ConfigOption<Double> JOB_MANAGER_CPU =
            key("kubernetes.jobmanager.cpu.amount")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDeprecatedKeys("kubernetes.jobmanager.cpu")
                    .withDescription("The number of cpu used by job manager");

    public static final ConfigOption<Double> JOB_MANAGER_CPU_LIMIT_FACTOR =
            key("kubernetes.jobmanager.cpu.limit-factor")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "The limit factor of cpu used by job manager. "
                                    + "The resources limit cpu will be set to cpu * limit-factor.");

    public static final ConfigOption<Double> JOB_MANAGER_MEMORY_LIMIT_FACTOR =
            key("kubernetes.jobmanager.memory.limit-factor")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "The limit factor of memory used by job manager. "
                                    + "The resources limit memory will be set to memory * limit-factor.");

    public static final ConfigOption<Double> TASK_MANAGER_CPU =
            key("kubernetes.taskmanager.cpu.amount")
                    .doubleType()
                    .defaultValue(-1.0)
                    .withDeprecatedKeys("kubernetes.taskmanager.cpu")
                    .withDescription(
                            "The number of cpu used by task manager. By default, the cpu is set "
                                    + "to the number of slots per TaskManager");

    public static final ConfigOption<Double> TASK_MANAGER_CPU_LIMIT_FACTOR =
            key("kubernetes.taskmanager.cpu.limit-factor")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "The limit factor of cpu used by task manager. "
                                    + "The resources limit cpu will be set to cpu * limit-factor.");

    public static final ConfigOption<Double> TASK_MANAGER_MEMORY_LIMIT_FACTOR =
            key("kubernetes.taskmanager.memory.limit-factor")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "The limit factor of memory used by task manager. "
                                    + "The resources limit memory will be set to memory * limit-factor.");

    public static final ConfigOption<ImagePullPolicy> CONTAINER_IMAGE_PULL_POLICY =
            key("kubernetes.container.image.pull-policy")
                    .enumType(ImagePullPolicy.class)
                    .defaultValue(ImagePullPolicy.IfNotPresent)
                    .withDescription(
                            "The Kubernetes container image pull policy. "
                                    + "The default policy is IfNotPresent to avoid putting pressure to image repository.");

    public static final ConfigOption<List<String>> CONTAINER_IMAGE_PULL_SECRETS =
            key("kubernetes.container.image.pull-secrets")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated list of the Kubernetes secrets used to access "
                                    + "private image registries.");

    public static final ConfigOption<String> KUBE_CONFIG_FILE =
            key("kubernetes.config.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The kubernetes config file will be used to create the client. The default "
                                    + "is located at ~/.kube/config");

    public static final ConfigOption<String> NAMESPACE =
            key("kubernetes.namespace")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "The namespace that will be used for running the jobmanager and taskmanager pods.");

    public static final ConfigOption<Map<String, String>> JOB_MANAGER_LABELS =
            key("kubernetes.jobmanager.labels")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The labels to be set for JobManager pod. Specified as key:value pairs separated by commas. "
                                    + "For example, version:alphav1,deploy:test.");

    public static final ConfigOption<Map<String, String>> TASK_MANAGER_LABELS =
            key("kubernetes.taskmanager.labels")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The labels to be set for TaskManager pods. Specified as key:value pairs separated by commas. "
                                    + "For example, version:alphav1,deploy:test.");

    public static final ConfigOption<Map<String, String>> JOB_MANAGER_NODE_SELECTOR =
            key("kubernetes.jobmanager.node-selector")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The node selector to be set for JobManager pod. Specified as key:value pairs separated by "
                                    + "commas. For example, environment:production,disk:ssd.");

    public static final ConfigOption<Map<String, String>> TASK_MANAGER_NODE_SELECTOR =
            key("kubernetes.taskmanager.node-selector")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The node selector to be set for TaskManager pods. Specified as key:value pairs separated by "
                                    + "commas. For example, environment:production,disk:ssd.");

    public static final ConfigOption<String> CLUSTER_ID =
            key("kubernetes.cluster-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The cluster-id, which should be no more than 45 characters, is used for identifying a unique Flink cluster. "
                                                    + "The id must only contain lowercase alphanumeric characters and \"-\". "
                                                    + "The required format is %s. "
                                                    + "If not set, the client will automatically generate it with a random ID.",
                                            code("[a-z]([-a-z0-9]*[a-z0-9])"))
                                    .build());

    @Documentation.OverrideDefault(
            "The default value depends on the actually running version. In general it looks like \"flink:<FLINK_VERSION>-scala_<SCALA_VERSION>\"")
    public static final ConfigOption<String> CONTAINER_IMAGE =
            key("kubernetes.container.image.ref")
                    .stringType()
                    .defaultValue(getDefaultFlinkImage())
                    .withDeprecatedKeys("kubernetes.container.image")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Image to use for Flink containers. "
                                                    + "The specified image must be based upon the same Apache Flink and Scala versions as used by the application. "
                                                    + "Visit %s for the official docker images provided by the Flink project. The Flink project also publishes docker images to %s.",
                                            link("https://hub.docker.com/_/flink?tab=tags", "here"),
                                            link(
                                                    "https://hub.docker.com/r/apache/flink/tags",
                                                    "apache/flink DockerHub repository"))
                                    .build());

    /** The following config options need to be set according to the image. */
    public static final ConfigOption<String> KUBERNETES_ENTRY_PATH =
            key("kubernetes.entry.path")
                    .stringType()
                    .defaultValue("/docker-entrypoint.sh")
                    .withDescription(
                            "The entrypoint script of kubernetes in the image. It will be used as command for jobmanager "
                                    + "and taskmanager container.");

    public static final ConfigOption<String> FLINK_CONF_DIR =
            key("kubernetes.flink.conf.dir")
                    .stringType()
                    .defaultValue("/opt/flink/conf")
                    .withDescription(
                            "The flink conf directory that will be mounted in pod. The flink-conf.yaml, log4j.properties, "
                                    + "logback.xml in this path will be overwritten from config map.");

    public static final ConfigOption<String> FLINK_LOG_DIR =
            key("kubernetes.flink.log.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The directory that logs of jobmanager and taskmanager be saved in the pod. "
                                    + "The default value is $FLINK_HOME/log.");

    public static final ConfigOption<String> HADOOP_CONF_CONFIG_MAP =
            key("kubernetes.hadoop.conf.config-map.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the name of an existing ConfigMap that contains custom Hadoop configuration "
                                    + "to be mounted on the JobManager(s) and TaskManagers.");

    public static final ConfigOption<Map<String, String>> JOB_MANAGER_ANNOTATIONS =
            key("kubernetes.jobmanager.annotations")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The user-specified annotations that are set to the JobManager pod. The value could be "
                                    + "in the form of a1:v1,a2:v2");

    public static final ConfigOption<Map<String, String>> TASK_MANAGER_ANNOTATIONS =
            key("kubernetes.taskmanager.annotations")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The user-specified annotations that are set to the TaskManager pod. The value could be "
                                    + "in the form of a1:v1,a2:v2");

    public static final ConfigOption<String> KUBERNETES_JOBMANAGER_ENTRYPOINT_ARGS =
            key("kubernetes.jobmanager.entrypoint.args")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Extra arguments used when starting the job manager.");

    public static final ConfigOption<String> KUBERNETES_TASKMANAGER_ENTRYPOINT_ARGS =
            key("kubernetes.taskmanager.entrypoint.args")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Extra arguments used when starting the task manager.");

    public static final ConfigOption<List<Map<String, String>>> JOB_MANAGER_TOLERATIONS =
            key("kubernetes.jobmanager.tolerations")
                    .mapType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The user-specified tolerations to be set to the JobManager pod. The value should be "
                                    + "in the form of key:key1,operator:Equal,value:value1,effect:NoSchedule;"
                                    + "key:key2,operator:Exists,effect:NoExecute,tolerationSeconds:6000");

    public static final ConfigOption<List<Map<String, String>>> TASK_MANAGER_TOLERATIONS =
            key("kubernetes.taskmanager.tolerations")
                    .mapType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The user-specified tolerations to be set to the TaskManager pod. The value should be "
                                    + "in the form of key:key1,operator:Equal,value:value1,effect:NoSchedule;"
                                    + "key:key2,operator:Exists,effect:NoExecute,tolerationSeconds:6000");

    public static final ConfigOption<Map<String, String>> REST_SERVICE_ANNOTATIONS =
            key("kubernetes.rest-service.annotations")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The user-specified annotations that are set to the rest Service. The value should be "
                                    + "in the form of a1:v1,a2:v2");

    /**
     * Defines the configuration key of that external resource in Kubernetes. This is used as a
     * suffix in an actual config.
     */
    public static final String EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX =
            "kubernetes.config-key";

    public static final ConfigOption<Map<String, String>> KUBERNETES_SECRETS =
            key("kubernetes.secrets")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The user-specified secrets that will be mounted into Flink container. The value should be in "
                                                    + "the form of %s.",
                                            code("foo:/opt/secrets-foo,bar:/opt/secrets-bar"))
                                    .build());

    public static final ConfigOption<List<Map<String, String>>> KUBERNETES_ENV_SECRET_KEY_REF =
            key("kubernetes.env.secretKeyRef")
                    .mapType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The user-specified secrets to set env variables in Flink container. The value should be in "
                                                    + "the form of %s.",
                                            code(
                                                    "env:FOO_ENV,secret:foo_secret,key:foo_key;env:BAR_ENV,secret:bar_secret,key:bar_key"))
                                    .build());

    /**
     * If configured, Flink will add "resources.limits.&gt;config-key&lt;" and
     * "resources.requests.&gt;config-key&lt;" to the main container of TaskExecutor and set the
     * value to {@link ExternalResourceOptions#EXTERNAL_RESOURCE_AMOUNT}.
     *
     * <p>It is intentionally included into user docs while unused.
     */
    @SuppressWarnings("unused")
    public static final ConfigOption<String> EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY =
            key(ExternalResourceOptions.genericKeyWithSuffix(
                            EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "If configured, Flink will add \"resources.limits.<config-key>\" and \"resources.requests.<config-key>\" "
                                    + "to the main container of TaskExecutor and set the value to the value of "
                                    + ExternalResourceOptions.EXTERNAL_RESOURCE_AMOUNT.key()
                                    + ".");

    public static final ConfigOption<Integer> KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES =
            key("kubernetes.transactional-operation.max-retries")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines the number of Kubernetes transactional operation retries before the "
                                                    + "client gives up. For example, %s.",
                                            code("FlinkKubeClient#checkAndUpdateConfigMap"))
                                    .build());

    public static final ConfigOption<String> JOB_MANAGER_POD_TEMPLATE;

    public static final ConfigOption<String> TASK_MANAGER_POD_TEMPLATE;

    /**
     * This option is here only for documentation generation, it is the fallback key of
     * JOB_MANAGER_POD_TEMPLATE and TASK_MANAGER_POD_TEMPLATE.
     */
    @SuppressWarnings("unused")
    public static final ConfigOption<String> KUBERNETES_POD_TEMPLATE;

    public static final ConfigOption<Integer> KUBERNETES_CLIENT_IO_EXECUTOR_POOL_SIZE =
            ConfigOptions.key("kubernetes.client.io-pool.size")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The size of the IO executor pool used by the Kubernetes client to execute blocking IO operations "
                                    + "(e.g. start/stop TaskManager pods, update leader related ConfigMaps, etc.). "
                                    + "Increasing the pool size allows to run more IO operations concurrently.");

    public static final ConfigOption<Integer> KUBERNETES_JOBMANAGER_REPLICAS =
            key("kubernetes.jobmanager.replicas")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Specify how many JobManager pods will be started simultaneously. "
                                    + "Configure the value to greater than 1 to start standby JobManagers. "
                                    + "It will help to achieve faster recovery. "
                                    + "Notice that high availability should be enabled when starting standby JobManagers.");

    public static final ConfigOption<Boolean> KUBERNETES_HOSTNETWORK_ENABLED =
            key("kubernetes.hostnetwork.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable HostNetwork mode. "
                                    + "The HostNetwork allows the pod could use the node network namespace instead of the individual pod network namespace. Please note that the JobManager service account should have the permission to update Kubernetes service.");

    public static final ConfigOption<String> KUBERNETES_CLIENT_USER_AGENT =
            key("kubernetes.client.user-agent")
                    .stringType()
                    .defaultValue("flink")
                    .withDescription(
                            "The user agent to be used for contacting with Kubernetes APIServer.");

    public static final ConfigOption<Boolean> KUBERNETES_HADOOP_CONF_MOUNT_DECORATOR_ENABLED =
            key("kubernetes.decorator.hadoop-conf-mount.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable Hadoop configuration mount decorator. This "
                                    + "must be set to false when Hadoop config is mounted outside of "
                                    + "Flink. A typical use-case is when one uses Flink Kubernetes "
                                    + "Operator.");

    public static final ConfigOption<Boolean> KUBERNETES_KERBEROS_MOUNT_DECORATOR_ENABLED =
            key("kubernetes.decorator.kerberos-mount.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable Kerberos mount decorator. This must be set "
                                    + "to false when Kerberos config and keytab is mounted outside of "
                                    + "Flink. A typical use-case is when one uses Flink Kubernetes "
                                    + "Operator.");

    /**
     * This will only be used to support blocklist mechanism, which is experimental currently, so we
     * do not want to expose this option in the documentation.
     */
    @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<String> KUBERNETES_NODE_NAME_LABEL =
            key("kubernetes.node-name-label")
                    .stringType()
                    .defaultValue("kubernetes.io/hostname")
                    .withDescription(
                            "The node label whose value is the same as the node name. "
                                    + "Currently, this will only be used to set the node affinity of TM pods to avoid being scheduled on blocked nodes.");

    private static String getDefaultFlinkImage() {
        // The default container image that ties to the exact needed versions of both Flink and
        // Scala.
        boolean snapshot =
                EnvironmentInformation.getVersion()
                        .toLowerCase(Locale.ENGLISH)
                        .contains("snapshot");
        String tag =
                snapshot
                        ? "latest"
                        : EnvironmentInformation.getVersion()
                                + "-scala_"
                                + EnvironmentInformation.getScalaVersion();
        return "apache/flink:" + tag;
    }

    /** The flink rest service exposed type. */
    public enum ServiceExposedType {
        ClusterIP(ClusterIPService.INSTANCE),
        NodePort(NodePortService.INSTANCE),
        LoadBalancer(LoadBalancerService.INSTANCE),
        Headless_ClusterIP(HeadlessClusterIPService.INSTANCE);

        private final ServiceType serviceType;

        ServiceExposedType(ServiceType serviceType) {
            this.serviceType = serviceType;
        }

        public ServiceType serviceType() {
            return serviceType;
        }

        /** Check whether it is ClusterIP type. */
        public boolean isClusterIP() {
            return this == ClusterIP || this == Headless_ClusterIP;
        }
    }

    /** The flink rest service exposed type. */
    public enum NodePortAddressType {
        InternalIP,
        ExternalIP,
    }

    /** The container image pull policy. */
    public enum ImagePullPolicy {
        IfNotPresent,
        Always,
        Never
    }

    static {
        final ConfigOption<String> defaultPodTemplate =
                key(KUBERNETES_POD_TEMPLATE_FILE_KEY + ".default")
                        .stringType()
                        .noDefaultValue()
                        .withDeprecatedKeys(KUBERNETES_POD_TEMPLATE_FILE_KEY);

        JOB_MANAGER_POD_TEMPLATE =
                key(KUBERNETES_POD_TEMPLATE_FILE_KEY + ".jobmanager")
                        .stringType()
                        .noDefaultValue()
                        .withFallbackKeys(defaultPodTemplate.key())
                        .withFallbackKeys(getDeprecatedKeys(defaultPodTemplate))
                        .withDescription(
                                "Specify a local file that contains the jobmanager pod template definition. "
                                        + "It will be used to initialize the jobmanager pod. "
                                        + "The main container should be defined with name '"
                                        + Constants.MAIN_CONTAINER_NAME
                                        + "'. If not explicitly configured, config option '"
                                        + defaultPodTemplate.key()
                                        + "' will be used.");

        TASK_MANAGER_POD_TEMPLATE =
                key(KUBERNETES_POD_TEMPLATE_FILE_KEY + ".taskmanager")
                        .stringType()
                        .noDefaultValue()
                        .withFallbackKeys(defaultPodTemplate.key())
                        .withFallbackKeys(getDeprecatedKeys(defaultPodTemplate))
                        .withDescription(
                                "Specify a local file that contains the taskmanager pod template definition. "
                                        + "It will be used to initialize the taskmanager pod. "
                                        + "The main container should be defined with name '"
                                        + Constants.MAIN_CONTAINER_NAME
                                        + "'. If not explicitly configured, config option '"
                                        + defaultPodTemplate.key()
                                        + "' will be used.");

        KUBERNETES_POD_TEMPLATE =
                defaultPodTemplate.withDescription(
                        "Specify a local file that contains the pod template definition. "
                                + "It will be used to initialize the jobmanager and taskmanager pod. "
                                + "The main container should be defined with name '"
                                + Constants.MAIN_CONTAINER_NAME
                                + "'. Notice that this can be overwritten by config options '"
                                + JOB_MANAGER_POD_TEMPLATE.key()
                                + "' and '"
                                + TASK_MANAGER_POD_TEMPLATE.key()
                                + "' for jobmanager and taskmanager respectively.");
    }

    private static String[] getDeprecatedKeys(ConfigOption<?> from) {
        return StreamSupport.stream(from.fallbackKeys().spliterator(), false)
                .filter(FallbackKey::isDeprecated)
                .map(FallbackKey::getKey)
                .toArray(String[]::new);
    }

    /** This class is not meant to be instantiated. */
    private KubernetesConfigOptions() {}
}
