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

package org.apache.flink.yarn;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.plugin.PluginConfig;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.runtime.security.token.DefaultDelegationTokenManager;
import org.apache.flink.runtime.security.token.DelegationTokenContainer;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.security.token.hadoop.HadoopDelegationTokenConverter;
import org.apache.flink.runtime.security.token.hadoop.KerberosLoginProvider;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;
import org.apache.flink.yarn.entrypoint.YarnApplicationClusterEntryPoint;
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.client.deployment.application.ApplicationConfiguration.APPLICATION_MAIN_CLASS;
import static org.apache.flink.configuration.ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_OPT_DIR;
import static org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever.JOB_GRAPH_FILE_PATH;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.yarn.Utils.getPathFromLocalFile;
import static org.apache.flink.yarn.Utils.getPathFromLocalFilePathStr;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR;

/** The descriptor with deployment information for deploying a Flink cluster on Yarn. */
public class YarnClusterDescriptor implements ClusterDescriptor<ApplicationId> {
    private static final Logger LOG = LoggerFactory.getLogger(YarnClusterDescriptor.class);

    @VisibleForTesting
    static final String IGNORE_UNRECOGNIZED_VM_OPTIONS = "-XX:+IgnoreUnrecognizedVMOptions";

    private final YarnConfiguration yarnConfiguration;

    private final YarnClient yarnClient;

    private final YarnClusterInformationRetriever yarnClusterInformationRetriever;

    /** True if the descriptor must not shut down the YarnClient. */
    private final boolean sharedYarnClient;

    /**
     * Lazily initialized list of files to ship. The path string for the files which is configured
     * by {@link YarnConfigOptions#SHIP_FILES} will be converted to {@link Path} with schema and
     * absolute path.
     */
    private final List<Path> shipFiles = new LinkedList<>();

    /**
     * Lazily initialized list of archives to ship. The path string for the archives which is
     * configured by {@link YarnConfigOptions#SHIP_ARCHIVES} will be converted to {@link Path} with
     * schema and absolute path.
     */
    private final List<Path> shipArchives = new LinkedList<>();

    private final String yarnQueue;

    private Path flinkJarPath;

    private final Configuration flinkConfiguration;

    private final String customName;

    private final String nodeLabel;

    private final String applicationType;

    private YarnConfigOptions.UserJarInclusion userJarInclusion;

    public YarnClusterDescriptor(
            Configuration flinkConfiguration,
            YarnConfiguration yarnConfiguration,
            YarnClient yarnClient,
            YarnClusterInformationRetriever yarnClusterInformationRetriever,
            boolean sharedYarnClient) {

        this.yarnConfiguration = Preconditions.checkNotNull(yarnConfiguration);
        this.yarnClient = Preconditions.checkNotNull(yarnClient);
        this.yarnClusterInformationRetriever =
                Preconditions.checkNotNull(yarnClusterInformationRetriever);
        this.sharedYarnClient = sharedYarnClient;

        this.flinkConfiguration = Preconditions.checkNotNull(flinkConfiguration);
        this.userJarInclusion = getUserJarInclusionMode(flinkConfiguration);

        getLocalFlinkDistPath(flinkConfiguration).ifPresent(this::setLocalJarPath);
        decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_FILES)
                .ifPresent(this::addShipFiles);
        decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_ARCHIVES)
                .ifPresent(this::addShipArchives);

        this.yarnQueue = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_QUEUE);
        this.customName = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_NAME);
        this.applicationType = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_TYPE);
        this.nodeLabel = flinkConfiguration.getString(YarnConfigOptions.NODE_LABEL);
    }

    private Optional<List<Path>> decodeFilesToShipToCluster(
            final Configuration configuration, final ConfigOption<List<String>> configOption) {
        checkNotNull(configuration);
        checkNotNull(configOption);

        List<Path> files =
                ConfigUtils.decodeListFromConfig(
                        configuration, configOption, this::createPathWithSchema);
        return files.isEmpty() ? Optional.empty() : Optional.of(files);
    }

    private Path createPathWithSchema(String path) {
        return isWithoutSchema(new Path(path)) ? getPathFromLocalFilePathStr(path) : new Path(path);
    }

    private boolean isWithoutSchema(Path path) {
        return StringUtils.isNullOrWhitespaceOnly(path.toUri().getScheme());
    }

    private Optional<Path> getLocalFlinkDistPath(final Configuration configuration) {
        final String localJarPath = configuration.getString(YarnConfigOptions.FLINK_DIST_JAR);
        if (localJarPath != null) {
            return Optional.of(new Path(localJarPath));
        }

        LOG.info(
                "No path for the flink jar passed. Using the location of "
                        + getClass()
                        + " to locate the jar");

        // check whether it's actually a jar file --> when testing we execute this class without a
        // flink-dist jar
        final String decodedPath = getDecodedJarPath();
        return decodedPath.endsWith(".jar")
                ? Optional.of(getPathFromLocalFilePathStr(decodedPath))
                : Optional.empty();
    }

    private String getDecodedJarPath() {
        final String encodedJarPath =
                getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        try {
            return URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(
                    "Couldn't decode the encoded Flink dist jar path: "
                            + encodedJarPath
                            + " You can supply a path manually via the command line.");
        }
    }

    @VisibleForTesting
    List<Path> getShipFiles() {
        return shipFiles;
    }

    @VisibleForTesting
    List<Path> getShipArchives() {
        return shipArchives;
    }

    public YarnClient getYarnClient() {
        return yarnClient;
    }

    /**
     * The class to start the application master with. This class runs the main method in case of
     * session cluster.
     */
    protected String getYarnSessionClusterEntrypoint() {
        return YarnSessionClusterEntrypoint.class.getName();
    }

    /**
     * The class to start the application master with. This class runs the main method in case of
     * the job cluster.
     */
    protected String getYarnJobClusterEntrypoint() {
        return YarnJobClusterEntrypoint.class.getName();
    }

    public Configuration getFlinkConfiguration() {
        return flinkConfiguration;
    }

    public void setLocalJarPath(Path localJarPath) {
        if (!localJarPath.toString().endsWith("jar")) {
            throw new IllegalArgumentException(
                    "The passed jar path ('"
                            + localJarPath
                            + "') does not end with the 'jar' extension");
        }
        this.flinkJarPath = localJarPath;
    }

    /**
     * Adds the given files to the list of files to ship.
     *
     * <p>Note that any file matching "<tt>flink-dist*.jar</tt>" will be excluded from the upload by
     * {@link YarnApplicationFileUploader#registerMultipleLocalResources(Collection, String,
     * LocalResourceType)} since we upload the Flink uber jar ourselves and do not need to deploy it
     * multiple times.
     *
     * @param shipFiles files to ship
     */
    public void addShipFiles(List<Path> shipFiles) {
        checkArgument(
                !isUsrLibDirIncludedInShipFiles(shipFiles, yarnConfiguration),
                "User-shipped directories configured via : %s should not include %s.",
                YarnConfigOptions.SHIP_FILES.key(),
                ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);
        this.shipFiles.addAll(shipFiles);
    }

    private void addShipArchives(List<Path> shipArchives) {
        checkArgument(
                isArchiveOnlyIncludedInShipArchiveFiles(shipArchives, yarnConfiguration),
                "Directories or non-archive files are included.");
        this.shipArchives.addAll(shipArchives);
    }

    private static boolean isArchiveOnlyIncludedInShipArchiveFiles(
            List<Path> shipFiles, YarnConfiguration yarnConfiguration) {
        long archivedFileCount =
                shipFiles.stream()
                        .map(
                                FunctionUtils.uncheckedFunction(
                                        path -> getFileStatus(path, yarnConfiguration)))
                        .filter(FileStatus::isFile)
                        .map(status -> status.getPath().getName().toLowerCase())
                        .filter(
                                name ->
                                        name.endsWith(".tar.gz")
                                                || name.endsWith(".tar")
                                                || name.endsWith(".tgz")
                                                || name.endsWith(".dst")
                                                || name.endsWith(".jar")
                                                || name.endsWith(".zip"))
                        .count();
        return archivedFileCount == shipFiles.size();
    }

    private static FileStatus getFileStatus(Path path, YarnConfiguration yarnConfiguration)
            throws IOException {
        return path.getFileSystem(yarnConfiguration).getFileStatus(path);
    }

    private void isReadyForDeployment(ClusterSpecification clusterSpecification) throws Exception {

        if (this.flinkJarPath == null) {
            throw new YarnDeploymentException("The Flink jar path is null");
        }
        if (this.flinkConfiguration == null) {
            throw new YarnDeploymentException("Flink configuration object has not been set");
        }

        // Check if we don't exceed YARN's maximum virtual cores.
        final int numYarnMaxVcores = yarnClusterInformationRetriever.getMaxVcores();

        int configuredAmVcores = flinkConfiguration.getInteger(YarnConfigOptions.APP_MASTER_VCORES);
        if (configuredAmVcores > numYarnMaxVcores) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The number of requested virtual cores for application master %d"
                                    + " exceeds the maximum number of virtual cores %d available in the Yarn Cluster.",
                            configuredAmVcores, numYarnMaxVcores));
        }

        int configuredVcores =
                flinkConfiguration.getInteger(
                        YarnConfigOptions.VCORES, clusterSpecification.getSlotsPerTaskManager());
        // don't configure more than the maximum configured number of vcores
        if (configuredVcores > numYarnMaxVcores) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The number of requested virtual cores per node %d"
                                    + " exceeds the maximum number of virtual cores %d available in the Yarn Cluster."
                                    + " Please note that the number of virtual cores is set to the number of task slots by default"
                                    + " unless configured in the Flink config with '%s.'",
                            configuredVcores, numYarnMaxVcores, YarnConfigOptions.VCORES.key()));
        }

        // check if required Hadoop environment variables are set. If not, warn user
        if (System.getenv("HADOOP_CONF_DIR") == null && System.getenv("YARN_CONF_DIR") == null) {
            LOG.warn(
                    "Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set. "
                            + "The Flink YARN Client needs one of these to be set to properly load the Hadoop "
                            + "configuration for accessing YARN.");
        }
    }

    public String getNodeLabel() {
        return nodeLabel;
    }

    // -------------------------------------------------------------
    // Lifecycle management
    // -------------------------------------------------------------

    @Override
    public void close() {
        if (!sharedYarnClient) {
            yarnClient.stop();
        }
    }

    // -------------------------------------------------------------
    // ClusterClient overrides
    // -------------------------------------------------------------

    @Override
    public ClusterClientProvider<ApplicationId> retrieve(ApplicationId applicationId)
            throws ClusterRetrieveException {

        try {
            // check if required Hadoop environment variables are set. If not, warn user
            if (System.getenv("HADOOP_CONF_DIR") == null
                    && System.getenv("YARN_CONF_DIR") == null) {
                LOG.warn(
                        "Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set."
                                + "The Flink YARN Client needs one of these to be set to properly load the Hadoop "
                                + "configuration for accessing YARN.");
            }

            final ApplicationReport report = yarnClient.getApplicationReport(applicationId);

            if (report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
                // Flink cluster is not running anymore
                LOG.error(
                        "The application {} doesn't run anymore. It has previously completed with final status: {}",
                        applicationId,
                        report.getFinalApplicationStatus());
                throw new RuntimeException(
                        "The Yarn application " + applicationId + " doesn't run anymore.");
            }

            setClusterEntrypointInfoToConfig(report);

            return () -> {
                try {
                    return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());
                } catch (Exception e) {
                    throw new RuntimeException("Couldn't retrieve Yarn cluster", e);
                }
            };
        } catch (Exception e) {
            throw new ClusterRetrieveException("Couldn't retrieve Yarn cluster", e);
        }
    }

    @Override
    public ClusterClientProvider<ApplicationId> deploySessionCluster(
            ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
        try {
            return deployInternal(
                    clusterSpecification,
                    "Flink session cluster",
                    getYarnSessionClusterEntrypoint(),
                    null,
                    false);
        } catch (Exception e) {
            throw new ClusterDeploymentException("Couldn't deploy Yarn session cluster", e);
        }
    }

    @Override
    public ClusterClientProvider<ApplicationId> deployApplicationCluster(
            final ClusterSpecification clusterSpecification,
            final ApplicationConfiguration applicationConfiguration)
            throws ClusterDeploymentException {
        checkNotNull(clusterSpecification);
        checkNotNull(applicationConfiguration);

        final YarnDeploymentTarget deploymentTarget =
                YarnDeploymentTarget.fromConfig(flinkConfiguration);
        if (YarnDeploymentTarget.APPLICATION != deploymentTarget) {
            throw new ClusterDeploymentException(
                    "Couldn't deploy Yarn Application Cluster."
                            + " Expected deployment.target="
                            + YarnDeploymentTarget.APPLICATION.getName()
                            + " but actual one was \""
                            + deploymentTarget.getName()
                            + "\"");
        }

        applicationConfiguration.applyToConfiguration(flinkConfiguration);

        // No need to do pipelineJars validation if it is a PyFlink job.
        if (!(PackagedProgramUtils.isPython(applicationConfiguration.getApplicationClassName())
                || PackagedProgramUtils.isPython(applicationConfiguration.getProgramArguments()))) {
            final List<String> pipelineJars =
                    flinkConfiguration
                            .getOptional(PipelineOptions.JARS)
                            .orElse(Collections.emptyList());
            Preconditions.checkArgument(pipelineJars.size() == 1, "Should only have one jar");
        }

        try {
            return deployInternal(
                    clusterSpecification,
                    "Flink Application Cluster",
                    YarnApplicationClusterEntryPoint.class.getName(),
                    null,
                    false);
        } catch (Exception e) {
            throw new ClusterDeploymentException("Couldn't deploy Yarn Application Cluster", e);
        }
    }

    @Override
    public ClusterClientProvider<ApplicationId> deployJobCluster(
            ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached)
            throws ClusterDeploymentException {

        LOG.warn(
                "Job Clusters are deprecated since Flink 1.15. Please use an Application Cluster/Application Mode instead.");
        try {
            return deployInternal(
                    clusterSpecification,
                    "Flink per-job cluster",
                    getYarnJobClusterEntrypoint(),
                    jobGraph,
                    detached);
        } catch (Exception e) {
            throw new ClusterDeploymentException("Could not deploy Yarn job cluster.", e);
        }
    }

    @Override
    public void killCluster(ApplicationId applicationId) throws FlinkException {
        try {
            yarnClient.killApplication(applicationId);

            try (final FileSystem fs = FileSystem.get(yarnConfiguration)) {
                final Path applicationDir =
                        YarnApplicationFileUploader.getApplicationDirPath(
                                getStagingDir(fs), applicationId);

                Utils.deleteApplicationFiles(applicationDir.toUri().toString());
            }

        } catch (YarnException | IOException e) {
            throw new FlinkException(
                    "Could not kill the Yarn Flink cluster with id " + applicationId + '.', e);
        }
    }

    /**
     * This method will block until the ApplicationMaster/JobManager have been deployed on YARN.
     *
     * @param clusterSpecification Initial cluster specification for the Flink cluster to be
     *     deployed
     * @param applicationName name of the Yarn application to start
     * @param yarnClusterEntrypoint Class name of the Yarn cluster entry point.
     * @param jobGraph A job graph which is deployed with the Flink cluster, {@code null} if none
     * @param detached True if the cluster should be started in detached mode
     */
    private ClusterClientProvider<ApplicationId> deployInternal(
            ClusterSpecification clusterSpecification,
            String applicationName,
            String yarnClusterEntrypoint,
            @Nullable JobGraph jobGraph,
            boolean detached)
            throws Exception {

        final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if (HadoopUtils.isKerberosSecurityEnabled(currentUser)) {
            boolean useTicketCache =
                    flinkConfiguration.getBoolean(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);

            if (!HadoopUtils.areKerberosCredentialsValid(currentUser, useTicketCache)) {
                throw new RuntimeException(
                        "Hadoop security with Kerberos is enabled but the login user "
                                + "does not have Kerberos credentials or delegation tokens!");
            }

            final boolean fetchToken =
                    flinkConfiguration.getBoolean(SecurityOptions.KERBEROS_FETCH_DELEGATION_TOKEN);
            final boolean yarnAccessFSEnabled =
                    !CollectionUtil.isNullOrEmpty(
                            flinkConfiguration.get(
                                    SecurityOptions.KERBEROS_HADOOP_FILESYSTEMS_TO_ACCESS));
            if (!fetchToken && yarnAccessFSEnabled) {
                throw new IllegalConfigurationException(
                        String.format(
                                "When %s is disabled, %s must be disabled as well.",
                                SecurityOptions.KERBEROS_FETCH_DELEGATION_TOKEN.key(),
                                SecurityOptions.KERBEROS_HADOOP_FILESYSTEMS_TO_ACCESS.key()));
            }
        }

        isReadyForDeployment(clusterSpecification);

        // ------------------ Check if the specified queue exists --------------------

        checkYarnQueues(yarnClient);

        // ------------------ Check if the YARN ClusterClient has the requested resources
        // --------------

        // Create application via yarnClient
        final YarnClientApplication yarnApplication = yarnClient.createApplication();
        final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();

        Resource maxRes = appResponse.getMaximumResourceCapability();

        final ClusterResourceDescription freeClusterMem;
        try {
            freeClusterMem = getCurrentFreeClusterResources(yarnClient);
        } catch (YarnException | IOException e) {
            failSessionDuringDeployment(yarnClient, yarnApplication);
            throw new YarnDeploymentException(
                    "Could not retrieve information about free cluster resources.", e);
        }

        final int yarnMinAllocationMB =
                yarnConfiguration.getInt(
                        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
                        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
        if (yarnMinAllocationMB <= 0) {
            throw new YarnDeploymentException(
                    "The minimum allocation memory "
                            + "("
                            + yarnMinAllocationMB
                            + " MB) configured via '"
                            + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
                            + "' should be greater than 0.");
        }

        final ClusterSpecification validClusterSpecification;
        try {
            validClusterSpecification =
                    validateClusterResources(
                            clusterSpecification, yarnMinAllocationMB, maxRes, freeClusterMem);
        } catch (YarnDeploymentException yde) {
            failSessionDuringDeployment(yarnClient, yarnApplication);
            throw yde;
        }

        LOG.info("Cluster specification: {}", validClusterSpecification);

        final ClusterEntrypoint.ExecutionMode executionMode =
                detached
                        ? ClusterEntrypoint.ExecutionMode.DETACHED
                        : ClusterEntrypoint.ExecutionMode.NORMAL;

        flinkConfiguration.setString(
                ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, executionMode.toString());

        ApplicationReport report =
                startAppMaster(
                        flinkConfiguration,
                        applicationName,
                        yarnClusterEntrypoint,
                        jobGraph,
                        yarnClient,
                        yarnApplication,
                        validClusterSpecification);

        // print the application id for user to cancel themselves.
        if (detached) {
            final ApplicationId yarnApplicationId = report.getApplicationId();
            logDetachedClusterInformation(yarnApplicationId, LOG);
        }

        setClusterEntrypointInfoToConfig(report);

        return () -> {
            try {
                return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());
            } catch (Exception e) {
                throw new RuntimeException("Error while creating RestClusterClient.", e);
            }
        };
    }

    private ClusterSpecification validateClusterResources(
            ClusterSpecification clusterSpecification,
            int yarnMinAllocationMB,
            Resource maximumResourceCapability,
            ClusterResourceDescription freeClusterResources)
            throws YarnDeploymentException {

        int jobManagerMemoryMb = clusterSpecification.getMasterMemoryMB();
        final int taskManagerMemoryMb = clusterSpecification.getTaskManagerMemoryMB();

        logIfComponentMemNotIntegerMultipleOfYarnMinAllocation(
                "JobManager", jobManagerMemoryMb, yarnMinAllocationMB);
        logIfComponentMemNotIntegerMultipleOfYarnMinAllocation(
                "TaskManager", taskManagerMemoryMb, yarnMinAllocationMB);

        // set the memory to minAllocationMB to do the next checks correctly
        if (jobManagerMemoryMb < yarnMinAllocationMB) {
            jobManagerMemoryMb = yarnMinAllocationMB;
        }

        final String note =
                "Please check the 'yarn.scheduler.maximum-allocation-mb' and the 'yarn.nodemanager.resource.memory-mb' configuration values\n";
        if (jobManagerMemoryMb > maximumResourceCapability.getMemorySize()) {
            throw new YarnDeploymentException(
                    "The cluster does not have the requested resources for the JobManager available!\n"
                            + "Maximum Memory: "
                            + maximumResourceCapability.getMemorySize()
                            + "MB Requested: "
                            + jobManagerMemoryMb
                            + "MB. "
                            + note);
        }

        if (taskManagerMemoryMb > maximumResourceCapability.getMemorySize()) {
            throw new YarnDeploymentException(
                    "The cluster does not have the requested resources for the TaskManagers available!\n"
                            + "Maximum Memory: "
                            + maximumResourceCapability.getMemorySize()
                            + " Requested: "
                            + taskManagerMemoryMb
                            + "MB. "
                            + note);
        }

        final String noteRsc =
                "\nThe Flink YARN client will try to allocate the YARN session, but maybe not all TaskManagers are "
                        + "connecting from the beginning because the resources are currently not available in the cluster. "
                        + "The allocation might take more time than usual because the Flink YARN client needs to wait until "
                        + "the resources become available.";

        if (taskManagerMemoryMb > freeClusterResources.containerLimit) {
            LOG.warn(
                    "The requested amount of memory for the TaskManagers ("
                            + taskManagerMemoryMb
                            + "MB) is more than "
                            + "the largest possible YARN container: "
                            + freeClusterResources.containerLimit
                            + noteRsc);
        }
        if (jobManagerMemoryMb > freeClusterResources.containerLimit) {
            LOG.warn(
                    "The requested amount of memory for the JobManager ("
                            + jobManagerMemoryMb
                            + "MB) is more than "
                            + "the largest possible YARN container: "
                            + freeClusterResources.containerLimit
                            + noteRsc);
        }

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMb)
                .setTaskManagerMemoryMB(taskManagerMemoryMb)
                .setSlotsPerTaskManager(clusterSpecification.getSlotsPerTaskManager())
                .createClusterSpecification();
    }

    private void logIfComponentMemNotIntegerMultipleOfYarnMinAllocation(
            String componentName, int componentMemoryMB, int yarnMinAllocationMB) {
        int normalizedMemMB =
                (componentMemoryMB + (yarnMinAllocationMB - 1))
                        / yarnMinAllocationMB
                        * yarnMinAllocationMB;
        if (normalizedMemMB <= 0) {
            normalizedMemMB = yarnMinAllocationMB;
        }
        if (componentMemoryMB != normalizedMemMB) {
            LOG.info(
                    "The configured {} memory is {} MB. YARN will allocate {} MB to make up an integer multiple of its "
                            + "minimum allocation memory ({} MB, configured via 'yarn.scheduler.minimum-allocation-mb'). The extra {} MB "
                            + "may not be used by Flink.",
                    componentName,
                    componentMemoryMB,
                    normalizedMemMB,
                    yarnMinAllocationMB,
                    normalizedMemMB - componentMemoryMB);
        }
    }

    private void checkYarnQueues(YarnClient yarnClient) {
        try {
            List<QueueInfo> queues = yarnClient.getAllQueues();
            if (queues.size() > 0
                    && this.yarnQueue
                            != null) { // check only if there are queues configured in yarn and for
                // this session.
                boolean queueFound = false;
                for (QueueInfo queue : queues) {
                    if (queue.getQueueName().equals(this.yarnQueue)
                            || queue.getQueueName().equals("root." + this.yarnQueue)) {
                        queueFound = true;
                        break;
                    }
                }
                if (!queueFound) {
                    String queueNames = StringUtils.toQuotedListString(queues.toArray());
                    LOG.warn(
                            "The specified queue '"
                                    + this.yarnQueue
                                    + "' does not exist. "
                                    + "Available queues: "
                                    + queueNames);
                }
            } else {
                LOG.debug("The YARN cluster does not have any queues configured");
            }
        } catch (Throwable e) {
            LOG.warn("Error while getting queue information from YARN: " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error details", e);
            }
        }
    }

    private ApplicationReport startAppMaster(
            Configuration configuration,
            String applicationName,
            String yarnClusterEntrypoint,
            JobGraph jobGraph,
            YarnClient yarnClient,
            YarnClientApplication yarnApplication,
            ClusterSpecification clusterSpecification)
            throws Exception {

        // ------------------ Initialize the file systems -------------------------

        org.apache.flink.core.fs.FileSystem.initialize(
                configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

        final FileSystem fs = FileSystem.get(yarnConfiguration);

        // hard coded check for the GoogleHDFS client because its not overriding the getScheme()
        // method.
        if (!fs.getClass().getSimpleName().equals("GoogleHadoopFileSystem")
                && fs.getScheme().startsWith("file")) {
            LOG.warn(
                    "The file system scheme is '"
                            + fs.getScheme()
                            + "'. This indicates that the "
                            + "specified Hadoop configuration path is wrong and the system is using the default Hadoop configuration values."
                            + "The Flink YARN client needs to store its files in a distributed file system");
        }

        ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();

        final List<Path> providedLibDirs =
                Utils.getQualifiedRemoteProvidedLibDirs(configuration, yarnConfiguration);

        final Optional<Path> providedUsrLibDir =
                Utils.getQualifiedRemoteProvidedUsrLib(configuration, yarnConfiguration);

        Path stagingDirPath = getStagingDir(fs);
        FileSystem stagingDirFs = stagingDirPath.getFileSystem(yarnConfiguration);
        final YarnApplicationFileUploader fileUploader =
                YarnApplicationFileUploader.from(
                        stagingDirFs,
                        stagingDirPath,
                        providedLibDirs,
                        appContext.getApplicationId(),
                        getFileReplication());

        // The files need to be shipped and added to classpath.
        Set<Path> systemShipFiles = new HashSet<>(shipFiles);

        final String logConfigFilePath =
                configuration.getString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE);
        if (logConfigFilePath != null) {
            systemShipFiles.add(getPathFromLocalFilePathStr(logConfigFilePath));
        }

        // Set-up ApplicationSubmissionContext for the application

        final ApplicationId appId = appContext.getApplicationId();

        // ------------------ Add Zookeeper namespace to local flinkConfiguration ------
        setHAClusterIdIfNotSet(configuration, appId);

        if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
            // activate re-execution of failed applications
            appContext.setMaxAppAttempts(
                    configuration.getInteger(
                            YarnConfigOptions.APPLICATION_ATTEMPTS.key(),
                            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS));

            activateHighAvailabilitySupport(appContext);
        } else {
            // set number of application retries to 1 in the default case
            appContext.setMaxAppAttempts(
                    configuration.getInteger(YarnConfigOptions.APPLICATION_ATTEMPTS.key(), 1));
        }

        final Set<Path> userJarFiles = new HashSet<>();
        if (jobGraph != null) {
            userJarFiles.addAll(
                    jobGraph.getUserJars().stream()
                            .map(f -> f.toUri())
                            .map(Path::new)
                            .collect(Collectors.toSet()));
        }

        final List<URI> jarUrls =
                ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URI::create);
        if (jarUrls != null
                && YarnApplicationClusterEntryPoint.class.getName().equals(yarnClusterEntrypoint)) {
            userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet()));
        }

        // only for per job mode
        if (jobGraph != null) {
            for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                    jobGraph.getUserArtifacts().entrySet()) {
                // only upload local files
                if (!Utils.isRemotePath(entry.getValue().filePath)) {
                    Path localPath = new Path(entry.getValue().filePath);
                    Tuple2<Path, Long> remoteFileInfo =
                            fileUploader.uploadLocalFileToRemote(localPath, entry.getKey());
                    jobGraph.setUserArtifactRemotePath(
                            entry.getKey(), remoteFileInfo.f0.toString());
                }
            }

            jobGraph.writeUserArtifactEntriesToConfiguration();
        }

        if (providedLibDirs == null || providedLibDirs.isEmpty()) {
            addLibFoldersToShipFiles(systemShipFiles);
        }

        // Register all files in provided lib dirs as local resources with public visibility
        // and upload the remaining dependencies as local resources with APPLICATION visibility.
        final List<String> systemClassPaths = fileUploader.registerProvidedLocalResources();
        final List<String> uploadedDependencies =
                fileUploader.registerMultipleLocalResources(
                        systemShipFiles, Path.CUR_DIR, LocalResourceType.FILE);
        systemClassPaths.addAll(uploadedDependencies);

        // upload and register ship-only files
        // Plugin files only need to be shipped and should not be added to classpath.
        if (providedLibDirs == null || providedLibDirs.isEmpty()) {
            Set<Path> shipOnlyFiles = new HashSet<>();
            addPluginsFoldersToShipFiles(shipOnlyFiles);
            fileUploader.registerMultipleLocalResources(
                    shipOnlyFiles, Path.CUR_DIR, LocalResourceType.FILE);
        }

        if (!shipArchives.isEmpty()) {
            fileUploader.registerMultipleLocalResources(
                    shipArchives, Path.CUR_DIR, LocalResourceType.ARCHIVE);
        }

        // only for application mode
        // Python jar file only needs to be shipped and should not be added to classpath.
        if (YarnApplicationClusterEntryPoint.class.getName().equals(yarnClusterEntrypoint)
                && PackagedProgramUtils.isPython(configuration.get(APPLICATION_MAIN_CLASS))) {
            fileUploader.registerMultipleLocalResources(
                    Collections.singletonList(
                            new Path(PackagedProgramUtils.getPythonJar().toURI())),
                    ConfigConstants.DEFAULT_FLINK_OPT_DIR,
                    LocalResourceType.FILE);
        }

        // Upload and register user jars
        final List<String> userClassPaths =
                fileUploader.registerMultipleLocalResources(
                        userJarFiles,
                        userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED
                                ? ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR
                                : Path.CUR_DIR,
                        LocalResourceType.FILE);

        // usrlib in remote will be used first.
        if (providedUsrLibDir.isPresent()) {
            final List<String> usrLibClassPaths =
                    fileUploader.registerMultipleLocalResources(
                            Collections.singletonList(providedUsrLibDir.get()),
                            Path.CUR_DIR,
                            LocalResourceType.FILE);
            userClassPaths.addAll(usrLibClassPaths);
        } else if (ClusterEntrypointUtils.tryFindUserLibDirectory().isPresent()) {
            // local usrlib will be automatically shipped if it exists and there is no remote
            // usrlib.
            final Set<File> usrLibShipFiles = new HashSet<>();
            addUsrLibFolderToShipFiles(usrLibShipFiles);
            final List<String> usrLibClassPaths =
                    fileUploader.registerMultipleLocalResources(
                            usrLibShipFiles.stream()
                                    .map(e -> new Path(e.toURI()))
                                    .collect(Collectors.toSet()),
                            Path.CUR_DIR,
                            LocalResourceType.FILE);
            userClassPaths.addAll(usrLibClassPaths);
        }

        if (userJarInclusion == YarnConfigOptions.UserJarInclusion.ORDER) {
            systemClassPaths.addAll(userClassPaths);
        }

        // normalize classpath by sorting
        Collections.sort(systemClassPaths);
        Collections.sort(userClassPaths);

        // classpath assembler
        StringBuilder classPathBuilder = new StringBuilder();
        if (userJarInclusion == YarnConfigOptions.UserJarInclusion.FIRST) {
            for (String userClassPath : userClassPaths) {
                classPathBuilder.append(userClassPath).append(File.pathSeparator);
            }
        }
        for (String classPath : systemClassPaths) {
            classPathBuilder.append(classPath).append(File.pathSeparator);
        }

        // Setup jar for ApplicationMaster
        final YarnLocalResourceDescriptor localResourceDescFlinkJar =
                fileUploader.uploadFlinkDist(flinkJarPath);
        classPathBuilder
                .append(localResourceDescFlinkJar.getResourceKey())
                .append(File.pathSeparator);

        // write job graph to tmp file and add it to local resource
        // TODO: server use user main method to generate job graph
        if (jobGraph != null) {
            File tmpJobGraphFile = null;
            try {
                tmpJobGraphFile = File.createTempFile(appId.toString(), null);
                try (FileOutputStream output = new FileOutputStream(tmpJobGraphFile);
                        ObjectOutputStream obOutput = new ObjectOutputStream(output)) {
                    obOutput.writeObject(jobGraph);
                }

                final String jobGraphFilename = "job.graph";
                configuration.setString(JOB_GRAPH_FILE_PATH, jobGraphFilename);

                fileUploader.registerSingleLocalResource(
                        jobGraphFilename,
                        new Path(tmpJobGraphFile.toURI()),
                        "",
                        LocalResourceType.FILE,
                        true,
                        false);
                classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
            } catch (Exception e) {
                LOG.warn("Add job graph to local resource fail.");
                throw e;
            } finally {
                if (tmpJobGraphFile != null && !tmpJobGraphFile.delete()) {
                    LOG.warn("Fail to delete temporary file {}.", tmpJobGraphFile.toPath());
                }
            }
        }

        // Upload the flink configuration
        // write out configuration file
        File tmpConfigurationFile = null;
        try {
            tmpConfigurationFile = File.createTempFile(appId + "-flink-conf.yaml", null);

            // remove localhost bind hosts as they render production clusters unusable
            removeLocalhostBindHostSetting(configuration, JobManagerOptions.BIND_HOST);
            removeLocalhostBindHostSetting(configuration, TaskManagerOptions.BIND_HOST);
            // this setting is unconditionally overridden anyway, so we remove it for clarity
            configuration.removeConfig(TaskManagerOptions.HOST);

            BootstrapTools.writeConfiguration(configuration, tmpConfigurationFile);

            String flinkConfigKey = "flink-conf.yaml";
            fileUploader.registerSingleLocalResource(
                    flinkConfigKey,
                    new Path(tmpConfigurationFile.getAbsolutePath()),
                    "",
                    LocalResourceType.FILE,
                    true,
                    true);
            classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
        } finally {
            if (tmpConfigurationFile != null && !tmpConfigurationFile.delete()) {
                LOG.warn("Fail to delete temporary file {}.", tmpConfigurationFile.toPath());
            }
        }

        if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) {
            for (String userClassPath : userClassPaths) {
                classPathBuilder.append(userClassPath).append(File.pathSeparator);
            }
        }

        // To support Yarn Secure Integration Test Scenario
        // In Integration test setup, the Yarn containers created by YarnMiniCluster does not have
        // the Yarn site XML
        // and KRB5 configuration files. We are adding these files as container local resources for
        // the container
        // applications (JM/TMs) to have proper secure cluster setup
        Path remoteYarnSiteXmlPath = null;
        if (System.getenv("IN_TESTS") != null) {
            File f = new File(System.getenv("YARN_CONF_DIR"), Utils.YARN_SITE_FILE_NAME);
            LOG.info(
                    "Adding Yarn configuration {} to the AM container local resource bucket",
                    f.getAbsolutePath());
            Path yarnSitePath = new Path(f.getAbsolutePath());
            remoteYarnSiteXmlPath =
                    fileUploader
                            .registerSingleLocalResource(
                                    Utils.YARN_SITE_FILE_NAME,
                                    yarnSitePath,
                                    "",
                                    LocalResourceType.FILE,
                                    false,
                                    false)
                            .getPath();
            if (System.getProperty("java.security.krb5.conf") != null) {
                configuration.set(
                        SecurityOptions.KERBEROS_KRB5_PATH,
                        System.getProperty("java.security.krb5.conf"));
            }
        }

        Path remoteKrb5Path = null;
        boolean hasKrb5 = false;
        String krb5Config = configuration.get(SecurityOptions.KERBEROS_KRB5_PATH);
        if (!StringUtils.isNullOrWhitespaceOnly(krb5Config)) {
            final File krb5 = new File(krb5Config);
            LOG.info(
                    "Adding KRB5 configuration {} to the AM container local resource bucket",
                    krb5.getAbsolutePath());
            final Path krb5ConfPath = new Path(krb5.getAbsolutePath());
            remoteKrb5Path =
                    fileUploader
                            .registerSingleLocalResource(
                                    Utils.KRB5_FILE_NAME,
                                    krb5ConfPath,
                                    "",
                                    LocalResourceType.FILE,
                                    false,
                                    false)
                            .getPath();
            hasKrb5 = true;
        }

        Path remotePathKeytab = null;
        String localizedKeytabPath = null;
        String keytab = configuration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB);
        if (keytab != null) {
            boolean localizeKeytab =
                    flinkConfiguration.getBoolean(YarnConfigOptions.SHIP_LOCAL_KEYTAB);
            localizedKeytabPath =
                    flinkConfiguration.getString(YarnConfigOptions.LOCALIZED_KEYTAB_PATH);
            if (localizeKeytab) {
                // Localize the keytab to YARN containers via local resource.
                LOG.info("Adding keytab {} to the AM container local resource bucket", keytab);
                remotePathKeytab =
                        fileUploader
                                .registerSingleLocalResource(
                                        localizedKeytabPath,
                                        new Path(keytab),
                                        "",
                                        LocalResourceType.FILE,
                                        false,
                                        false)
                                .getPath();
            } else {
                // // Assume Keytab is pre-installed in the container.
                localizedKeytabPath =
                        flinkConfiguration.getString(YarnConfigOptions.LOCALIZED_KEYTAB_PATH);
            }
        }

        final JobManagerProcessSpec processSpec =
                JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
                        flinkConfiguration, JobManagerOptions.TOTAL_PROCESS_MEMORY);
        final ContainerLaunchContext amContainer =
                setupApplicationMasterContainer(yarnClusterEntrypoint, hasKrb5, processSpec);

        boolean fetchToken = configuration.getBoolean(SecurityOptions.DELEGATION_TOKENS_ENABLED);
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);
        if (kerberosLoginProvider.isLoginPossible(true)) {
            setTokensFor(amContainer, fetchToken);
        } else {
            LOG.info(
                    "Cannot use kerberos delegation token manager, no valid kerberos credentials provided.");
        }

        amContainer.setLocalResources(fileUploader.getRegisteredLocalResources());
        fileUploader.close();

        Utils.setAclsFor(amContainer, flinkConfiguration);

        // Setup CLASSPATH and environment variables for ApplicationMaster
        final Map<String, String> appMasterEnv =
                generateApplicationMasterEnv(
                        fileUploader,
                        classPathBuilder.toString(),
                        localResourceDescFlinkJar.toString(),
                        appId.toString());

        if (localizedKeytabPath != null) {
            appMasterEnv.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, localizedKeytabPath);
            String principal = configuration.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL);
            appMasterEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, principal);
            if (remotePathKeytab != null) {
                appMasterEnv.put(YarnConfigKeys.REMOTE_KEYTAB_PATH, remotePathKeytab.toString());
            }
        }

        // To support Yarn Secure Integration Test Scenario
        if (remoteYarnSiteXmlPath != null) {
            appMasterEnv.put(
                    YarnConfigKeys.ENV_YARN_SITE_XML_PATH, remoteYarnSiteXmlPath.toString());
        }
        if (remoteKrb5Path != null) {
            appMasterEnv.put(YarnConfigKeys.ENV_KRB5_PATH, remoteKrb5Path.toString());
        }

        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(clusterSpecification.getMasterMemoryMB());
        capability.setVirtualCores(
                flinkConfiguration.getInteger(YarnConfigOptions.APP_MASTER_VCORES));

        final String customApplicationName = customName != null ? customName : applicationName;

        appContext.setApplicationName(customApplicationName);
        appContext.setApplicationType(applicationType != null ? applicationType : "Apache Flink");
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);

        // Set priority for application
        int priorityNum = flinkConfiguration.getInteger(YarnConfigOptions.APPLICATION_PRIORITY);
        if (priorityNum >= 0) {
            Priority priority = Priority.newInstance(priorityNum);
            appContext.setPriority(priority);
        }

        if (yarnQueue != null) {
            appContext.setQueue(yarnQueue);
        }

        setApplicationNodeLabel(appContext);

        setApplicationTags(appContext);

        // add a hook to clean up in case deployment fails
        Thread deploymentFailureHook =
                new DeploymentFailureHook(yarnApplication, fileUploader.getApplicationDir());
        Runtime.getRuntime().addShutdownHook(deploymentFailureHook);
        LOG.info("Submitting application master " + appId);
        yarnClient.submitApplication(appContext);

        LOG.info("Waiting for the cluster to be allocated");
        final long startTime = System.currentTimeMillis();
        long lastLogTime = System.currentTimeMillis();
        ApplicationReport report;
        YarnApplicationState lastAppState = YarnApplicationState.NEW;
        loop:
        while (true) {
            try {
                report = yarnClient.getApplicationReport(appId);
            } catch (IOException e) {
                throw new YarnDeploymentException("Failed to deploy the cluster.", e);
            }
            YarnApplicationState appState = report.getYarnApplicationState();
            LOG.debug("Application State: {}", appState);
            switch (appState) {
                case FAILED:
                case KILLED:
                    throw new YarnDeploymentException(
                            "The YARN application unexpectedly switched to state "
                                    + appState
                                    + " during deployment. \n"
                                    + "Diagnostics from YARN: "
                                    + report.getDiagnostics()
                                    + "\n"
                                    + "If log aggregation is enabled on your cluster, use this command to further investigate the issue:\n"
                                    + "yarn logs -applicationId "
                                    + appId);
                    // break ..
                case RUNNING:
                    LOG.info("YARN application has been deployed successfully.");
                    break loop;
                case FINISHED:
                    LOG.info("YARN application has been finished successfully.");
                    break loop;
                default:
                    if (appState != lastAppState) {
                        LOG.info("Deploying cluster, current state " + appState);
                    }
                    if (System.currentTimeMillis() - lastLogTime > 60000) {
                        lastLogTime = System.currentTimeMillis();
                        LOG.info(
                                "Deployment took more than {} seconds. Please check if the requested resources are available in the YARN cluster",
                                (lastLogTime - startTime) / 1000);
                    }
            }
            lastAppState = appState;
            Thread.sleep(250);
        }

        // since deployment was successful, remove the hook
        ShutdownHookUtil.removeShutdownHook(deploymentFailureHook, getClass().getSimpleName(), LOG);
        return report;
    }

    private void removeLocalhostBindHostSetting(
            Configuration configuration, ConfigOption<?> option) {
        configuration
                .getOptional(option)
                .filter(bindHost -> bindHost.equals("localhost"))
                .ifPresent(
                        bindHost -> {
                            LOG.info(
                                    "Removing 'localhost' {} setting from effective configuration; using '0.0.0.0' instead.",
                                    option);
                            configuration.removeConfig(option);
                        });
    }

    private void setTokensFor(ContainerLaunchContext containerLaunchContext, boolean fetchToken)
            throws Exception {
        Credentials credentials = new Credentials();

        LOG.info("Loading delegation tokens available locally to add to the AM container");
        // for user
        UserGroupInformation currUsr = UserGroupInformation.getCurrentUser();

        Collection<Token<? extends TokenIdentifier>> usrTok =
                currUsr.getCredentials().getAllTokens();
        for (Token<? extends TokenIdentifier> token : usrTok) {
            LOG.info("Adding user token " + token.getService() + " with " + token);
            credentials.addToken(token.getService(), token);
        }

        if (fetchToken) {
            LOG.info("Fetching delegation tokens to add to the AM container.");
            DelegationTokenManager delegationTokenManager =
                    new DefaultDelegationTokenManager(flinkConfiguration, null, null, null);
            DelegationTokenContainer container = new DelegationTokenContainer();
            delegationTokenManager.obtainDelegationTokens(container);

            // This is here for backward compatibility to make log aggregation work
            for (Map.Entry<String, byte[]> e : container.getTokens().entrySet()) {
                if (e.getKey().equals("hadoopfs")) {
                    credentials.addAll(HadoopDelegationTokenConverter.deserialize(e.getValue()));
                }
            }
        }

        ByteBuffer tokens = ByteBuffer.wrap(HadoopDelegationTokenConverter.serialize(credentials));
        containerLaunchContext.setTokens(tokens);

        LOG.info("Delegation tokens added to the AM container.");
    }

    /**
     * Returns the configured remote target home directory if set, otherwise returns the default
     * home directory.
     *
     * @param defaultFileSystem default file system used
     * @return the remote target home directory
     */
    @VisibleForTesting
    Path getStagingDir(FileSystem defaultFileSystem) throws IOException {
        final String configuredStagingDir =
                flinkConfiguration.getString(YarnConfigOptions.STAGING_DIRECTORY);
        if (configuredStagingDir == null) {
            return defaultFileSystem.getHomeDirectory();
        }
        FileSystem stagingDirFs =
                new Path(configuredStagingDir).getFileSystem(defaultFileSystem.getConf());
        return stagingDirFs.makeQualified(new Path(configuredStagingDir));
    }

    private int getFileReplication() {
        final int yarnFileReplication =
                yarnConfiguration.getInt(
                        DFSConfigKeys.DFS_REPLICATION_KEY, DFSConfigKeys.DFS_REPLICATION_DEFAULT);
        final int fileReplication =
                flinkConfiguration.getInteger(YarnConfigOptions.FILE_REPLICATION);
        return fileReplication > 0 ? fileReplication : yarnFileReplication;
    }

    private static String encodeYarnLocalResourceDescriptorListToString(
            List<YarnLocalResourceDescriptor> resources) {
        return String.join(
                LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR,
                resources.stream()
                        .map(YarnLocalResourceDescriptor::toString)
                        .collect(Collectors.toList()));
    }

    /**
     * Kills YARN application and stops YARN client.
     *
     * <p>Use this method to kill the App before it has been properly deployed
     */
    private void failSessionDuringDeployment(
            YarnClient yarnClient, YarnClientApplication yarnApplication) {
        LOG.info("Killing YARN application");

        try {
            yarnClient.killApplication(
                    yarnApplication.getNewApplicationResponse().getApplicationId());
        } catch (Exception e) {
            // we only log a debug message here because the "killApplication" call is a best-effort
            // call (we don't know if the application has been deployed when the error occurred).
            LOG.debug("Error while killing YARN application", e);
        }
    }

    private static class ClusterResourceDescription {
        public final long totalFreeMemory;
        public final long containerLimit;
        public final long[] nodeManagersFree;

        public ClusterResourceDescription(
                long totalFreeMemory, long containerLimit, long[] nodeManagersFree) {
            this.totalFreeMemory = totalFreeMemory;
            this.containerLimit = containerLimit;
            this.nodeManagersFree = nodeManagersFree;
        }
    }

    private ClusterResourceDescription getCurrentFreeClusterResources(YarnClient yarnClient)
            throws YarnException, IOException {
        List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.RUNNING);

        int totalFreeMemory = 0;
        long containerLimit = 0;
        long[] nodeManagersFree = new long[nodes.size()];

        for (int i = 0; i < nodes.size(); i++) {
            NodeReport rep = nodes.get(i);
            long free =
                    rep.getCapability().getMemorySize()
                            - (rep.getUsed() != null ? rep.getUsed().getMemorySize() : 0);
            nodeManagersFree[i] = free;
            totalFreeMemory += free;
            if (free > containerLimit) {
                containerLimit = free;
            }
        }
        return new ClusterResourceDescription(totalFreeMemory, containerLimit, nodeManagersFree);
    }

    @Override
    public String getClusterDescription() {

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);

            YarnClusterMetrics metrics = yarnClient.getYarnClusterMetrics();

            ps.append("NodeManagers in the ClusterClient " + metrics.getNumNodeManagers());
            List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.RUNNING);
            final String format = "|%-16s |%-16s %n";
            ps.printf("|Property         |Value          %n");
            ps.println("+---------------------------------------+");
            long totalMemory = 0;
            int totalCores = 0;
            for (NodeReport rep : nodes) {
                final Resource res = rep.getCapability();
                totalMemory += res.getMemorySize();
                totalCores += res.getVirtualCores();
                ps.format(format, "NodeID", rep.getNodeId());
                ps.format(format, "Memory", getDisplayMemory(res.getMemorySize()));
                ps.format(format, "vCores", res.getVirtualCores());
                ps.format(format, "HealthReport", rep.getHealthReport());
                ps.format(format, "Containers", rep.getNumContainers());
                ps.println("+---------------------------------------+");
            }
            ps.println(
                    "Summary: totalMemory "
                            + getDisplayMemory(totalMemory)
                            + " totalCores "
                            + totalCores);
            List<QueueInfo> qInfo = yarnClient.getAllQueues();
            for (QueueInfo q : qInfo) {
                ps.println(
                        "Queue: "
                                + q.getQueueName()
                                + ", Current Capacity: "
                                + q.getCurrentCapacity()
                                + " Max Capacity: "
                                + q.getMaximumCapacity()
                                + " Applications: "
                                + q.getApplications().size());
            }
            return baos.toString();
        } catch (Exception e) {
            throw new RuntimeException("Couldn't get cluster description", e);
        }
    }

    private void activateHighAvailabilitySupport(ApplicationSubmissionContext appContext)
            throws InvocationTargetException, IllegalAccessException {

        ApplicationSubmissionContextReflector reflector =
                ApplicationSubmissionContextReflector.getInstance();

        reflector.setKeepContainersAcrossApplicationAttempts(appContext, true);

        reflector.setAttemptFailuresValidityInterval(
                appContext,
                flinkConfiguration.getLong(
                        YarnConfigOptions.APPLICATION_ATTEMPT_FAILURE_VALIDITY_INTERVAL));
    }

    private void setApplicationTags(final ApplicationSubmissionContext appContext)
            throws InvocationTargetException, IllegalAccessException {

        final ApplicationSubmissionContextReflector reflector =
                ApplicationSubmissionContextReflector.getInstance();
        final String tagsString = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_TAGS);

        final Set<String> applicationTags = new HashSet<>();

        // Trim whitespace and cull empty tags
        for (final String tag : tagsString.split(",")) {
            final String trimmedTag = tag.trim();
            if (!trimmedTag.isEmpty()) {
                applicationTags.add(trimmedTag);
            }
        }

        reflector.setApplicationTags(appContext, applicationTags);
    }

    private void setApplicationNodeLabel(final ApplicationSubmissionContext appContext)
            throws InvocationTargetException, IllegalAccessException {

        if (nodeLabel != null) {
            final ApplicationSubmissionContextReflector reflector =
                    ApplicationSubmissionContextReflector.getInstance();
            reflector.setApplicationNodeLabel(appContext, nodeLabel);
        }
    }

    /**
     * Singleton object which uses reflection to determine whether the {@link
     * ApplicationSubmissionContext} supports various methods which, depending on the Hadoop
     * version, may or may not be supported.
     *
     * <p>If an unsupported method is invoked, nothing happens.
     *
     * <p>Currently three methods are proxied: - setApplicationTags (>= 2.4.0) -
     * setAttemptFailuresValidityInterval (>= 2.6.0) - setKeepContainersAcrossApplicationAttempts
     * (>= 2.4.0) - setNodeLabelExpression (>= 2.6.0)
     */
    private static class ApplicationSubmissionContextReflector {
        private static final Logger LOG =
                LoggerFactory.getLogger(ApplicationSubmissionContextReflector.class);

        private static final ApplicationSubmissionContextReflector instance =
                new ApplicationSubmissionContextReflector(ApplicationSubmissionContext.class);

        public static ApplicationSubmissionContextReflector getInstance() {
            return instance;
        }

        private static final String APPLICATION_TAGS_METHOD_NAME = "setApplicationTags";
        private static final String ATTEMPT_FAILURES_METHOD_NAME =
                "setAttemptFailuresValidityInterval";
        private static final String KEEP_CONTAINERS_METHOD_NAME =
                "setKeepContainersAcrossApplicationAttempts";
        private static final String NODE_LABEL_EXPRESSION_NAME = "setNodeLabelExpression";

        private final Method applicationTagsMethod;
        private final Method attemptFailuresValidityIntervalMethod;
        private final Method keepContainersMethod;
        @Nullable private final Method nodeLabelExpressionMethod;

        private ApplicationSubmissionContextReflector(Class<ApplicationSubmissionContext> clazz) {
            Method applicationTagsMethod;
            Method attemptFailuresValidityIntervalMethod;
            Method keepContainersMethod;
            Method nodeLabelExpressionMethod;

            try {
                // this method is only supported by Hadoop 2.4.0 onwards
                applicationTagsMethod = clazz.getMethod(APPLICATION_TAGS_METHOD_NAME, Set.class);
                LOG.debug(
                        "{} supports method {}.",
                        clazz.getCanonicalName(),
                        APPLICATION_TAGS_METHOD_NAME);
            } catch (NoSuchMethodException e) {
                LOG.debug(
                        "{} does not support method {}.",
                        clazz.getCanonicalName(),
                        APPLICATION_TAGS_METHOD_NAME);
                // assign null because the Hadoop version apparently does not support this call.
                applicationTagsMethod = null;
            }

            this.applicationTagsMethod = applicationTagsMethod;

            try {
                // this method is only supported by Hadoop 2.6.0 onwards
                attemptFailuresValidityIntervalMethod =
                        clazz.getMethod(ATTEMPT_FAILURES_METHOD_NAME, long.class);
                LOG.debug(
                        "{} supports method {}.",
                        clazz.getCanonicalName(),
                        ATTEMPT_FAILURES_METHOD_NAME);
            } catch (NoSuchMethodException e) {
                LOG.debug(
                        "{} does not support method {}.",
                        clazz.getCanonicalName(),
                        ATTEMPT_FAILURES_METHOD_NAME);
                // assign null because the Hadoop version apparently does not support this call.
                attemptFailuresValidityIntervalMethod = null;
            }

            this.attemptFailuresValidityIntervalMethod = attemptFailuresValidityIntervalMethod;

            try {
                // this method is only supported by Hadoop 2.4.0 onwards
                keepContainersMethod = clazz.getMethod(KEEP_CONTAINERS_METHOD_NAME, boolean.class);
                LOG.debug(
                        "{} supports method {}.",
                        clazz.getCanonicalName(),
                        KEEP_CONTAINERS_METHOD_NAME);
            } catch (NoSuchMethodException e) {
                LOG.debug(
                        "{} does not support method {}.",
                        clazz.getCanonicalName(),
                        KEEP_CONTAINERS_METHOD_NAME);
                // assign null because the Hadoop version apparently does not support this call.
                keepContainersMethod = null;
            }

            this.keepContainersMethod = keepContainersMethod;

            try {
                nodeLabelExpressionMethod =
                        clazz.getMethod(NODE_LABEL_EXPRESSION_NAME, String.class);
                LOG.debug(
                        "{} supports method {}.",
                        clazz.getCanonicalName(),
                        NODE_LABEL_EXPRESSION_NAME);
            } catch (NoSuchMethodException e) {
                LOG.debug(
                        "{} does not support method {}.",
                        clazz.getCanonicalName(),
                        NODE_LABEL_EXPRESSION_NAME);
                nodeLabelExpressionMethod = null;
            }

            this.nodeLabelExpressionMethod = nodeLabelExpressionMethod;
        }

        public void setApplicationTags(
                ApplicationSubmissionContext appContext, Set<String> applicationTags)
                throws InvocationTargetException, IllegalAccessException {
            if (applicationTagsMethod != null) {
                LOG.debug(
                        "Calling method {} of {}.",
                        applicationTagsMethod.getName(),
                        appContext.getClass().getCanonicalName());
                applicationTagsMethod.invoke(appContext, applicationTags);
            } else {
                LOG.debug(
                        "{} does not support method {}. Doing nothing.",
                        appContext.getClass().getCanonicalName(),
                        APPLICATION_TAGS_METHOD_NAME);
            }
        }

        public void setApplicationNodeLabel(
                ApplicationSubmissionContext appContext, String nodeLabel)
                throws InvocationTargetException, IllegalAccessException {
            if (nodeLabelExpressionMethod != null) {
                LOG.debug(
                        "Calling method {} of {}.",
                        nodeLabelExpressionMethod.getName(),
                        appContext.getClass().getCanonicalName());
                nodeLabelExpressionMethod.invoke(appContext, nodeLabel);
            } else {
                LOG.debug(
                        "{} does not support method {}. Doing nothing.",
                        appContext.getClass().getCanonicalName(),
                        NODE_LABEL_EXPRESSION_NAME);
            }
        }

        public void setAttemptFailuresValidityInterval(
                ApplicationSubmissionContext appContext, long validityInterval)
                throws InvocationTargetException, IllegalAccessException {
            if (attemptFailuresValidityIntervalMethod != null) {
                LOG.debug(
                        "Calling method {} of {}.",
                        attemptFailuresValidityIntervalMethod.getName(),
                        appContext.getClass().getCanonicalName());
                attemptFailuresValidityIntervalMethod.invoke(appContext, validityInterval);
            } else {
                LOG.debug(
                        "{} does not support method {}. Doing nothing.",
                        appContext.getClass().getCanonicalName(),
                        ATTEMPT_FAILURES_METHOD_NAME);
            }
        }

        public void setKeepContainersAcrossApplicationAttempts(
                ApplicationSubmissionContext appContext, boolean keepContainers)
                throws InvocationTargetException, IllegalAccessException {

            if (keepContainersMethod != null) {
                LOG.debug(
                        "Calling method {} of {}.",
                        keepContainersMethod.getName(),
                        appContext.getClass().getCanonicalName());
                keepContainersMethod.invoke(appContext, keepContainers);
            } else {
                LOG.debug(
                        "{} does not support method {}. Doing nothing.",
                        appContext.getClass().getCanonicalName(),
                        KEEP_CONTAINERS_METHOD_NAME);
            }
        }
    }

    private static class YarnDeploymentException extends RuntimeException {
        private static final long serialVersionUID = -812040641215388943L;

        public YarnDeploymentException(String message) {
            super(message);
        }

        public YarnDeploymentException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private class DeploymentFailureHook extends Thread {

        private final YarnClient yarnClient;
        private final YarnClientApplication yarnApplication;
        private final Path yarnFilesDir;

        DeploymentFailureHook(YarnClientApplication yarnApplication, Path yarnFilesDir) {
            this.yarnApplication = Preconditions.checkNotNull(yarnApplication);
            this.yarnFilesDir = Preconditions.checkNotNull(yarnFilesDir);

            // A new yarn client need to be created in shutdown hook in order to avoid
            // the yarn client has been closed by YarnClusterDescriptor.
            this.yarnClient = YarnClient.createYarnClient();
            this.yarnClient.init(yarnConfiguration);
        }

        @Override
        public void run() {
            LOG.info("Cancelling deployment from Deployment Failure Hook");
            yarnClient.start();
            failSessionDuringDeployment(yarnClient, yarnApplication);
            yarnClient.stop();
            LOG.info("Deleting files in {}.", yarnFilesDir);
            try {
                FileSystem fs = FileSystem.get(yarnConfiguration);

                if (!fs.delete(yarnFilesDir, true)) {
                    throw new IOException(
                            "Deleting files in " + yarnFilesDir + " was unsuccessful");
                }

                fs.close();
            } catch (IOException e) {
                LOG.error("Failed to delete Flink Jar and configuration files in HDFS", e);
            }
        }
    }

    @VisibleForTesting
    void addLibFoldersToShipFiles(Collection<Path> effectiveShipFiles) {
        // Add lib folder to the ship files if the environment variable is set.
        // This is for convenience when running from the command-line.
        // (for other files users explicitly set the ship files)
        String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);
        if (libDir != null) {
            File directoryFile = new File(libDir);
            if (directoryFile.isDirectory()) {
                effectiveShipFiles.add(getPathFromLocalFile(directoryFile));
            } else {
                throw new YarnDeploymentException(
                        "The environment variable '"
                                + ENV_FLINK_LIB_DIR
                                + "' is set to '"
                                + libDir
                                + "' but the directory doesn't exist.");
            }
        } else if (shipFiles.isEmpty()) {
            LOG.warn(
                    "Environment variable '{}' not set and ship files have not been provided manually. "
                            + "Not shipping any library files.",
                    ENV_FLINK_LIB_DIR);
        }
    }

    @VisibleForTesting
    void addUsrLibFolderToShipFiles(Collection<File> effectiveShipFiles) {
        // Add usrlib folder to the ship files if it exists
        // Classes in the folder will be loaded by UserClassLoader if CLASSPATH_INCLUDE_USER_JAR is
        // DISABLED.
        ClusterEntrypointUtils.tryFindUserLibDirectory()
                .ifPresent(
                        usrLibDirFile -> {
                            effectiveShipFiles.add(usrLibDirFile);
                            LOG.info(
                                    "usrlib: {} will be shipped automatically.",
                                    usrLibDirFile.getAbsolutePath());
                        });
    }

    @VisibleForTesting
    void addPluginsFoldersToShipFiles(Collection<Path> effectiveShipFiles) {
        final Optional<File> pluginsDir = PluginConfig.getPluginsDir();
        pluginsDir.ifPresent(dir -> effectiveShipFiles.add(getPathFromLocalFile(dir)));
    }

    ContainerLaunchContext setupApplicationMasterContainer(
            String yarnClusterEntrypoint, boolean hasKrb5, JobManagerProcessSpec processSpec) {
        // ------------------ Prepare Application Master Container  ------------------------------

        // respect custom JVM options in the YAML file
        String javaOpts = flinkConfiguration.getString(CoreOptions.FLINK_JVM_OPTIONS);
        if (flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS).length() > 0) {
            javaOpts += " " + flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS);
        }

        javaOpts += " " + IGNORE_UNRECOGNIZED_VM_OPTIONS;

        // krb5.conf file will be available as local resource in JM/TM container
        if (hasKrb5) {
            javaOpts += " -Djava.security.krb5.conf=krb5.conf";
        }

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        final Map<String, String> startCommandValues = new HashMap<>();
        startCommandValues.put("java", "$JAVA_HOME/bin/java");

        String jvmHeapMem =
                JobManagerProcessUtils.generateJvmParametersStr(processSpec, flinkConfiguration);
        startCommandValues.put("jvmmem", jvmHeapMem);

        startCommandValues.put("jvmopts", javaOpts);
        startCommandValues.put(
                "logging", YarnLogConfigUtil.getLoggingYarnCommand(flinkConfiguration));

        startCommandValues.put("class", yarnClusterEntrypoint);
        startCommandValues.put(
                "redirects",
                "1> "
                        + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                        + "/jobmanager.out "
                        + "2> "
                        + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                        + "/jobmanager.err");
        String dynamicParameterListStr =
                JobManagerProcessUtils.generateDynamicConfigsStr(processSpec);
        startCommandValues.put("args", dynamicParameterListStr);

        final String commandTemplate =
                flinkConfiguration.getString(
                        ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
                        ConfigConstants.DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE);
        final String amCommand =
                BootstrapTools.getStartCommand(commandTemplate, startCommandValues);

        amContainer.setCommands(Collections.singletonList(amCommand));

        LOG.debug("Application Master start command: " + amCommand);

        return amContainer;
    }

    private static YarnConfigOptions.UserJarInclusion getUserJarInclusionMode(
            org.apache.flink.configuration.Configuration config) {
        return config.get(YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR);
    }

    private static boolean isUsrLibDirIncludedInShipFiles(
            List<Path> shipFiles, YarnConfiguration yarnConfig) {
        return shipFiles.stream()
                .map(FunctionUtils.uncheckedFunction(path -> getFileStatus(path, yarnConfig)))
                .filter(FileStatus::isDirectory)
                .map(status -> status.getPath().getName().toLowerCase())
                .anyMatch(name -> name.equals(DEFAULT_FLINK_USR_LIB_DIR));
    }

    private void setClusterEntrypointInfoToConfig(final ApplicationReport report) {
        checkNotNull(report);

        final ApplicationId appId = report.getApplicationId();
        final String host = report.getHost();
        final int port = report.getRpcPort();

        LOG.info("Found Web Interface {}:{} of application '{}'.", host, port, appId);

        flinkConfiguration.setString(JobManagerOptions.ADDRESS, host);
        flinkConfiguration.setInteger(JobManagerOptions.PORT, port);

        flinkConfiguration.setString(RestOptions.ADDRESS, host);
        flinkConfiguration.setInteger(RestOptions.PORT, port);

        flinkConfiguration.set(YarnConfigOptions.APPLICATION_ID, ConverterUtils.toString(appId));

        setHAClusterIdIfNotSet(flinkConfiguration, appId);
    }

    private void setHAClusterIdIfNotSet(Configuration configuration, ApplicationId appId) {
        // set cluster-id to app id if not specified
        if (!configuration.contains(HighAvailabilityOptions.HA_CLUSTER_ID)) {
            configuration.set(
                    HighAvailabilityOptions.HA_CLUSTER_ID, ConverterUtils.toString(appId));
        }
    }

    public static void logDetachedClusterInformation(
            ApplicationId yarnApplicationId, Logger logger) {
        logger.info(
                "The Flink YARN session cluster has been started in detached mode. In order to "
                        + "stop Flink gracefully, use the following command:\n"
                        + "$ echo \"stop\" | ./bin/yarn-session.sh -id {}\n"
                        + "If this should not be possible, then you can also kill Flink via YARN's web interface or via:\n"
                        + "$ yarn application -kill {}\n"
                        + "Note that killing Flink might not clean up all job artifacts and temporary files.",
                yarnApplicationId,
                yarnApplicationId);
    }

    @VisibleForTesting
    Map<String, String> generateApplicationMasterEnv(
            final YarnApplicationFileUploader fileUploader,
            final String classPathStr,
            final String localFlinkJarStr,
            final String appIdStr)
            throws IOException {
        final Map<String, String> env = new HashMap<>();
        // set user specified app master environment variables
        env.putAll(
                ConfigurationUtils.getPrefixedKeyValuePairs(
                        ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX,
                        this.flinkConfiguration));
        // set Flink app class path
        env.put(ENV_FLINK_CLASSPATH, classPathStr);
        // Set FLINK_LIB_DIR to `lib` folder under working dir in container
        env.put(ENV_FLINK_LIB_DIR, Path.CUR_DIR + "/" + ConfigConstants.DEFAULT_FLINK_LIB_DIR);
        // Set FLINK_OPT_DIR to `opt` folder under working dir in container
        env.put(ENV_FLINK_OPT_DIR, Path.CUR_DIR + "/" + ConfigConstants.DEFAULT_FLINK_OPT_DIR);
        // set Flink on YARN internal configuration values
        env.put(YarnConfigKeys.FLINK_DIST_JAR, localFlinkJarStr);
        env.put(YarnConfigKeys.ENV_APP_ID, appIdStr);
        env.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, fileUploader.getHomeDir().toString());
        env.put(
                YarnConfigKeys.ENV_CLIENT_SHIP_FILES,
                encodeYarnLocalResourceDescriptorListToString(
                        fileUploader.getEnvShipResourceList()));
        env.put(
                YarnConfigKeys.FLINK_YARN_FILES,
                fileUploader.getApplicationDir().toUri().toString());
        // https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md#identity-on-an-insecure-cluster-hadoop_user_name
        env.put(
                YarnConfigKeys.ENV_HADOOP_USER_NAME,
                UserGroupInformation.getCurrentUser().getUserName());
        // set classpath from YARN configuration
        Utils.setupYarnClassPath(this.yarnConfiguration, env);
        return env;
    }

    private String getDisplayMemory(long memoryMB) {
        return MemorySize.ofMebiBytes(memoryMB).toHumanReadableString();
    }
}
