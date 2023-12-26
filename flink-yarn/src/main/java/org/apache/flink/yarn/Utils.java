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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnResourceManagerDriverConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR;

/** Utility class that provides helper methods to work with Apache Hadoop YARN. */
public final class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    /** KRB5 file name populated in YARN container for secure IT run. */
    public static final String KRB5_FILE_NAME = "krb5.conf";

    /** Yarn site xml file name populated in YARN container for secure IT run. */
    public static final String YARN_SITE_FILE_NAME = "yarn-site.xml";

    /** Constant representing a wildcard access control list. */
    private static final String WILDCARD_ACL = "*";

    /** The prefixes that Flink adds to the YARN config. */
    private static final String[] FLINK_CONFIG_PREFIXES = {"flink.yarn."};

    @VisibleForTesting
    static final String YARN_RM_FAIR_SCHEDULER_CLAZZ =
            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler";

    @VisibleForTesting
    static final String YARN_RM_SLS_FAIR_SCHEDULER_CLAZZ =
            "org.apache.hadoop.yarn.sls.scheduler.SLSFairScheduler";

    @VisibleForTesting
    static final String YARN_RM_INCREMENT_ALLOCATION_MB_KEY =
            "yarn.resource-types.memory-mb.increment-allocation";

    @VisibleForTesting
    static final String YARN_RM_INCREMENT_ALLOCATION_MB_LEGACY_KEY =
            "yarn.scheduler.increment-allocation-mb";

    private static final int DEFAULT_YARN_RM_INCREMENT_ALLOCATION_MB = 1024;

    @VisibleForTesting
    static final String YARN_RM_INCREMENT_ALLOCATION_VCORES_KEY =
            "yarn.resource-types.vcores.increment-allocation";

    @VisibleForTesting
    static final String YARN_RM_INCREMENT_ALLOCATION_VCORES_LEGACY_KEY =
            "yarn.scheduler.increment-allocation-vcores";

    private static final int DEFAULT_YARN_RM_INCREMENT_ALLOCATION_VCORES = 1;

    public static void setupYarnClassPath(Configuration conf, Map<String, String> appMasterEnv) {
        addToEnvironment(
                appMasterEnv, Environment.CLASSPATH.name(), appMasterEnv.get(ENV_FLINK_CLASSPATH));
        String[] applicationClassPathEntries =
                conf.getStrings(
                        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
        for (String c : applicationClassPathEntries) {
            addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
        }
    }

    /**
     * Deletes the YARN application files, e.g., Flink binaries, libraries, etc., from the remote
     * filesystem.
     *
     * @param applicationFilesDir The application files directory.
     */
    public static void deleteApplicationFiles(final String applicationFilesDir) {
        if (!StringUtils.isNullOrWhitespaceOnly(applicationFilesDir)) {
            final org.apache.flink.core.fs.Path path =
                    new org.apache.flink.core.fs.Path(applicationFilesDir);
            try {
                final org.apache.flink.core.fs.FileSystem fileSystem = path.getFileSystem();
                if (!fileSystem.delete(path, true)) {
                    LOG.error(
                            "Deleting yarn application files under {} was unsuccessful.",
                            applicationFilesDir);
                }
            } catch (final IOException e) {
                LOG.error(
                        "Could not properly delete yarn application files directory {}.",
                        applicationFilesDir,
                        e);
            }
        } else {
            LOG.debug(
                    "No yarn application files directory set. Therefore, cannot clean up the data.");
        }
    }

    /**
     * Creates a YARN resource for the remote object at the given location.
     *
     * @param remoteRsrcPath remote location of the resource
     * @param resourceSize size of the resource
     * @param resourceModificationTime last modification time of the resource
     * @return YARN resource
     */
    static LocalResource registerLocalResource(
            Path remoteRsrcPath,
            long resourceSize,
            long resourceModificationTime,
            LocalResourceVisibility resourceVisibility,
            LocalResourceType resourceType) {
        LocalResource localResource = Records.newRecord(LocalResource.class);
        localResource.setResource(URL.fromURI(remoteRsrcPath.toUri()));
        localResource.setSize(resourceSize);
        localResource.setTimestamp(resourceModificationTime);
        localResource.setType(resourceType);
        localResource.setVisibility(resourceVisibility);
        return localResource;
    }

    /**
     * Creates a YARN resource for the remote object at the given location.
     *
     * @param fs remote filesystem
     * @param remoteRsrcPath resource path to be registered
     * @return YARN resource
     */
    private static LocalResource registerLocalResource(
            FileSystem fs, Path remoteRsrcPath, LocalResourceType resourceType) throws IOException {
        FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
        return registerLocalResource(
                remoteRsrcPath,
                jarStat.getLen(),
                jarStat.getModificationTime(),
                LocalResourceVisibility.APPLICATION,
                resourceType);
    }

    /**
     * Copied method from org.apache.hadoop.yarn.util.Apps. It was broken by YARN-1824 (2.4.0) and
     * fixed for 2.4.1 by https://issues.apache.org/jira/browse/YARN-1931
     */
    public static void addToEnvironment(
            Map<String, String> environment, String variable, String value) {
        String val = environment.get(variable);
        if (val == null) {
            val = value;
        } else {
            val = val + File.pathSeparator + value;
        }
        environment.put(StringInterner.weakIntern(variable), StringInterner.weakIntern(val));
    }

    /**
     * Resolve keytab path either as absolute path or relative to working directory.
     *
     * @param workingDir current working directory
     * @param keytabPath configured keytab path.
     * @return resolved keytab path, or null if not found.
     */
    public static String resolveKeytabPath(String workingDir, String keytabPath) {
        String keytab = null;
        if (keytabPath != null) {
            File f;
            f = new File(keytabPath);
            if (f.exists()) {
                keytab = f.getAbsolutePath();
                LOG.info("Resolved keytab path: {}", keytab);
            } else {
                // try using relative paths, this is the case when the keytab was shipped
                // as a local resource
                f = new File(workingDir, keytabPath);
                if (f.exists()) {
                    keytab = f.getAbsolutePath();
                    LOG.info("Resolved keytab path: {}", keytab);
                } else {
                    LOG.warn("Could not resolve keytab path with: {}", keytabPath);
                    keytab = null;
                }
            }
        }
        return keytab;
    }

    /** Private constructor to prevent instantiation. */
    private Utils() {
        throw new RuntimeException();
    }

    /**
     * Creates the launch context, which describes how to bring up a TaskExecutor / TaskManager
     * process in an allocated YARN container.
     *
     * <p>This code is extremely YARN specific and registers all the resources that the TaskExecutor
     * needs (such as JAR file, config file, ...) and all environment variables in a YARN container
     * launch context. The launch context then ensures that those resources will be copied into the
     * containers transient working directory.
     *
     * @param flinkConfig The Flink configuration object.
     * @param yarnConfig The YARN configuration object.
     * @param configuration The YarnResourceManagerDriver configurations.
     * @param tmParams The TaskExecutor container memory parameters.
     * @param taskManagerDynamicProperties The dynamic configurations to be updated for the
     *     TaskExecutors based on client uploaded Flink config.
     * @param workingDirectory The current application master container's working directory.
     * @param taskManagerMainClass The class with the main method.
     * @param log The logger.
     * @return The launch context for the TaskManager processes.
     * @throws Exception Thrown if the launch context could not be created, for example if the
     *     resources could not be copied.
     */
    static ContainerLaunchContext createTaskExecutorContext(
            org.apache.flink.configuration.Configuration flinkConfig,
            YarnConfiguration yarnConfig,
            YarnResourceManagerDriverConfiguration configuration,
            ContaineredTaskManagerParameters tmParams,
            String taskManagerDynamicProperties,
            String workingDirectory,
            Class<?> taskManagerMainClass,
            Logger log)
            throws Exception {

        // get and validate all relevant variables

        String remoteFlinkJarPath =
                checkNotNull(
                        configuration.getFlinkDistJar(),
                        "Environment variable %s not set",
                        YarnConfigKeys.FLINK_DIST_JAR);

        String shipListString =
                checkNotNull(
                        configuration.getClientShipFiles(),
                        "Environment variable %s not set",
                        YarnConfigKeys.ENV_CLIENT_SHIP_FILES);

        final String remoteKeytabPath = configuration.getRemoteKeytabPath();
        final String localKeytabPath = configuration.getLocalKeytabPath();
        final String keytabPrincipal = configuration.getKeytabPrinciple();
        final String remoteYarnConfPath = configuration.getYarnSiteXMLPath();
        final String remoteKrb5Path = configuration.getKrb5Path();

        if (log.isDebugEnabled()) {
            log.debug("TM:remote keytab path obtained {}", remoteKeytabPath);
            log.debug("TM:local keytab path obtained {}", localKeytabPath);
            log.debug("TM:keytab principal obtained {}", keytabPrincipal);
            log.debug("TM:remote yarn conf path obtained {}", remoteYarnConfPath);
            log.debug("TM:remote krb5 path obtained {}", remoteKrb5Path);
        }

        String classPathString =
                checkNotNull(
                        configuration.getFlinkClasspath(),
                        "Environment variable %s not set",
                        YarnConfigKeys.ENV_FLINK_CLASSPATH);

        // register keytab
        LocalResource keytabResource = null;
        if (remoteKeytabPath != null) {
            log.info(
                    "TM:Adding keytab {} to the container local resource bucket", remoteKeytabPath);
            Path keytabPath = new Path(remoteKeytabPath);
            FileSystem fs = keytabPath.getFileSystem(yarnConfig);
            keytabResource = registerLocalResource(fs, keytabPath, LocalResourceType.FILE);
        }

        // To support Yarn Secure Integration Test Scenario
        LocalResource yarnConfResource = null;
        if (remoteYarnConfPath != null) {
            log.info(
                    "TM:Adding remoteYarnConfPath {} to the container local resource bucket",
                    remoteYarnConfPath);
            Path yarnConfPath = new Path(remoteYarnConfPath);
            FileSystem fs = yarnConfPath.getFileSystem(yarnConfig);
            yarnConfResource = registerLocalResource(fs, yarnConfPath, LocalResourceType.FILE);
        }

        // register krb5.conf
        LocalResource krb5ConfResource = null;
        boolean hasKrb5 = false;
        if (remoteKrb5Path != null) {
            log.info(
                    "Adding remoteKrb5Path {} to the container local resource bucket",
                    remoteKrb5Path);
            Path krb5ConfPath = new Path(remoteKrb5Path);
            FileSystem fs = krb5ConfPath.getFileSystem(yarnConfig);
            krb5ConfResource = registerLocalResource(fs, krb5ConfPath, LocalResourceType.FILE);
            hasKrb5 = true;
        }

        Map<String, LocalResource> taskManagerLocalResources = new HashMap<>();

        // register Flink Jar with remote HDFS
        final YarnLocalResourceDescriptor flinkDistLocalResourceDesc =
                YarnLocalResourceDescriptor.fromString(remoteFlinkJarPath);
        taskManagerLocalResources.put(
                flinkDistLocalResourceDesc.getResourceKey(),
                flinkDistLocalResourceDesc.toLocalResource());

        // To support Yarn Secure Integration Test Scenario
        if (yarnConfResource != null) {
            taskManagerLocalResources.put(YARN_SITE_FILE_NAME, yarnConfResource);
        }
        if (krb5ConfResource != null) {
            taskManagerLocalResources.put(KRB5_FILE_NAME, krb5ConfResource);
        }
        if (keytabResource != null) {
            taskManagerLocalResources.put(localKeytabPath, keytabResource);
        }

        // prepare additional files to be shipped
        decodeYarnLocalResourceDescriptorListFromString(shipListString)
                .forEach(
                        resourceDesc ->
                                taskManagerLocalResources.put(
                                        resourceDesc.getResourceKey(),
                                        resourceDesc.toLocalResource()));

        // now that all resources are prepared, we can create the launch context

        log.info("Creating container launch context for TaskManagers");

        boolean hasLogback = new File(workingDirectory, "logback.xml").exists();
        boolean hasLog4j = new File(workingDirectory, "log4j.properties").exists();

        String launchCommand =
                BootstrapTools.getTaskManagerShellCommand(
                        flinkConfig,
                        tmParams,
                        ".",
                        ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                        hasLogback,
                        hasLog4j,
                        hasKrb5,
                        taskManagerMainClass,
                        taskManagerDynamicProperties);

        if (log.isDebugEnabled()) {
            log.debug("Starting TaskManagers with command: " + launchCommand);
        } else {
            log.info("Starting TaskManagers");
        }

        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Collections.singletonList(launchCommand));
        ctx.setLocalResources(taskManagerLocalResources);

        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.putAll(tmParams.taskManagerEnv());

        // add YARN classpath, etc to the container environment
        containerEnv.put(ENV_FLINK_CLASSPATH, classPathString);
        setupYarnClassPath(yarnConfig, containerEnv);

        containerEnv.put(
                YarnConfigKeys.ENV_HADOOP_USER_NAME,
                UserGroupInformation.getCurrentUser().getUserName());

        if (remoteKeytabPath != null && localKeytabPath != null && keytabPrincipal != null) {
            containerEnv.put(YarnConfigKeys.REMOTE_KEYTAB_PATH, remoteKeytabPath);
            containerEnv.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, localKeytabPath);
            containerEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, keytabPrincipal);
        } else if (localKeytabPath != null && keytabPrincipal != null) {
            containerEnv.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, localKeytabPath);
            containerEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, keytabPrincipal);
        }

        ctx.setEnvironment(containerEnv);

        setAclsFor(ctx, flinkConfig);

        // For TaskManager YARN container context, read the tokens from the jobmanager yarn
        // container local file.
        // NOTE: must read the tokens from the local file, not from the UGI context, because if UGI
        // is login
        // using Kerberos keytabs, there is no HDFS delegation token in the UGI context.
        final String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);

        if (fileLocation != null) {
            log.debug("Adding security tokens to TaskExecutor's container launch context.");

            try (DataOutputBuffer dob = new DataOutputBuffer()) {
                Credentials cred =
                        Credentials.readTokenStorageFile(
                                new File(fileLocation),
                                HadoopUtils.getHadoopConfiguration(flinkConfig));

                // Filter out AMRMToken before setting the tokens to the TaskManager container
                // context.
                Credentials taskManagerCred = new Credentials();
                Collection<Token<? extends TokenIdentifier>> userTokens = cred.getAllTokens();
                for (Token<? extends TokenIdentifier> token : userTokens) {
                    if (!token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                        taskManagerCred.addToken(token.getService(), token);
                    }
                }

                taskManagerCred.writeTokenStorageToStream(dob);
                ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
                ctx.setTokens(securityTokens);
            } catch (Throwable t) {
                log.error("Failed to add Hadoop's security tokens.", t);
            }
        } else {
            log.info(
                    "Could not set security tokens because Hadoop's token file location is unknown.");
        }

        return ctx;
    }

    static boolean isRemotePath(String path) throws IOException {
        org.apache.flink.core.fs.Path flinkPath = new org.apache.flink.core.fs.Path(path);
        return flinkPath.getFileSystem().isDistributedFS();
    }

    private static List<YarnLocalResourceDescriptor>
            decodeYarnLocalResourceDescriptorListFromString(String resources) throws Exception {
        final List<YarnLocalResourceDescriptor> resourceDescriptors = new ArrayList<>();
        for (String shipResourceDescStr : resources.split(LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR)) {
            if (!shipResourceDescStr.isEmpty()) {
                resourceDescriptors.add(
                        YarnLocalResourceDescriptor.fromString(shipResourceDescStr));
            }
        }
        return resourceDescriptors;
    }

    @VisibleForTesting
    static Resource getUnitResource(YarnConfiguration yarnConfig) {
        final int unitMemMB, unitVcore;

        final String yarnRmSchedulerClazzName = yarnConfig.get(YarnConfiguration.RM_SCHEDULER);
        if (Objects.equals(yarnRmSchedulerClazzName, YARN_RM_FAIR_SCHEDULER_CLAZZ)
                || Objects.equals(yarnRmSchedulerClazzName, YARN_RM_SLS_FAIR_SCHEDULER_CLAZZ)) {
            String propMem = yarnConfig.get(YARN_RM_INCREMENT_ALLOCATION_MB_KEY);
            String propVcore = yarnConfig.get(YARN_RM_INCREMENT_ALLOCATION_VCORES_KEY);

            unitMemMB =
                    propMem != null
                            ? Integer.parseInt(propMem)
                            : yarnConfig.getInt(
                                    YARN_RM_INCREMENT_ALLOCATION_MB_LEGACY_KEY,
                                    DEFAULT_YARN_RM_INCREMENT_ALLOCATION_MB);
            unitVcore =
                    propVcore != null
                            ? Integer.parseInt(propVcore)
                            : yarnConfig.getInt(
                                    YARN_RM_INCREMENT_ALLOCATION_VCORES_LEGACY_KEY,
                                    DEFAULT_YARN_RM_INCREMENT_ALLOCATION_VCORES);
        } else {
            unitMemMB =
                    yarnConfig.getInt(
                            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
                            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
            unitVcore =
                    yarnConfig.getInt(
                            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
                            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
        }

        return Resource.newInstance(unitMemMB, unitVcore);
    }

    public static List<Path> getQualifiedRemoteProvidedLibDirs(
            org.apache.flink.configuration.Configuration configuration,
            YarnConfiguration yarnConfiguration)
            throws IOException {

        return getRemoteSharedLibPaths(
                configuration,
                pathStr -> {
                    final Path path = new Path(pathStr);
                    return path.getFileSystem(yarnConfiguration).makeQualified(path);
                });
    }

    private static List<Path> getRemoteSharedLibPaths(
            org.apache.flink.configuration.Configuration configuration,
            FunctionWithException<String, Path, IOException> strToPathMapper)
            throws IOException {

        final List<Path> providedLibDirs =
                ConfigUtils.decodeListFromConfig(
                        configuration, YarnConfigOptions.PROVIDED_LIB_DIRS, strToPathMapper);

        for (Path path : providedLibDirs) {
            if (!Utils.isRemotePath(path.toString())) {
                throw new IllegalArgumentException(
                        "The \""
                                + YarnConfigOptions.PROVIDED_LIB_DIRS.key()
                                + "\" should only contain"
                                + " dirs accessible from all worker nodes, while the \""
                                + path
                                + "\" is local.");
            }
        }
        return providedLibDirs;
    }

    public static boolean isUsrLibDirectory(final FileSystem fileSystem, final Path path)
            throws IOException {
        final FileStatus fileStatus = fileSystem.getFileStatus(path);
        // Use the Path obj from fileStatus to get rid of trailing slash
        return fileStatus.isDirectory()
                && ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR.equals(fileStatus.getPath().getName());
    }

    public static Optional<Path> getQualifiedRemoteProvidedUsrLib(
            org.apache.flink.configuration.Configuration configuration,
            YarnConfiguration yarnConfiguration)
            throws IOException, IllegalArgumentException {
        String usrlib = configuration.getString(YarnConfigOptions.PROVIDED_USRLIB_DIR);
        if (usrlib == null) {
            return Optional.empty();
        }
        final Path qualifiedUsrLibPath =
                FileSystem.get(yarnConfiguration).makeQualified(new Path(usrlib));
        checkArgument(
                isRemotePath(qualifiedUsrLibPath.toString()),
                "The \"%s\" must point to a remote dir "
                        + "which is accessible from all worker nodes.",
                YarnConfigOptions.PROVIDED_USRLIB_DIR.key());
        checkArgument(
                isUsrLibDirectory(FileSystem.get(yarnConfiguration), qualifiedUsrLibPath),
                "The \"%s\" should be named with \"%s\".",
                YarnConfigOptions.PROVIDED_USRLIB_DIR.key(),
                ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);
        return Optional.of(qualifiedUsrLibPath);
    }

    public static YarnConfiguration getYarnAndHadoopConfiguration(
            org.apache.flink.configuration.Configuration flinkConfig) {
        final YarnConfiguration yarnConfig = getYarnConfiguration(flinkConfig);
        yarnConfig.addResource(HadoopUtils.getHadoopConfiguration(flinkConfig));

        return yarnConfig;
    }

    /**
     * Add additional config entries from the flink config to the yarn config.
     *
     * @param flinkConfig The Flink configuration object.
     * @return The yarn configuration.
     */
    public static YarnConfiguration getYarnConfiguration(
            org.apache.flink.configuration.Configuration flinkConfig) {
        final YarnConfiguration yarnConfig = new YarnConfiguration();

        for (String key : flinkConfig.keySet()) {
            for (String prefix : FLINK_CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String newKey = key.substring("flink.".length());
                    String value = flinkConfig.getString(key, null);
                    yarnConfig.set(newKey, value);
                    LOG.debug(
                            "Adding Flink config entry for {} as {}={} to Yarn config",
                            key,
                            newKey,
                            value);
                }
            }
        }

        return yarnConfig;
    }

    /**
     * Sets the application ACLs for the given ContainerLaunchContext based on the values specified
     * in the given Flink configuration. Only ApplicationAccessType.VIEW_APP and
     * ApplicationAccessType.MODIFY_APP ACLs are set, and only if they are configured in the Flink
     * configuration. If the viewAcls or modifyAcls string contains the WILDCARD_ACL constant, it
     * will replace the entire string with the WILDCARD_ACL. The resulting map is then set as the
     * application acls for the given container launch context.
     *
     * @param amContainer the ContainerLaunchContext to set the ACLs for.
     * @param flinkConfig the Flink configuration to read the ACL values from.
     */
    public static void setAclsFor(
            ContainerLaunchContext amContainer,
            org.apache.flink.configuration.Configuration flinkConfig) {
        Map<ApplicationAccessType, String> acls = new HashMap<>();
        final String viewAcls = flinkConfig.getString(YarnConfigOptions.APPLICATION_VIEW_ACLS);
        final String modifyAcls = flinkConfig.getString(YarnConfigOptions.APPLICATION_MODIFY_ACLS);
        validateAclString(viewAcls);
        validateAclString(modifyAcls);

        if (viewAcls != null && !viewAcls.isEmpty()) {
            acls.put(ApplicationAccessType.VIEW_APP, viewAcls);
        }
        if (modifyAcls != null && !modifyAcls.isEmpty()) {
            acls.put(ApplicationAccessType.MODIFY_APP, modifyAcls);
        }
        if (!acls.isEmpty()) {
            amContainer.setApplicationACLs(acls);
        }
    }

    /* Validates the ACL string to ensure that it is either null or the wildcard ACL. */
    private static void validateAclString(String acl) {
        if (acl != null && acl.contains("*") && !acl.equals("*")) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid wildcard ACL %s. The ACL wildcard does not support regex. The only valid wildcard ACL is '*'.",
                            acl));
        }
    }

    public static Path getPathFromLocalFile(File localFile) {
        return new Path(localFile.toURI());
    }

    public static Path getPathFromLocalFilePathStr(String localPathStr) {
        return getPathFromLocalFile(new File(localPathStr));
    }
}
