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

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.runtime.jobmanager.JobManagerProcessUtils.createDefaultJobManagerProcessSpec;
import static org.apache.flink.yarn.Utils.getPathFromLocalFile;
import static org.apache.flink.yarn.configuration.YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link YarnClusterDescriptor}. */
class YarnClusterDescriptorTest {

    private static final int YARN_MAX_VCORES = 16;
    private static YarnConfiguration yarnConfiguration;

    private static YarnClient yarnClient;

    private final ClusterSpecification clusterSpecification =
            new ClusterSpecification.ClusterSpecificationBuilder()
                    .setSlotsPerTaskManager(Integer.MAX_VALUE)
                    .createClusterSpecification();
    private final ApplicationConfiguration appConfig =
            new ApplicationConfiguration(new String[0], null);

    @TempDir java.nio.file.Path temporaryFolder;

    private File flinkJar;

    @BeforeAll
    static void setupClass() {
        yarnConfiguration = new YarnConfiguration();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
    }

    @BeforeEach
    void beforeTest() throws IOException {
        flinkJar = Files.createTempFile(temporaryFolder, "flink", ".jar").toFile();
    }

    @AfterAll
    static void tearDownClass() {
        yarnClient.stop();
    }

    @Test
    void testFailIfTaskSlotsHigherThanMaxVcores() throws ClusterDeploymentException {
        final Configuration flinkConfiguration = new Configuration();

        YarnClusterDescriptor clusterDescriptor = createYarnClusterDescriptor(flinkConfiguration);

        clusterDescriptor.setLocalJarPath(new Path(flinkJar.getPath()));

        try {
            clusterDescriptor.deploySessionCluster(clusterSpecification);

            fail("The deploy call should have failed.");
        } catch (ClusterDeploymentException e) {
            // we expect the cause to be an IllegalConfigurationException
            if (!(e.getCause() instanceof IllegalConfigurationException)) {
                throw e;
            }
        } finally {
            clusterDescriptor.close();
        }
    }

    @Test
    void testConfigOverwrite() throws ClusterDeploymentException {
        Configuration configuration = new Configuration();
        // overwrite vcores in config
        configuration.setInteger(YarnConfigOptions.VCORES, Integer.MAX_VALUE);

        YarnClusterDescriptor clusterDescriptor = createYarnClusterDescriptor(configuration);

        clusterDescriptor.setLocalJarPath(new Path(flinkJar.getPath()));

        // configure slots
        ClusterSpecification clusterSpecification =
                new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();

        try {
            clusterDescriptor.deploySessionCluster(clusterSpecification);

            fail("The deploy call should have failed.");
        } catch (ClusterDeploymentException e) {
            // we expect the cause to be an IllegalConfigurationException
            if (!(e.getCause() instanceof IllegalConfigurationException)) {
                throw e;
            }
        } finally {
            clusterDescriptor.close();
        }
    }

    @Test
    void testSetupApplicationMasterContainer() {
        Configuration cfg = new Configuration();
        YarnClusterDescriptor clusterDescriptor = createYarnClusterDescriptor(cfg);

        final JobManagerProcessSpec jobManagerProcessSpec =
                createDefaultJobManagerProcessSpec(1024);
        final String java = "$JAVA_HOME/bin/java";
        final String jvmmem =
                JobManagerProcessUtils.generateJvmParametersStr(jobManagerProcessSpec, cfg);
        final String dynamicParameters =
                JobManagerProcessUtils.generateDynamicConfigsStr(jobManagerProcessSpec);
        final String jvmOpts = "-Djvm"; // if set
        final String jmJvmOpts = "-DjmJvm"; // if set
        final String krb5 = "-Djava.security.krb5.conf=krb5.conf";
        final String logfile =
                "-Dlog.file=\""
                        + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                        + "/jobmanager.log\""; // if set
        final String logback =
                "-Dlogback.configurationFile=file:"
                        + YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME; // if set
        final String log4j =
                "-Dlog4j.configuration=file:"
                        + YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME
                        + " -Dlog4j.configurationFile=file:"
                        + YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME; // if set
        final String mainClass = clusterDescriptor.getYarnSessionClusterEntrypoint();
        final String redirects =
                "1> "
                        + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                        + "/jobmanager.out "
                        + "2> "
                        + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                        + "/jobmanager.err";

        try {
            // no logging, with/out krb5
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, false, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, true, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    krb5,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            // logback only, with/out krb5
            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, false, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    logfile,
                                    logback,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, true, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    krb5,
                                    logfile,
                                    logback,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            // log4j, with/out krb5
            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, false, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    logfile,
                                    log4j,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, true, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    krb5,
                                    logfile,
                                    log4j,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            // logback, with/out krb5
            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, false, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    logfile,
                                    logback,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, true, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    krb5,
                                    logfile,
                                    logback,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            // logback, with/out krb5, different JVM opts
            // IMPORTANT: Be aware that we are using side effects here to modify the created
            // YarnClusterDescriptor,
            // because we have a reference to the ClusterDescriptor's configuration which we modify
            // continuously
            cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, false, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    jvmOpts,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    logfile,
                                    logback,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, true, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    jvmOpts,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    krb5,
                                    logfile,
                                    logback,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            // log4j, with/out krb5, different JVM opts
            // IMPORTANT: Be aware that we are using side effects here to modify the created
            // YarnClusterDescriptor
            cfg.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, jmJvmOpts);
            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, false, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    jvmOpts,
                                    jmJvmOpts,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    logfile,
                                    log4j,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME);
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, true, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    jvmmem,
                                    jvmOpts,
                                    jmJvmOpts,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    krb5,
                                    logfile,
                                    log4j,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));

            // now try some configurations with different yarn.container-start-command-template
            // IMPORTANT: Be aware that we are using side effects here to modify the created
            // YarnClusterDescriptor
            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            cfg.setString(
                    ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
                    "%java% 1 %jvmmem% 2 %jvmopts% 3 %logging% 4 %class% 5 %args% 6 %redirects%");
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, true, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    "1",
                                    jvmmem,
                                    "2",
                                    jvmOpts,
                                    jmJvmOpts,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    krb5,
                                    "3",
                                    logfile,
                                    logback,
                                    "4",
                                    mainClass,
                                    "5",
                                    dynamicParameters,
                                    "6",
                                    redirects));

            cfg.set(
                    YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                    YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME);
            cfg.setString(
                    ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
                    "%java% %logging% %jvmopts% %jvmmem% %class% %args% %redirects%");
            // IMPORTANT: Be aware that we are using side effects here to modify the created
            // YarnClusterDescriptor
            assertThat(
                            clusterDescriptor
                                    .setupApplicationMasterContainer(
                                            mainClass, true, jobManagerProcessSpec)
                                    .getCommands()
                                    .get(0))
                    .isEqualTo(
                            String.join(
                                    " ",
                                    java,
                                    logfile,
                                    logback,
                                    jvmOpts,
                                    jmJvmOpts,
                                    YarnClusterDescriptor.IGNORE_UNRECOGNIZED_VM_OPTIONS,
                                    krb5,
                                    jvmmem,
                                    mainClass,
                                    dynamicParameters,
                                    redirects));
        } finally {
            clusterDescriptor.close();
        }
    }

    /** Tests to ship files through the {@code YarnClusterDescriptor.addShipFiles}. */
    @Test
    void testExplicitFileShipping() throws Exception {
        try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
            descriptor.setLocalJarPath(new Path("/path/to/flink.jar"));

            File libFile = Files.createTempFile(temporaryFolder, "libFile", ".jar").toFile();
            File libFolder =
                    Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString())
                            .toFile();

            assertThat(descriptor.getShipFiles())
                    .doesNotContain(getPathFromLocalFile(libFile), getPathFromLocalFile(libFolder));

            List<Path> shipFiles = new ArrayList<>();
            shipFiles.add(getPathFromLocalFile(libFile));
            shipFiles.add(getPathFromLocalFile(libFolder));

            descriptor.addShipFiles(shipFiles);

            assertThat(descriptor.getShipFiles())
                    .contains(getPathFromLocalFile(libFile), getPathFromLocalFile(libFolder));

            // only execute part of the deployment to test for shipped files
            Set<Path> effectiveShipFiles = new HashSet<>();
            descriptor.addLibFoldersToShipFiles(effectiveShipFiles);

            assertThat(effectiveShipFiles).isEmpty();
            assertThat(descriptor.getShipFiles())
                    .hasSize(2)
                    .contains(getPathFromLocalFile(libFile), getPathFromLocalFile(libFolder));
        }
    }

    /** Tests to ship files through the {@link YarnConfigOptions#SHIP_FILES}. */
    @Test
    void testShipFiles() throws IOException {
        String hdfsDir = "hdfs:///flink/hdfs_dir";
        String hdfsFile = "hdfs:///flink/hdfs_file";
        File libFile = Files.createTempFile(temporaryFolder, "libFile", ".jar").toFile();
        File libFolder =
                Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString()).toFile();
        final org.apache.hadoop.conf.Configuration hdConf =
                new org.apache.hadoop.conf.Configuration();
        hdConf.set(
                MiniDFSCluster.HDFS_MINIDFS_BASEDIR, temporaryFolder.toAbsolutePath().toString());
        try (final MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(hdConf).build()) {
            final org.apache.hadoop.fs.Path hdfsRootPath =
                    new org.apache.hadoop.fs.Path(hdfsCluster.getURI());
            hdfsCluster.getFileSystem().mkdirs(new org.apache.hadoop.fs.Path(hdfsDir));
            hdfsCluster.getFileSystem().createNewFile(new org.apache.hadoop.fs.Path(hdfsFile));

            Configuration flinkConfiguration = new Configuration();
            flinkConfiguration.set(
                    YarnConfigOptions.SHIP_FILES,
                    Arrays.asList(
                            libFile.getAbsolutePath(),
                            libFolder.getAbsolutePath(),
                            hdfsDir,
                            hdfsFile));
            final YarnConfiguration yarnConfig = new YarnConfiguration();
            yarnConfig.set(
                    CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, hdfsRootPath.toString());
            YarnClusterDescriptor descriptor =
                    createYarnClusterDescriptor(flinkConfiguration, yarnConfig);
            assertThat(descriptor.getShipFiles())
                    .containsExactly(
                            getPathFromLocalFile(libFile),
                            getPathFromLocalFile(libFolder),
                            new Path(hdfsDir),
                            new Path(hdfsFile));
        }
    }

    @Test
    void testEnvironmentLibShipping() throws Exception {
        testEnvironmentDirectoryShipping(ConfigConstants.ENV_FLINK_LIB_DIR, false);
    }

    @Test
    void testEnvironmentPluginsShipping() throws Exception {
        testEnvironmentDirectoryShipping(ConfigConstants.ENV_FLINK_PLUGINS_DIR, true);
    }

    private void testEnvironmentDirectoryShipping(String environmentVariable, boolean onlyShip)
            throws Exception {
        try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
            File libFolder =
                    Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString())
                            .toFile();
            File libFile = new File(libFolder, "libFile.jar");
            assertThat(libFile.createNewFile()).isTrue();

            Set<Path> effectiveShipFiles = new HashSet<>();

            final Map<String, String> oldEnv = System.getenv();
            try {
                Map<String, String> env = new HashMap<>(1);
                env.put(environmentVariable, libFolder.getAbsolutePath());
                CommonTestUtils.setEnv(env);
                // only execute part of the deployment to test for shipped files
                if (onlyShip) {
                    descriptor.addPluginsFoldersToShipFiles(effectiveShipFiles);
                } else {
                    descriptor.addLibFoldersToShipFiles(effectiveShipFiles);
                }
            } finally {
                CommonTestUtils.setEnv(oldEnv);
            }

            // only add the ship the folder, not the contents
            assertThat(effectiveShipFiles)
                    .doesNotContain(getPathFromLocalFile(libFile))
                    .contains(getPathFromLocalFile(libFolder));
            assertThat(descriptor.getShipFiles())
                    .doesNotContain(getPathFromLocalFile(libFile), getPathFromLocalFile(libFolder));
        }
    }

    @Test
    void testEnvironmentEmptyPluginsShipping() {
        try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
            File pluginsFolder =
                    Paths.get(
                                    temporaryFolder.toFile().getAbsolutePath(),
                                    "s0m3_p4th_th4t_sh0uld_n0t_3x1sts")
                            .toFile();
            Set<Path> effectiveShipFiles = new HashSet<>();

            final Map<String, String> oldEnv = System.getenv();
            try {
                Map<String, String> env = new HashMap<>(1);
                env.put(ConfigConstants.ENV_FLINK_PLUGINS_DIR, pluginsFolder.getAbsolutePath());
                CommonTestUtils.setEnv(env);
                // only execute part of the deployment to test for shipped files
                descriptor.addPluginsFoldersToShipFiles(effectiveShipFiles);
            } finally {
                CommonTestUtils.setEnv(oldEnv);
            }

            assertThat(effectiveShipFiles).isEmpty();
        }
    }

    @Test
    void testDisableSystemClassPathIncludeUserJarAndWithIllegalShipDirectoryName() {
        final Configuration configuration = new Configuration();
        configuration.set(CLASSPATH_INCLUDE_USER_JAR, YarnConfigOptions.UserJarInclusion.DISABLED);

        final YarnClusterDescriptor yarnClusterDescriptor =
                createYarnClusterDescriptor(configuration);
        java.nio.file.Path p = temporaryFolder.resolve(ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);
        p.toFile().mkdir();
        assertThatThrownBy(
                        () ->
                                yarnClusterDescriptor.addShipFiles(
                                        Collections.singletonList(new Path(p.toString()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("User-shipped directories configured via :");
    }

    /** Tests that the usrlib will be automatically shipped. */
    @Test
    void testShipUsrLib() throws IOException {
        final Map<String, String> oldEnv = System.getenv();
        final Map<String, String> env = new HashMap<>(1);
        final File homeFolder =
                Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString()).toFile();
        final File libFolder = new File(homeFolder.getAbsolutePath(), "lib");
        assertThat(libFolder.createNewFile()).isTrue();
        final File usrLibFolder =
                new File(homeFolder.getAbsolutePath(), ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);
        assertThat(usrLibFolder.mkdirs()).isTrue();
        final File usrLibFile = new File(usrLibFolder, "usrLibFile.jar");
        assertThat(usrLibFile.createNewFile()).isTrue();
        env.put(ConfigConstants.ENV_FLINK_LIB_DIR, libFolder.getAbsolutePath());
        CommonTestUtils.setEnv(env);

        try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
            final Set<File> effectiveShipFiles = new HashSet<>();
            descriptor.addUsrLibFolderToShipFiles(effectiveShipFiles);
            assertThat(effectiveShipFiles).containsExactlyInAnyOrder(usrLibFolder);
        } finally {
            CommonTestUtils.setEnv(oldEnv);
        }
    }

    /** Tests that the {@code YarnConfigOptions.SHIP_ARCHIVES} only supports archive files. */
    @Test
    void testShipArchives() throws IOException {
        final File homeFolder =
                Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString()).toFile();
        File dir1 = new File(homeFolder.getPath(), "dir1");
        File file1 = new File(homeFolder.getPath(), "file1");
        File archive1 = new File(homeFolder.getPath(), "archive1.zip");
        File archive2 = new File(homeFolder.getPath(), "archive2.zip");
        assertThat(dir1.mkdirs()).isTrue();
        assertThat(file1.createNewFile()).isTrue();
        assertThat(archive1.createNewFile()).isTrue();
        assertThat(archive2.createNewFile()).isTrue();

        Configuration flinkConfiguration = new Configuration();
        flinkConfiguration.set(
                YarnConfigOptions.SHIP_ARCHIVES,
                Arrays.asList(dir1.getAbsolutePath(), archive1.getAbsolutePath()));
        assertThrows(
                "Directories or non-archive files are included.",
                IllegalArgumentException.class,
                () -> createYarnClusterDescriptor(flinkConfiguration));

        flinkConfiguration.set(
                YarnConfigOptions.SHIP_ARCHIVES,
                Arrays.asList(file1.getAbsolutePath(), archive1.getAbsolutePath()));
        assertThrows(
                "Directories or non-archive files are included.",
                IllegalArgumentException.class,
                () -> createYarnClusterDescriptor(flinkConfiguration));

        flinkConfiguration.set(
                YarnConfigOptions.SHIP_ARCHIVES,
                Arrays.asList(archive1.getAbsolutePath(), archive2.getAbsolutePath()));
        createYarnClusterDescriptor(flinkConfiguration);

        String archive3 = "hdfs:///flink/archive3.zip";
        final org.apache.hadoop.conf.Configuration hdConf =
                new org.apache.hadoop.conf.Configuration();
        hdConf.set(
                MiniDFSCluster.HDFS_MINIDFS_BASEDIR, temporaryFolder.toAbsolutePath().toString());
        try (final MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(hdConf).build()) {
            final org.apache.hadoop.fs.Path hdfsRootPath =
                    new org.apache.hadoop.fs.Path(hdfsCluster.getURI());
            hdfsCluster.getFileSystem().createNewFile(new org.apache.hadoop.fs.Path(archive3));

            flinkConfiguration.set(
                    YarnConfigOptions.SHIP_ARCHIVES,
                    Arrays.asList(archive1.getAbsolutePath(), archive3));
            final YarnConfiguration yarnConfig = new YarnConfiguration();
            yarnConfig.set(
                    CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, hdfsRootPath.toString());

            YarnClusterDescriptor descriptor =
                    createYarnClusterDescriptor(flinkConfiguration, yarnConfig);
            assertThat(descriptor.getShipArchives())
                    .containsExactly(getPathFromLocalFile(archive1), new Path(archive3));
        }
    }

    /** Tests that the YarnClient is only shut down if it is not shared. */
    @Test
    void testYarnClientShutDown() {
        YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor();

        yarnClusterDescriptor.close();

        assertThat(yarnClient.isInState(Service.STATE.STARTED)).isTrue();

        final YarnClient closableYarnClient = YarnClient.createYarnClient();
        closableYarnClient.init(yarnConfiguration);
        closableYarnClient.start();

        yarnClusterDescriptor =
                YarnTestUtils.createClusterDescriptorWithLogging(
                        temporaryFolder.toFile().getAbsolutePath(),
                        new Configuration(),
                        yarnConfiguration,
                        closableYarnClient,
                        false);

        yarnClusterDescriptor.close();

        assertThat(closableYarnClient.isInState(Service.STATE.STOPPED)).isTrue();
    }

    @Test
    void testDeployApplicationClusterWithDeploymentTargetNotCorrectlySet() {
        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("file:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName());
        try (final YarnClusterDescriptor yarnClusterDescriptor =
                createYarnClusterDescriptor(flinkConfig)) {
            assertThrows(
                    "Expected deployment.target=yarn-application",
                    ClusterDeploymentException.class,
                    () ->
                            yarnClusterDescriptor.deployApplicationCluster(
                                    clusterSpecification, appConfig));
        }
    }

    @Test
    void testGetStagingDirWithoutSpecifyingStagingDir() throws IOException {
        try (final YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor()) {
            YarnConfiguration yarnConfig = new YarnConfiguration();
            yarnConfig.set("fs.defaultFS", "file://tmp");
            FileSystem defaultFileSystem = FileSystem.get(yarnConfig);
            Path stagingDir = yarnClusterDescriptor.getStagingDir(defaultFileSystem);
            assertThat(defaultFileSystem.getScheme()).isEqualTo("file");
            assertThat(stagingDir.getFileSystem(yarnConfig).getScheme()).isEqualTo("file");
        }
    }

    @Test
    void testGetStagingDirWithSpecifyingStagingDir() throws IOException {
        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(YarnConfigOptions.STAGING_DIRECTORY, "file:///tmp/path1");
        try (final YarnClusterDescriptor yarnClusterDescriptor =
                createYarnClusterDescriptor(flinkConfig)) {
            YarnConfiguration yarnConfig = new YarnConfiguration();
            yarnConfig.set("fs.defaultFS", "viewfs://hadoop-ns01");
            yarnConfig.set("fs.viewfs.mounttable.hadoop-ns01.link./tmp", "file://tmp");
            FileSystem defaultFileSystem = FileSystem.get(yarnConfig);

            Path stagingDir = yarnClusterDescriptor.getStagingDir(defaultFileSystem);

            assertThat(defaultFileSystem.getScheme()).isEqualTo("viewfs");
            assertThat(stagingDir.getFileSystem(yarnConfig).getScheme()).isEqualTo("file");
        }
    }

    @Test
    void testDeployApplicationClusterWithMultipleJarsSet() {
        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(
                PipelineOptions.JARS,
                Arrays.asList("local:///path/of/user.jar", "local:///user2.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        try (final YarnClusterDescriptor yarnClusterDescriptor =
                createYarnClusterDescriptor(flinkConfig)) {
            assertThrows(
                    "Should only have one jar",
                    IllegalArgumentException.class,
                    () ->
                            yarnClusterDescriptor.deployApplicationCluster(
                                    clusterSpecification, appConfig));
        }
    }

    private YarnClusterDescriptor createYarnClusterDescriptor() {
        return createYarnClusterDescriptor(new Configuration());
    }

    private YarnClusterDescriptor createYarnClusterDescriptor(Configuration configuration) {
        YarnTestUtils.configureLogFile(configuration, temporaryFolder.toFile().getAbsolutePath());

        return this.createYarnClusterDescriptor(configuration, yarnConfiguration);
    }

    private YarnClusterDescriptor createYarnClusterDescriptor(
            Configuration configuration, YarnConfiguration yarnConfiguration) {
        YarnTestUtils.configureLogFile(configuration, temporaryFolder.toFile().getAbsolutePath());

        return YarnClusterDescriptorBuilder.newBuilder(yarnClient, true)
                .setFlinkConfiguration(configuration)
                .setYarnConfiguration(yarnConfiguration)
                .setYarnClusterInformationRetriever(() -> YARN_MAX_VCORES)
                .build();
    }

    @Test
    public void testGenerateApplicationMasterEnv(@TempDir File flinkHomeDir) throws IOException {
        final String fakeLocalFlinkJar = "./lib/flink_dist.jar";
        final String fakeClassPath = fakeLocalFlinkJar + ":./usrlib/user.jar";
        final ApplicationId appId = ApplicationId.newInstance(0, 0);
        final Map<String, String> masterEnv =
                getTestMasterEnv(
                        new Configuration(), flinkHomeDir, fakeClassPath, fakeLocalFlinkJar, appId);

        assertThat(masterEnv)
                .containsEntry(ConfigConstants.ENV_FLINK_LIB_DIR, "./lib")
                .containsEntry(YarnConfigKeys.ENV_APP_ID, appId.toString())
                .containsEntry(
                        YarnConfigKeys.FLINK_YARN_FILES,
                        YarnApplicationFileUploader.getApplicationDirPath(
                                        new Path(flinkHomeDir.getPath()), appId)
                                .toString())
                .containsEntry(YarnConfigKeys.ENV_FLINK_CLASSPATH, fakeClassPath);
        assertThat(masterEnv.get(YarnConfigKeys.ENV_CLIENT_SHIP_FILES)).isEmpty();
        assertThat(masterEnv)
                .containsEntry(YarnConfigKeys.FLINK_DIST_JAR, fakeLocalFlinkJar)
                .containsEntry(YarnConfigKeys.ENV_CLIENT_HOME_DIR, flinkHomeDir.getPath());
    }

    @Test
    public void testEnvFlinkLibDirVarNotOverriddenByContainerEnv(@TempDir File tmpDir)
            throws IOException {
        final Configuration flinkConfig = new Configuration();
        flinkConfig.setString(
                ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX
                        + ConfigConstants.ENV_FLINK_LIB_DIR,
                "fake_path");
        final Map<String, String> masterEnv =
                getTestMasterEnv(
                        flinkConfig,
                        tmpDir,
                        "",
                        "./lib/flink_dist.jar",
                        ApplicationId.newInstance(0, 0));
        assertThat(masterEnv).containsEntry(ConfigConstants.ENV_FLINK_LIB_DIR, "./lib");
    }

    private Map<String, String> getTestMasterEnv(
            Configuration flinkConfig,
            File flinkHomeDir,
            String fakeClassPath,
            String fakeLocalFlinkJar,
            ApplicationId appId)
            throws IOException {
        try (final YarnClusterDescriptor yarnClusterDescriptor =
                createYarnClusterDescriptor(flinkConfig)) {
            final YarnApplicationFileUploader yarnApplicationFileUploader =
                    YarnApplicationFileUploader.from(
                            FileSystem.get(new YarnConfiguration()),
                            new Path(flinkHomeDir.getPath()),
                            new ArrayList<>(),
                            appId,
                            DFSConfigKeys.DFS_REPLICATION_DEFAULT);
            return yarnClusterDescriptor.generateApplicationMasterEnv(
                    yarnApplicationFileUploader,
                    fakeClassPath,
                    fakeLocalFlinkJar,
                    appId.toString());
        }
    }
}
