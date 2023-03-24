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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Utils}. */
class UtilsTest {

    private static final String YARN_RM_ARBITRARY_SCHEDULER_CLAZZ =
            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler";

    @Test
    void testDeleteApplicationFiles(@TempDir Path tempDir) throws Exception {
        final Path applicationFilesDir = Files.createTempDirectory(tempDir, ".flink");
        Files.createTempFile(applicationFilesDir, "flink", ".jar");
        try (Stream<Path> files = Files.list(tempDir)) {
            assertThat(files.count()).isEqualTo(1L);
        }
        try (Stream<Path> files = Files.list(applicationFilesDir)) {
            assertThat(files).hasSize(1);
        }

        Utils.deleteApplicationFiles(applicationFilesDir.toString());
        try (Stream<Path> files = Files.list(tempDir.toFile().toPath())) {
            assertThat(files).isEmpty();
        }
    }

    @Test
    void testGetUnitResource() {
        final int minMem = 64;
        final int minVcore = 1;
        final int incMem = 512;
        final int incVcore = 2;
        final int incMemLegacy = 1024;
        final int incVcoreLegacy = 4;

        YarnConfiguration yarnConfig = new YarnConfiguration();
        yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, minMem);
        yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, minVcore);
        yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_MB_LEGACY_KEY, incMemLegacy);
        yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_VCORES_LEGACY_KEY, incVcoreLegacy);

        verifyUnitResourceVariousSchedulers(
                yarnConfig, minMem, minVcore, incMemLegacy, incVcoreLegacy);

        yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_MB_KEY, incMem);
        yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_VCORES_KEY, incVcore);

        verifyUnitResourceVariousSchedulers(yarnConfig, minMem, minVcore, incMem, incVcore);
    }

    @Test
    void testSharedLibWithNonQualifiedPath() throws Exception {
        final String sharedLibPath = "/flink/sharedLib";
        final String nonQualifiedPath = "hdfs://" + sharedLibPath;
        final String defaultFs = "hdfs://localhost:9000";
        final String qualifiedPath = defaultFs + sharedLibPath;

        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(
                YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(nonQualifiedPath));
        final YarnConfiguration yarnConfig = new YarnConfiguration();
        yarnConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultFs);

        final List<org.apache.hadoop.fs.Path> sharedLibs =
                Utils.getQualifiedRemoteProvidedLibDirs(flinkConfig, yarnConfig);
        assertThat(sharedLibs).hasSize(1);
        assertThat(sharedLibs.get(0).toUri()).hasToString(qualifiedPath);
    }

    @Test
    void testSharedLibIsNotRemotePathShouldThrowException() {
        final String localLib = "file:///flink/sharedLib";
        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(localLib));

        final String msg =
                "The \""
                        + YarnConfigOptions.PROVIDED_LIB_DIRS.key()
                        + "\" should only "
                        + "contain dirs accessible from all worker nodes";
        assertThatThrownBy(
                        () ->
                                Utils.getQualifiedRemoteProvidedLibDirs(
                                        flinkConfig, new YarnConfiguration()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(msg);
    }

    @Test
    void testInvalidRemoteUsrLib(@TempDir Path tempDir) throws IOException {
        final String sharedLibPath = "hdfs:///flink/badlib";

        final org.apache.hadoop.conf.Configuration hdConf =
                new org.apache.hadoop.conf.Configuration();
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.toAbsolutePath().toString());
        try (final MiniDFSCluster hdfsCluster = new MiniDFSCluster.Builder(hdConf).build()) {
            final org.apache.hadoop.fs.Path hdfsRootPath =
                    new org.apache.hadoop.fs.Path(hdfsCluster.getURI());
            hdfsCluster.getFileSystem().mkdirs(new org.apache.hadoop.fs.Path(sharedLibPath));

            final Configuration flinkConfig = new Configuration();
            flinkConfig.set(YarnConfigOptions.PROVIDED_USRLIB_DIR, sharedLibPath);
            final YarnConfiguration yarnConfig = new YarnConfiguration();
            yarnConfig.set(
                    CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, hdfsRootPath.toString());
            assertThatThrownBy(
                            () -> Utils.getQualifiedRemoteProvidedUsrLib(flinkConfig, yarnConfig))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                            "The \"%s\" should be named with \"%s\".",
                            YarnConfigOptions.PROVIDED_USRLIB_DIR.key(),
                            ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);
        }
    }

    @Test
    void testSharedUsrLibIsNotRemotePathShouldThrowException(@TempDir Path tempDir) {
        final File localLib = new File(tempDir.toAbsolutePath().toString(), "usrlib");
        assertThat(localLib.mkdirs()).isTrue();
        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(YarnConfigOptions.PROVIDED_USRLIB_DIR, localLib.getAbsolutePath());
        assertThatThrownBy(
                        () ->
                                Utils.getQualifiedRemoteProvidedUsrLib(
                                        flinkConfig, new YarnConfiguration()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The \"%s\" must point to a remote dir "
                                + "which is accessible from all worker nodes.",
                        YarnConfigOptions.PROVIDED_USRLIB_DIR.key());
    }

    @Test
    void testGetYarnConfiguration() {
        final String flinkPrefix = "flink.yarn.";
        final String yarnPrefix = "yarn.";

        final String k1 = "brooklyn";
        final String v1 = "nets";

        final String k2 = "golden.state";
        final String v2 = "warriors";

        final String k3 = "miami";
        final String v3 = "heat";

        final Configuration flinkConfig = new Configuration();
        flinkConfig.setString(flinkPrefix + k1, v1);
        flinkConfig.setString(flinkPrefix + k2, v2);
        flinkConfig.setString(k3, v3);

        final YarnConfiguration yarnConfig = Utils.getYarnConfiguration(flinkConfig);

        assertThat(yarnConfig.get(yarnPrefix + k1, null)).isEqualTo(v1);
        assertThat(yarnConfig.get(yarnPrefix + k2, null)).isEqualTo(v2);
        assertThat(yarnConfig.get(yarnPrefix + k3)).isNull();
    }

    private static void verifyUnitResourceVariousSchedulers(
            YarnConfiguration yarnConfig, int minMem, int minVcore, int incMem, int incVcore) {
        yarnConfig.set(YarnConfiguration.RM_SCHEDULER, Utils.YARN_RM_FAIR_SCHEDULER_CLAZZ);
        verifyUnitResource(yarnConfig, incMem, incVcore);

        yarnConfig.set(YarnConfiguration.RM_SCHEDULER, Utils.YARN_RM_SLS_FAIR_SCHEDULER_CLAZZ);
        verifyUnitResource(yarnConfig, incMem, incVcore);

        yarnConfig.set(YarnConfiguration.RM_SCHEDULER, YARN_RM_ARBITRARY_SCHEDULER_CLAZZ);
        verifyUnitResource(yarnConfig, minMem, minVcore);
    }

    private static void verifyUnitResource(
            YarnConfiguration yarnConfig, int expectedMem, int expectedVcore) {
        final Resource unitResource = Utils.getUnitResource(yarnConfig);
        assertThat(unitResource.getMemorySize()).isEqualTo(expectedMem);
        assertThat(unitResource.getVirtualCores()).isEqualTo(expectedVcore);
    }
}
