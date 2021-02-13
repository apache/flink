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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link Utils}. */
public class UtilsTest extends TestLogger {

    private static final String YARN_RM_ARBITRARY_SCHEDULER_CLAZZ =
            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler";

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testDeleteApplicationFiles() throws Exception {
        final Path applicationFilesDir = temporaryFolder.newFolder(".flink").toPath();
        Files.createFile(applicationFilesDir.resolve("flink.jar"));
        try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
            assertThat(files.count(), equalTo(1L));
        }
        try (Stream<Path> files = Files.list(applicationFilesDir)) {
            assertThat(files.count(), equalTo(1L));
        }

        Utils.deleteApplicationFiles(applicationFilesDir.toString());
        try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
            assertThat(files.count(), equalTo(0L));
        }
    }

    @Test
    public void testGetUnitResource() {
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
    public void testSharedLibWithNonQualifiedPath() throws Exception {
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
                Utils.getQualifiedRemoteSharedPaths(flinkConfig, yarnConfig);
        assertThat(sharedLibs.size(), is(1));
        assertThat(sharedLibs.get(0).toUri().toString(), is(qualifiedPath));
    }

    @Test
    public void testSharedLibIsNotRemotePathShouldThrowException() throws IOException {
        final String localLib = "file:///flink/sharedLib";
        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(localLib));

        try {
            Utils.getQualifiedRemoteSharedPaths(flinkConfig, new YarnConfiguration());
            fail("We should throw an exception when the shared lib is set to local path.");
        } catch (FlinkException ex) {
            final String msg =
                    "The \""
                            + YarnConfigOptions.PROVIDED_LIB_DIRS.key()
                            + "\" should only "
                            + "contain dirs accessible from all worker nodes";
            assertThat(ex, FlinkMatchers.containsMessage(msg));
        }
    }

    @Test
    public void testGetYarnConfiguration() {
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

        assertEquals(v1, yarnConfig.get(yarnPrefix + k1, null));
        assertEquals(v2, yarnConfig.get(yarnPrefix + k2, null));
        assertTrue(yarnConfig.get(yarnPrefix + k3) == null);
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
        assertThat(unitResource.getMemory(), is(expectedMem));
        assertThat(unitResource.getVirtualCores(), is(expectedVcore));
    }
}
