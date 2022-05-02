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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.yarn.configuration.YarnResourceManagerDriverConfiguration;
import org.apache.flink.yarn.util.TestUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for various utilities. */
class UtilsTest {
    private static final Logger LOG = LoggerFactory.getLogger(UtilsTest.class);

    @TempDir File temporaryFolder;

    @Test
    void testUberjarLocator() {
        File dir = TestUtils.findFile("..", new TestUtils.RootDirFilenameFilter());
        assertThat(dir).isNotNull();
        assertThat(dir.getName()).endsWith(".jar");
        dir = dir.getParentFile().getParentFile(); // from uberjar to lib to root
        assertThat(dir).exists().isDirectory();
        assertThat(dir.list()).contains("lib", "bin", "conf");
    }

    @Test
    void testCreateTaskExecutorCredentials() throws Exception {
        File root = temporaryFolder;
        File home = new File(root, "home");
        boolean created = home.mkdir();
        assertThat(created).isTrue();

        Configuration flinkConf = new Configuration();
        YarnConfiguration yarnConf = new YarnConfiguration();

        Map<String, String> env = new HashMap<>();
        env.put(YarnConfigKeys.ENV_APP_ID, "foo");
        env.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, home.getAbsolutePath());
        env.put(YarnConfigKeys.ENV_CLIENT_SHIP_FILES, "");
        env.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, "");
        env.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, "foo");
        env.put(
                YarnConfigKeys.FLINK_DIST_JAR,
                new YarnLocalResourceDescriptor(
                                "flink.jar",
                                new Path(root.toURI()),
                                0,
                                System.currentTimeMillis(),
                                LocalResourceVisibility.APPLICATION,
                                LocalResourceType.FILE)
                        .toString());
        env.put(YarnConfigKeys.FLINK_YARN_FILES, "");
        env.put(ApplicationConstants.Environment.PWD.key(), home.getAbsolutePath());
        env = Collections.unmodifiableMap(env);

        final YarnResourceManagerDriverConfiguration yarnResourceManagerDriverConfiguration =
                new YarnResourceManagerDriverConfiguration(env, "localhost", null);

        File credentialFile = temporaryFolder.toPath().resolve("container_tokens").toFile();
        credentialFile.createNewFile();
        final Text amRmTokenKind = AMRMTokenIdentifier.KIND_NAME;
        final Text hdfsDelegationTokenKind = new Text("HDFS_DELEGATION_TOKEN");
        final Text amRmTokenService = new Text("rm-ip:8030");
        final Text hdfsDelegationTokenService = new Text("ha-hdfs:hadoop-namespace");
        Credentials amCredentials = new Credentials();
        amCredentials.addToken(
                amRmTokenService,
                new Token<>(new byte[4], new byte[4], amRmTokenKind, amRmTokenService));
        amCredentials.addToken(
                hdfsDelegationTokenService,
                new Token<>(
                        new byte[4],
                        new byte[4],
                        hdfsDelegationTokenKind,
                        hdfsDelegationTokenService));
        amCredentials.writeTokenStorageFile(
                new org.apache.hadoop.fs.Path(credentialFile.getAbsolutePath()), yarnConf);

        TaskExecutorProcessSpec spec =
                TaskExecutorProcessUtils.newProcessSpecBuilder(flinkConf)
                        .withTotalProcessMemory(MemorySize.parse("1g"))
                        .build();
        ContaineredTaskManagerParameters tmParams =
                new ContaineredTaskManagerParameters(spec, new HashMap<>(1));
        Configuration taskManagerConf = new Configuration();

        String workingDirectory = root.getAbsolutePath();
        Class<?> taskManagerMainClass = YarnTaskExecutorRunner.class;
        ContainerLaunchContext ctx;

        final Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put("HADOOP_TOKEN_FILE_LOCATION", credentialFile.getAbsolutePath());
            CommonTestUtils.setEnv(systemEnv);
            ctx =
                    Utils.createTaskExecutorContext(
                            flinkConf,
                            yarnConf,
                            yarnResourceManagerDriverConfiguration,
                            tmParams,
                            "",
                            workingDirectory,
                            taskManagerMainClass,
                            LOG);
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        Credentials credentials = new Credentials();
        try (DataInputStream dis =
                new DataInputStream(new ByteArrayInputStream(ctx.getTokens().array()))) {
            credentials.readTokenStorageStream(dis);
        }
        Collection<Token<? extends TokenIdentifier>> tokens = credentials.getAllTokens();
        boolean hasHdfsDelegationToken = false;
        boolean hasAmRmToken = false;
        for (Token<? extends TokenIdentifier> token : tokens) {
            if (token.getKind().equals(amRmTokenKind)) {
                hasAmRmToken = true;
            } else if (token.getKind().equals(hdfsDelegationTokenKind)) {
                hasHdfsDelegationToken = true;
            }
        }
        assertThat(hasHdfsDelegationToken).isTrue();
        assertThat(hasAmRmToken).isFalse();
    }
}
