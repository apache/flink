/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.plugin;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/** Tests for the {@link PluginConfig} utility class. */
public class PluginConfigTest extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static Map<String, String> oldEnvVariables;

    @BeforeClass
    public static void setup() {
        oldEnvVariables = System.getenv();
    }

    @After
    public void teardown() {
        if (oldEnvVariables != null) {
            CommonTestUtils.setEnv(oldEnvVariables, true);
        }
    }

    @Test
    public void getPluginsDir_existingDirectory_returnsDirectoryFile() throws IOException {
        final File pluginsDirectory = temporaryFolder.newFolder();
        final Map<String, String> envVariables =
                ImmutableMap.of(
                        ConfigConstants.ENV_FLINK_PLUGINS_DIR, pluginsDirectory.getAbsolutePath());
        CommonTestUtils.setEnv(envVariables);

        assertThat(PluginConfig.getPluginsDir().get(), is(pluginsDirectory));
    }

    @Test
    public void getPluginsDir_nonExistingDirectory_returnsEmpty() {
        final Map<String, String> envVariables =
                ImmutableMap.of(
                        ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                        new File(temporaryFolder.getRoot().getAbsoluteFile(), "should_not_exist")
                                .getAbsolutePath());
        CommonTestUtils.setEnv(envVariables);

        assertFalse(PluginConfig.getPluginsDir().isPresent());
    }
}
