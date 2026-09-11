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

package org.apache.flink.table.planner.loader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PlannerModule}. */
class PlannerModuleTest {

    @TempDir private Path tempDir;

    @Test
    void resolveTmpDirectoryHonorsIoTmpDirsFromYaml() throws IOException {
        Path confDir = Files.createDirectory(tempDir.resolve("conf"));
        Path configuredTmp = Files.createDirectory(tempDir.resolve("custom-tmp"));
        Files.write(
                confDir.resolve(GlobalConfiguration.FLINK_CONF_FILENAME),
                ("io.tmp.dirs: " + configuredTmp.toAbsolutePath() + System.lineSeparator())
                        .getBytes());

        Configuration configuration =
                GlobalConfiguration.loadConfiguration(confDir.toAbsolutePath().toString(), null);

        assertThat(PlannerModule.resolveTmpDirectory(configuration))
                .isEqualTo(Paths.get(configuredTmp.toAbsolutePath().toString()));
    }

    @Test
    void resolveTmpDirectoryFallsBackToDefaultWhenIoTmpDirsUnset() {
        assertThat(PlannerModule.resolveTmpDirectory(new Configuration()))
                .isEqualTo(Paths.get(CoreOptions.TMP_DIRS.defaultValue()));
    }
}
