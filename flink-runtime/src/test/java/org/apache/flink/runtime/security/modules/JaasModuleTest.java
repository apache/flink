/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.security.modules;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.security.SecurityConfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.flink.runtime.security.modules.JaasModule.JAVA_SECURITY_AUTH_LOGIN_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link JaasModule}. */
class JaasModuleTest {

    @BeforeEach
    void setUp() {
        // clear the property
        System.getProperties().remove(JAVA_SECURITY_AUTH_LOGIN_CONFIG);
    }

    @Test
    void testJaasModuleFilePathIfWorkingDirPresent(@TempDir Path tempDir) throws IOException {
        testJaasModuleFilePath(tempDir.toString());
    }

    @Test
    void testJaasModuleFilePathIfWorkingDirNotPresent(@TempDir Path tempDir) throws IOException {
        testJaasModuleFilePath(tempDir.resolve("tmp").toString());
    }

    @Test
    void testJaasModuleFilePathIfWorkingDirIsSymLink(@TempDir Path tempDir) throws IOException {
        Path symlink = createSymLinkFolderStructure(tempDir);
        testJaasModuleFilePath(symlink.toString());
    }

    @Test
    void testJaasModuleFilePathIfWorkingDirNoPresentAndPathContainsSymLink(@TempDir Path tempDir)
            throws IOException {
        Path symlink = createSymLinkFolderStructure(tempDir);
        testJaasModuleFilePath(symlink.resolve("tmp").toString());
    }

    private static Path createSymLinkFolderStructure(Path baseFolder) throws IOException {
        Path actualFolder = Files.createDirectory(baseFolder.resolve("actual_folder"));
        return Files.createSymbolicLink(baseFolder.resolve("symlink"), actualFolder);
    }

    /** Test that the jaas config file is created in the working directory. */
    private static void testJaasModuleFilePath(String workingDir) throws IOException {
        Configuration configuration = new Configuration();
        // set the string for CoreOptions.TMP_DIRS to mock the working directory.
        configuration.set(CoreOptions.TMP_DIRS, workingDir);
        SecurityConfiguration sc = new SecurityConfiguration(configuration);
        JaasModule module = new JaasModule(sc);

        module.install();

        assertJaasFileLocateInRightDirectory(workingDir);
    }

    /**
     * Test that the jaas file will be created in the directory specified by {@link
     * CoreOptions#TMP_DIRS}'s default value if we do not manually specify it.
     */
    @Test
    void testCreateJaasModuleFileInTemporary() throws IOException {
        Configuration configuration = new Configuration();
        SecurityConfiguration sc = new SecurityConfiguration(configuration);
        JaasModule module = new JaasModule(sc);

        module.install();

        assertJaasFileLocateInRightDirectory(CoreOptions.TMP_DIRS.defaultValue());
    }

    private static void assertJaasFileLocateInRightDirectory(String directory) throws IOException {
        String resolvedExpectedPath = new File(directory).toPath().toRealPath().toString();
        String resolvedActualPathWithFile =
                new File(System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG))
                        .toPath()
                        .toRealPath()
                        .toString();
        assertThat(resolvedActualPathWithFile)
                .as("The resolved configured directory does not match the expected resolved one.")
                .startsWith(resolvedExpectedPath);

        assertThat(System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG))
                .as("The configured directory does not match the expected one.")
                .startsWith(directory);
    }
}
