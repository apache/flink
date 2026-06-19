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

package org.apache.flink.client.program;

import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.configuration.ConfigConstants;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link PackagedProgram}. */
class PackagedProgramTest {

    @Test
    void testExtractContainedLibraries(@TempDir java.nio.file.Path temporaryFolder)
            throws Exception {
        String s = "testExtractContainedLibraries";
        byte[] nestedJarContent = s.getBytes(ConfigConstants.DEFAULT_CHARSET);
        File fakeJar = temporaryFolder.resolve("test.jar").toFile();
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(fakeJar))) {
            ZipEntry entry = new ZipEntry("lib/internalTest.jar");
            zos.putNextEntry(entry);
            zos.write(nestedJarContent);
            zos.closeEntry();
        }

        final List<File> files = PackagedProgram.extractContainedLibraries(fakeJar.toURI().toURL());
        assertThat(files)
                .hasSize(1)
                .allSatisfy(
                        f -> assertThat(f).content(ConfigConstants.DEFAULT_CHARSET).isEqualTo(s));
    }

    @Test
    void testNotThrowExceptionWhenJarFileIsNull() throws Exception {
        PackagedProgram.newBuilder()
                .setUserClassPaths(
                        Collections.singletonList(
                                new File(CliFrontendTestUtils.getTestJarPath()).toURI().toURL()))
                .setEntryPointClassName(TEST_JAR_MAIN_CLASS);
    }

    @Test
    void testBuilderThrowExceptionIfjarFileAndEntryPointClassNameAreBothNull() {
        assertThatThrownBy(() -> PackagedProgram.newBuilder().build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
