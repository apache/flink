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
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PackagedProgramDescriptor}. */
public class PackagedProgramDescriptorTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPackagedProgramDescriptor(boolean jarFileIsNull) throws Exception {
        File testJar = new File(CliFrontendTestUtils.getTestJarPath());
        File jarFile = jarFileIsNull ? null : testJar;
        SavepointRestoreSettings savepointSettings = SavepointRestoreSettings.none();
        List<URL> userClassPaths = Collections.singletonList(testJar.toURI().toURL());
        String[] programArgs = {"--input", "test", "--output", "result"};
        String mainClasssName = TEST_JAR_MAIN_CLASS;
        PackagedProgram program =
                PackagedProgram.newBuilder()
                        .setJarFile(jarFile)
                        .setEntryPointClassName(mainClasssName)
                        .setUserClassPaths(userClassPaths)
                        .setArguments(programArgs)
                        .setSavepointRestoreSettings(savepointSettings)
                        .build();
        PackagedProgramDescriptor descriptor = program.getDescriptor();

        assertThat(descriptor.getMainClassName()).isEqualTo(mainClasssName);

        PackagedProgram reconstructedProgram = descriptor.toPackageProgram();

        // Verify the reconstructed program has the expected properties
        assertThat(reconstructedProgram.getUserCodeClassLoader()).isNotNull();
        assertThat(reconstructedProgram.getClasspaths())
                .containsExactly(userClassPaths.toArray(URL[]::new));
        assertThat(reconstructedProgram.getSavepointSettings()).isEqualTo(savepointSettings);
        assertThat(reconstructedProgram.getArguments()).containsExactly(programArgs);
        assertThat(reconstructedProgram.getMainClassName()).isEqualTo(mainClasssName);

        reconstructedProgram.close();
    }
}
