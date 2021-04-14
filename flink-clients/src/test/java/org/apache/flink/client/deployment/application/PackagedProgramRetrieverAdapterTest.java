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

package org.apache.flink.client.deployment.application;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.testjar.TestJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.apache.flink.client.program.PackagedProgramUtils.PYTHON_DRIVER_CLASS_NAME;
import static org.apache.flink.client.program.PackagedProgramUtils.PYTHON_GATEWAY_CLASS_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link PackagedProgramRetrieverAdapter}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PackagedProgramUtils.class)
public class PackagedProgramRetrieverAdapterTest extends TestLogger {

    private static final String[] PROGRAM_ARGUMENTS = {"--arg", "suffix"};
    private static final Configuration CONFIGURATION = new Configuration();

    @Test
    public void testBuildClasspathPackagedProgramRetriever() throws IOException, FlinkException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, CONFIGURATION)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .build();
        assertTrue(retrieverUnderTest instanceof ClasspathPackagedProgramRetriever);

        final PackagedProgram packagedProgram = retrieverUnderTest.getPackagedProgram();
        assertArrayEquals(PROGRAM_ARGUMENTS, packagedProgram.getArguments());
        assertEquals(TestJob.class.getCanonicalName(), packagedProgram.getMainClassName());
    }

    @Test
    public void testBuildJarFilePackagedProgramRetriever() throws IOException, FlinkException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(
                                PROGRAM_ARGUMENTS, CONFIGURATION, TestJob.getTestJobJar())
                        .build();
        assertTrue(retrieverUnderTest instanceof JarFilePackagedProgramRetriever);

        final PackagedProgram packagedProgram = retrieverUnderTest.getPackagedProgram();
        assertArrayEquals(PROGRAM_ARGUMENTS, packagedProgram.getArguments());
        assertEquals(
                TestJob.getTestJobJar().getAbsoluteFile().toURI().toURL(),
                packagedProgram.getJobJarAndDependencies().get(0));
    }

    @Test
    public void testBuildPythonBasedPackagedProgramRetriever() throws IOException, FlinkException {
        PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, CONFIGURATION)
                        .setJobClassName(PYTHON_GATEWAY_CLASS_NAME)
                        .build();
        assertTrue(retrieverUnderTest instanceof PythonBasedPackagedProgramRetriever);

        retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, CONFIGURATION)
                        .setJobClassName(PYTHON_DRIVER_CLASS_NAME)
                        .build();
        assertTrue(retrieverUnderTest instanceof PythonBasedPackagedProgramRetriever);

        final String[] pythonArguments = {"-py", "test.py"};
        retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(pythonArguments, CONFIGURATION).build();
        assertTrue(retrieverUnderTest instanceof PythonBasedPackagedProgramRetriever);

        PowerMockito.mockStatic(PackagedProgramUtils.class);
        PowerMockito.when(PackagedProgramUtils.getPythonJar())
                .thenReturn(TestJob.getTestJobJar().getAbsoluteFile().toURI().toURL());

        final PackagedProgram packagedProgram = retrieverUnderTest.getPackagedProgram();
        assertArrayEquals(pythonArguments, packagedProgram.getArguments());
    }
}
