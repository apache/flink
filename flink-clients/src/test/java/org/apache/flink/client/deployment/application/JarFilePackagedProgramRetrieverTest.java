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

import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.TestJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link JarFilePackagedProgramRetriever}. */
public class JarFilePackagedProgramRetrieverTest extends PackagedProgramRetrieverTestBase {

    @Test
    public void testRetrieveFromJarFileWithoutUserLib()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(
                                PROGRAM_ARGUMENTS, CONFIGURATION, testJar)
                        .build();
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, CONFIGURATION);

        assertThat(jobGraph.getUserJars(), containsInAnyOrder(new Path(testJar.toURI())));
        assertThat(jobGraph.getClasspaths().isEmpty(), is(true));
    }

    @Test
    public void testRetrieveFromJarFileWithUserLib()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(
                                PROGRAM_ARGUMENTS, CONFIGURATION, testJar)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .build();
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, CONFIGURATION);

        assertThat(
                jobGraph.getUserJars(),
                containsInAnyOrder(new org.apache.flink.core.fs.Path(testJar.toURI())));
        assertThat(
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_URLS.stream().map(URL::toString).toArray()));
    }

    @Test
    public void testSavepointRestoreSettings()
            throws FlinkException, IOException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(
                                PROGRAM_ARGUMENTS, CONFIGURATION, testJar)
                        .build();
        testSavepointRestoreSettings(retrieverUnderTest);
    }

    @Test
    public void testRetrieveCorrectUserClasspathsWithoutSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(
                                PROGRAM_ARGUMENTS, CONFIGURATION, testJar)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .setJarsOnClasspath(Collections::emptyList)
                        .build();
        testRetrieveCorrectUserClasspathsWithoutSpecifiedEntryClass(retrieverUnderTest);
    }

    @Test
    public void testRetrieveCorrectUserClasspathsWithSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(
                                PROGRAM_ARGUMENTS, CONFIGURATION, testJar)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .setJarsOnClasspath(Collections::emptyList)
                        .build();
        testRetrieveCorrectUserClasspathsWithSpecifiedEntryClass(retrieverUnderTest);
    }

    @Test
    public void testGetPackagedProgramWithConfiguration() throws IOException, FlinkException {
        final Configuration configuration = new Configuration();
        configuration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
        configuration.setBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(
                                PROGRAM_ARGUMENTS, configuration, testJar)
                        .build();
        assertTrue(
                retrieverUnderTest.getPackagedProgram().getUserCodeClassLoader()
                        instanceof FlinkUserCodeClassLoaders.ParentFirstClassLoader);
    }
}
