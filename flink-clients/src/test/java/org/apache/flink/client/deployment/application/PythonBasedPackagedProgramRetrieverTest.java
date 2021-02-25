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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.TestNonRichInputFormat;
import org.apache.flink.api.common.operators.util.TestNonRichOutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.TestJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.types.Nothing;
import org.apache.flink.util.FlinkException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/** Tests for the {@link PythonBasedPackagedProgramRetriever}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PackagedProgramUtils.class)
public class PythonBasedPackagedProgramRetrieverTest extends PackagedProgramRetrieverTestBase {

    @BeforeClass
    public static void init() throws FileNotFoundException {
        testJar = TestJob.getTestJobJar();
    }

    @Test
    public void testRetrieveFromPythonBasedWithoutUserLib()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PYTHON_ARGUMENTS, CONFIGURATION, testJar)
                        .build();

        mockGetPythonJar();
        mockGetPipelineFromProgram();

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, CONFIGURATION);

        assertThat(jobGraph.getUserJars(), containsInAnyOrder(new Path(testJar.toURI())));
        assertThat(jobGraph.getClasspaths().isEmpty(), is(true));
    }

    @Test
    public void testRetrieveFromPythonBasedWithUserLib()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PYTHON_ARGUMENTS, CONFIGURATION, testJar)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .build();

        mockGetPythonJar();
        mockGetPipelineFromProgram();

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, CONFIGURATION);

        assertThat(jobGraph.getUserJars(), containsInAnyOrder(new Path(testJar.toURI())));
        assertThat(
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_URLS.stream().map(URL::toString).toArray()));
    }

    @Test
    public void testSavepointRestoreSettings()
            throws FlinkException, IOException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PYTHON_ARGUMENTS, CONFIGURATION, testJar)
                        .build();

        mockGetPythonJar();
        mockGetPipelineFromProgram();

        testSavepointRestoreSettings(retrieverUnderTest);
    }

    @Test
    public void testRetrieveCorrectUserClassPathsWithoutSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PYTHON_ARGUMENTS, CONFIGURATION, testJar)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .setJarsOnClassPath(Collections::emptyList)
                        .build();

        mockGetPythonJar();
        mockGetPipelineFromProgram();

        testRetrieveCorrectUserClassPathsWithoutSpecifiedEntryClass(retrieverUnderTest);
    }

    @Test
    public void testRetrieveCorrectUserClassPathsWithSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PYTHON_ARGUMENTS, CONFIGURATION, testJar)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .setJarsOnClassPath(Collections::emptyList)
                        .build();

        mockGetPythonJar();
        mockGetPipelineFromProgram();

        testRetrieveCorrectUserClassPathsWithSpecifiedEntryClass(retrieverUnderTest);
    }

    @Test
    public void testGetPackagedProgramWithConfiguration() throws IOException, FlinkException {
        final Configuration configuration = new Configuration();
        configuration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
        configuration.setBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PYTHON_ARGUMENTS, configuration, testJar)
                        .build();

        mockGetPythonJar();

        assertTrue(
                retrieverUnderTest.getPackagedProgram().getUserCodeClassLoader()
                        instanceof FlinkUserCodeClassLoaders.ParentFirstClassLoader);
    }

    private void mockGetPythonJar() throws MalformedURLException {
        mockStatic(PackagedProgramUtils.class);
        when(PackagedProgramUtils.getPythonJar())
                .thenReturn(testJar.getAbsoluteFile().toURI().toURL());
    }

    private void mockGetPipelineFromProgram() throws ProgramInvocationException {
        final GenericDataSourceBase<String, TestNonRichInputFormat> source =
                new GenericDataSourceBase<String, TestNonRichInputFormat>(
                        new TestNonRichInputFormat(),
                        new OperatorInformation<>(BasicTypeInfo.STRING_TYPE_INFO),
                        "testSource");
        final GenericDataSinkBase<String> sink =
                new GenericDataSinkBase<>(
                        new TestNonRichOutputFormat(),
                        new UnaryOperatorInformation<>(
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.getInfoFor(Nothing.class)),
                        "testSink");
        sink.setInput(source);
        final Plan plan = new Plan(sink, "testGetPipelineFromProgram");
        plan.setExecutionConfig(new ExecutionConfig());
        when(PackagedProgramUtils.getPipelineFromProgram(
                        any(PackagedProgram.class),
                        any(Configuration.class),
                        anyInt(),
                        anyBoolean()))
                .thenReturn(plan);
    }
}
