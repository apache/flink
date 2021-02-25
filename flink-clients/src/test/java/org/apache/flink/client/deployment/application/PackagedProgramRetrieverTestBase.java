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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.TestJob;
import org.apache.flink.client.testjar.TestJobInfo;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Abstract test base for the {@link PackagedProgramRetriever}. */
public abstract class PackagedProgramRetrieverTestBase extends TestLogger {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule public static final TemporaryFolder JOB_DIRS = new TemporaryFolder();

    static final String[] PROGRAM_ARGUMENTS = {"--arg", "suffix"};

    static final String[] PYTHON_ARGUMENTS = {"-py", "test.py"};

    static final Configuration CONFIGURATION = new Configuration();

    /*
     * The directory structure used to test
     *
     * userDirHasEntryClass/
     *                    |_jarWithEntryClass
     *                    |_jarWithoutEntryClass
     *                    |_textFile
     *
     * userDirHasNotEntryClass/
     *                       |_jarWithoutEntryClass
     *                       |_textFile
     */

    static final Collection<URL> EXPECTED_URLS = new ArrayList<>();

    static File userDirHasEntryClass;

    static File userDirHasNotEntryClass;

    static File testJar;

    @BeforeClass
    public static void init() throws IOException {
        final String textFileName = "test.txt";
        final String userDirHasEntryClassName = "_test_user_dir_has_entry_class";
        final String userDirHasNotEntryClassName = "_test_user_dir_has_not_entry_class";

        userDirHasEntryClass = JOB_DIRS.newFolder(userDirHasEntryClassName);
        userDirHasNotEntryClass = JOB_DIRS.newFolder(userDirHasNotEntryClassName);
        testJar = TestJob.getTestJobJar();

        final Path userJarPath =
                userDirHasEntryClass.toPath().resolve(TestJobInfo.JOB_JAR_PATH.toFile().getName());
        final Path userLibJarPath =
                userDirHasEntryClass
                        .toPath()
                        .resolve(TestJobInfo.JOB_LIB_JAR_PATH.toFile().getName());

        // create files
        Files.copy(TestJobInfo.JOB_JAR_PATH, userJarPath);
        Files.copy(TestJobInfo.JOB_LIB_JAR_PATH, userLibJarPath);

        Files.createFile(userDirHasEntryClass.toPath().resolve(textFileName));
        Files.copy(
                TestJobInfo.JOB_LIB_JAR_PATH,
                userDirHasNotEntryClass
                        .toPath()
                        .resolve(TestJobInfo.JOB_LIB_JAR_PATH.toFile().getName()));

        Files.createFile(userDirHasNotEntryClass.toPath().resolve(textFileName));

        final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
        Stream.of(userJarPath, userLibJarPath)
                .map(path -> FileUtils.relativizePath(workingDirectory, path))
                .map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
                .forEach(EXPECTED_URLS::add);
    }

    @AfterClass
    public static void clean() {
        if (!EXPECTED_URLS.isEmpty()) {
            EXPECTED_URLS.clear();
        }
    }

    protected JobGraph retrieveJobGraph(
            PackagedProgramRetriever retrieverUnderTest, Configuration configuration)
            throws FlinkException, ProgramInvocationException, MalformedURLException {
        final PackagedProgram packagedProgram = retrieverUnderTest.getPackagedProgram();

        final int defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.JARS,
                packagedProgram.getJobJarAndDependencies(),
                URL::toString);
        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.CLASSPATHS,
                packagedProgram.getClasspaths(),
                URL::toString);

        final Pipeline pipeline =
                PackagedProgramUtils.getPipelineFromProgram(
                        packagedProgram, configuration, defaultParallelism, false);
        return PipelineExecutorUtils.getJobGraph(pipeline, configuration);
    }

    protected void testSavepointRestoreSettings(PackagedProgramRetriever retrieverUnderTest)
            throws FlinkException, IOException, ProgramInvocationException {
        final Configuration configuration = new Configuration();
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath("foobar", true);
        final JobID jobId = new JobID();

        configuration.setString(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        SavepointRestoreSettings.toConfiguration(savepointRestoreSettings, configuration);

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, configuration);

        assertThat(jobGraph.getSavepointRestoreSettings(), is(equalTo(savepointRestoreSettings)));
        assertEquals(jobGraph.getJobID(), jobId);
    }

    protected void testRetrieveCorrectUserClassPathsWithoutSpecifiedEntryClass(
            PackagedProgramRetriever retrieverUnderTest)
            throws IOException, FlinkException, ProgramInvocationException {
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, CONFIGURATION);

        assertThat(
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_URLS.stream().map(URL::toString).toArray()));
    }

    protected void testRetrieveCorrectUserClassPathsWithSpecifiedEntryClass(
            PackagedProgramRetriever retrieverUnderTest)
            throws IOException, FlinkException, ProgramInvocationException {
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, CONFIGURATION);

        assertThat(
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_URLS.stream().map(URL::toString).toArray()));
    }
}
