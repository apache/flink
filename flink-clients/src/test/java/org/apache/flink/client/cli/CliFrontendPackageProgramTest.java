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

package org.apache.flink.client.cli;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Collections;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_CLASSLOADERTEST_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getNonJarFilePath;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** Tests for the RUN command with {@link PackagedProgram PackagedPrograms}. */
class CliFrontendPackageProgramTest {

    private CliFrontend frontend;

    @BeforeAll
    static void init() {
        CliFrontendTestUtils.pipeSystemOutToNull();
    }

    @AfterAll
    static void shutdown() {
        CliFrontendTestUtils.restoreSystemOut();
    }

    @BeforeEach
    void setup() {
        final Configuration configuration = new Configuration();
        frontend = new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));
    }

    @Test
    void testNonExistingJarFile() {
        ProgramOptions programOptions = mock(ProgramOptions.class);
        when(programOptions.getJarFilePath()).thenReturn("/some/none/existing/path");

        assertThatThrownBy(() -> frontend.buildProgram(programOptions))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testFileNotJarFile() {
        ProgramOptions programOptions = mock(ProgramOptions.class);
        when(programOptions.getJarFilePath()).thenReturn(getNonJarFilePath());
        when(programOptions.getProgramArgs()).thenReturn(new String[0]);
        when(programOptions.getSavepointRestoreSettings())
                .thenReturn(SavepointRestoreSettings.none());

        assertThatThrownBy(() -> frontend.buildProgram(programOptions))
                .isInstanceOf(ProgramInvocationException.class);
    }

    @Test
    void testVariantWithExplicitJarAndArgumentsOption() throws Exception {
        String[] arguments = {
            "--classpath",
            "file:///tmp/foo",
            "--classpath",
            "file:///tmp/bar",
            "-j",
            getTestJarPath(),
            "-a",
            "--debug",
            "true",
            "arg1",
            "arg2"
        };
        URL[] classpath = new URL[] {new URL("file:///tmp/foo"), new URL("file:///tmp/bar")};
        String[] reducedArguments = new String[] {"--debug", "true", "arg1", "arg2"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(getTestJarPath());
        assertThat(programOptions.getClasspaths().toArray()).isEqualTo(classpath);
        assertThat(programOptions.getProgramArgs()).isEqualTo(reducedArguments);

        PackagedProgram prog = frontend.buildProgram(programOptions);

        assertThat(prog.getArguments()).isEqualTo(reducedArguments);
        assertThat(prog.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
    }

    @Test
    void testVariantWithExplicitJarAndNoArgumentsOption() throws Exception {
        String[] arguments = {
            "--classpath",
            "file:///tmp/foo",
            "--classpath",
            "file:///tmp/bar",
            "-j",
            getTestJarPath(),
            "--debug",
            "true",
            "arg1",
            "arg2"
        };
        URL[] classpath = new URL[] {new URL("file:///tmp/foo"), new URL("file:///tmp/bar")};
        String[] reducedArguments = new String[] {"--debug", "true", "arg1", "arg2"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(getTestJarPath());
        assertThat(programOptions.getClasspaths().toArray()).isEqualTo(classpath);
        assertThat(programOptions.getProgramArgs()).isEqualTo(reducedArguments);

        PackagedProgram prog = frontend.buildProgram(programOptions);

        assertThat(prog.getArguments()).isEqualTo(reducedArguments);
        assertThat(prog.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
    }

    @Test
    void testValidVariantWithNoJarAndNoArgumentsOption() throws Exception {
        String[] arguments = {
            "--classpath",
            "file:///tmp/foo",
            "--classpath",
            "file:///tmp/bar",
            getTestJarPath(),
            "--debug",
            "true",
            "arg1",
            "arg2"
        };
        URL[] classpath = new URL[] {new URL("file:///tmp/foo"), new URL("file:///tmp/bar")};
        String[] reducedArguments = {"--debug", "true", "arg1", "arg2"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(getTestJarPath());
        assertThat(programOptions.getClasspaths().toArray()).isEqualTo(classpath);
        assertThat(programOptions.getProgramArgs()).isEqualTo(reducedArguments);

        PackagedProgram prog = frontend.buildProgram(programOptions);

        assertThat(prog.getArguments()).isEqualTo(reducedArguments);
        assertThat(prog.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
    }

    @Test
    void testNoJarNoArgumentsAtAll() {
        assertThatThrownBy(() -> frontend.run(new String[0])).isInstanceOf(CliArgsException.class);
    }

    @Test
    void testNonExistingFileWithArguments() throws Exception {
        String[] arguments = {
            "--classpath",
            "file:///tmp/foo",
            "--classpath",
            "file:///tmp/bar",
            "/some/none/existing/path",
            "--debug",
            "true",
            "arg1",
            "arg2"
        };
        URL[] classpath = new URL[] {new URL("file:///tmp/foo"), new URL("file:///tmp/bar")};
        String[] reducedArguments = {"--debug", "true", "arg1", "arg2"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(arguments[4]);
        assertThat(programOptions.getClasspaths().toArray()).isEqualTo(classpath);
        assertThat(programOptions.getProgramArgs()).isEqualTo(reducedArguments);

        assertThatThrownBy(() -> frontend.buildProgram(programOptions))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testNonExistingFileWithoutArguments() throws Exception {
        String[] arguments = {"/some/none/existing/path"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(arguments[0]);
        assertThat(programOptions.getProgramArgs()).isEqualTo(new String[0]);

        try {
            frontend.buildProgram(programOptions);
        } catch (FileNotFoundException e) {
            // that's what we want
        }
    }

    /**
     * Ensure that we will never have the following error.
     *
     * <pre>
     * 	org.apache.flink.client.program.ProgramInvocationException: The main method caused an error.
     * 	at org.apache.flink.client.program.PackagedProgram.callMainMethod(PackagedProgram.java:398)
     * 	at org.apache.flink.client.program.PackagedProgram.invokeInteractiveModeForExecution(PackagedProgram.java:301)
     * 	at org.apache.flink.client.program.Client.getOptimizedPlan(Client.java:140)
     * 	at org.apache.flink.client.program.Client.getOptimizedPlanAsJson(Client.java:125)
     * 	at org.apache.flink.client.cli.CliFrontend.info(CliFrontend.java:439)
     * 	at org.apache.flink.client.cli.CliFrontend.parseParameters(CliFrontend.java:931)
     * 	at org.apache.flink.client.cli.CliFrontend.main(CliFrontend.java:951)
     * Caused by: java.io.IOException: java.lang.RuntimeException: java.lang.ClassNotFoundException: org.apache.hadoop.hive.ql.io.RCFileInputFormat
     * 	at org.apache.hcatalog.mapreduce.HCatInputFormat.setInput(HCatInputFormat.java:102)
     * 	at org.apache.hcatalog.mapreduce.HCatInputFormat.setInput(HCatInputFormat.java:54)
     * 	at tlabs.CDR_In_Report.createHCatInputFormat(CDR_In_Report.java:322)
     * 	at tlabs.CDR_Out_Report.main(CDR_Out_Report.java:380)
     * 	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     * 	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
     * 	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
     * 	at java.lang.reflect.Method.invoke(Method.java:622)
     * 	at org.apache.flink.client.program.PackagedProgram.callMainMethod(PackagedProgram.java:383)
     * </pre>
     *
     * <p>The test works as follows:
     *
     * <ul>
     *   <li>Use the CliFrontend to invoke a jar file that loads a class which is only available in
     *       the jarfile itself (via a custom classloader)
     *   <li>Change the Usercode classloader of the PackagedProgram to a special classloader for
     *       this test
     *   <li>the classloader will accept the special class (and return a String.class)
     * </ul>
     */
    @Test
    void testPlanWithExternalClass() throws Exception {
        final boolean[] callme = {
            false
        }; // create a final object reference, to be able to change its val later

        try {
            String[] arguments = {
                "--classpath",
                "file:///tmp/foo",
                "--classpath",
                "file:///tmp/bar",
                "-c",
                TEST_JAR_CLASSLOADERTEST_CLASS,
                getTestJarPath(),
                "true",
                "arg1",
                "arg2"
            };
            URL[] classpath = new URL[] {new URL("file:///tmp/foo"), new URL("file:///tmp/bar")};
            String[] reducedArguments = {"true", "arg1", "arg2"};

            CommandLine commandLine =
                    CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
            ProgramOptions programOptions = ProgramOptions.create(commandLine);

            assertThat(programOptions.getJarFilePath()).isEqualTo(getTestJarPath());
            assertThat(programOptions.getClasspaths().toArray()).isEqualTo(classpath);
            assertThat(programOptions.getEntryPointClassName())
                    .isEqualTo(TEST_JAR_CLASSLOADERTEST_CLASS);
            assertThat(programOptions.getProgramArgs()).isEqualTo(reducedArguments);

            PackagedProgram prog = spy(frontend.buildProgram(programOptions));

            ClassLoader testClassLoader =
                    new ClassLoader(prog.getUserCodeClassLoader()) {
                        @Override
                        public Class<?> loadClass(String name) throws ClassNotFoundException {
                            if ("org.apache.hadoop.hive.ql.io.RCFileInputFormat".equals(name)) {
                                callme[0] = true;
                                return String.class; // Intentionally return the wrong class.
                            } else {
                                return super.loadClass(name);
                            }
                        }
                    };
            when(prog.getUserCodeClassLoader()).thenReturn(testClassLoader);

            assertThat(prog.getMainClassName()).isEqualTo(TEST_JAR_CLASSLOADERTEST_CLASS);
            assertThat(prog.getArguments()).isEqualTo(reducedArguments);

            Configuration c = new Configuration();
            Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), c);

            // we expect this to fail with a "ClassNotFoundException"
            Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(prog, c, 666, true);
            FlinkPipelineTranslationUtil.translateToJSONExecutionPlan(
                    prog.getUserCodeClassLoader(), pipeline);
            fail("Should have failed with a ClassNotFoundException");
        } catch (ProgramInvocationException e) {
            if (!(e.getCause() instanceof ClassNotFoundException)) {
                e.printStackTrace();
                fail("Program didn't throw ClassNotFoundException");
            }
            if (!callme[0]) {
                fail("Classloader was not called");
            }
        }
    }
}
