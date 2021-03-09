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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/** Test utilities. */
public class CliFrontendTestUtils {

    public static final String TEST_JAR_MAIN_CLASS = "org.apache.flink.client.testjar.TestJob";

    public static final String TEST_JAR_CLASSLOADERTEST_CLASS =
            "org.apache.flink.client.testjar.JobWithExternalDependency";

    public static final String TEST_JOB_MANAGER_ADDRESS = "192.168.1.33";

    public static final int TEST_JOB_MANAGER_PORT = 55443;

    private static final PrintStream previousSysout = System.out;

    public static String getTestJarPath() throws FileNotFoundException, MalformedURLException {
        String projectBaseDir = System.getProperty("project.basedir");
        Path testJarPath = Paths.get(projectBaseDir, "target", "maven-test-jar.jar");
        File f = testJarPath.toFile();
        if (!f.exists()) {
            throw new FileNotFoundException(
                    "Test jar not present. Invoke tests using maven "
                            + "or build the jar using 'mvn process-test-classes' in flink-clients");
        }
        return f.getAbsolutePath();
    }

    public static String getNonJarFilePath() {
        return CliFrontendRunTest.class.getResource("/testconfig/flink-conf.yaml").getFile();
    }

    public static String getConfigDir() {
        String confFile =
                CliFrontendRunTest.class.getResource("/testconfig/flink-conf.yaml").getFile();
        return new File(confFile).getAbsoluteFile().getParent();
    }

    public static String getInvalidConfigDir() {
        String confFile =
                CliFrontendRunTest.class
                        .getResource("/invalidtestconfig/flink-conf.yaml")
                        .getFile();
        return new File(confFile).getAbsoluteFile().getParent();
    }

    public static void pipeSystemOutToNull() {
        System.setOut(new PrintStream(new BlackholeOutputSteam()));
    }

    public static void restoreSystemOut() {
        System.setOut(previousSysout);
    }

    private static final class BlackholeOutputSteam extends java.io.OutputStream {
        @Override
        public void write(int b) {}
    }

    public static void checkJobManagerAddress(
            Configuration config, String expectedAddress, int expectedPort) {
        String jobManagerAddress = config.getString(JobManagerOptions.ADDRESS);
        int jobManagerPort = config.getInteger(JobManagerOptions.PORT, -1);

        assertEquals(expectedAddress, jobManagerAddress);
        assertEquals(expectedPort, jobManagerPort);
    }

    // --------------------------------------------------------------------------------------------

    private CliFrontendTestUtils() {}
}
