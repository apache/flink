/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.OperatingSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.python.env.process.ProcessPythonEnvironmentManager.PYTHON_WORKING_DIR;

/** Utils used to prepare the python environment. */
@Internal
public class PythonEnvironmentManagerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PythonEnvironmentManagerUtils.class);

    private static final int MAX_RETRY_TIMES = 3;

    private static final String PYFLINK_UDF_RUNNER_SH = "pyflink-udf-runner.sh";
    private static final String PYFLINK_UDF_RUNNER_BAT = "pyflink-udf-runner.bat";

    @VisibleForTesting public static final String PYFLINK_UDF_RUNNER_DIR = "PYFLINK_UDF_RUNNER_DIR";

    private static final String GET_SITE_PACKAGES_PATH_SCRIPT =
            "import sys;"
                    + "import sysconfig;"
                    + "print(sysconfig.get_path('platlib', vars={'base': sys.argv[1], 'platbase': sys.argv[1]}))";

    private static final String GET_RUNNER_DIR_SCRIPT =
            "import pyflink;"
                    + "import os;"
                    + "print(os.path.join(os.path.abspath(os.path.dirname(pyflink.__file__)), 'bin'))";

    private static final String GET_PYTHON_VERSION = "import sys;print(sys.version)";

    /**
     * Installs the 3rd party libraries listed in the user-provided requirements file. An optional
     * requirements cached directory can be provided to support offline installation. In order not
     * to populate the public environment, the libraries will be installed to the specified
     * directory, and added to the PYTHONPATH of the UDF workers.
     *
     * @param requirementsFilePath The path of the requirements file.
     * @param requirementsCacheDir The path of the requirements cached directory.
     * @param requirementsInstallDir The target directory of the installation.
     * @param pythonExecutable The python interpreter used to launch the pip program.
     * @param environmentVariables The environment variables used to launch the pip program.
     */
    public static void pipInstallRequirements(
            String requirementsFilePath,
            @Nullable String requirementsCacheDir,
            String requirementsInstallDir,
            String pythonExecutable,
            Map<String, String> environmentVariables)
            throws IOException {
        String sitePackagesPath =
                getSitePackagesPath(requirementsInstallDir, pythonExecutable, environmentVariables);
        String path = String.join(File.pathSeparator, requirementsInstallDir, "bin");
        appendToEnvironmentVariable("PYTHONPATH", sitePackagesPath, environmentVariables);
        appendToEnvironmentVariable("PATH", path, environmentVariables);

        List<String> commands =
                new ArrayList<>(
                        Arrays.asList(
                                pythonExecutable,
                                "-m",
                                "pip",
                                "install",
                                "--ignore-installed",
                                "-r",
                                requirementsFilePath,
                                "--prefix",
                                requirementsInstallDir));
        if (requirementsCacheDir != null) {
            commands.addAll(Arrays.asList("--no-index", "--find-links", requirementsCacheDir));
        }

        int retries = 0;
        while (true) {
            try {
                execute(commands.toArray(new String[0]), environmentVariables, true);
                break;
            } catch (Throwable t) {
                retries++;
                if (retries < MAX_RETRY_TIMES) {
                    LOG.warn(
                            String.format(
                                    "Pip install failed, retrying... (%d/%d)",
                                    retries, MAX_RETRY_TIMES),
                            t);
                } else {
                    LOG.error(
                            String.format(
                                    "Pip install failed, already retried %d time...", retries));
                    throw new IOException(t);
                }
            }
        }
    }

    public static String getPythonUdfRunnerScript(
            String pythonExecutable, Map<String, String> environmentVariables) throws IOException {
        String runnerDir;
        if (environmentVariables.containsKey(PYFLINK_UDF_RUNNER_DIR)) {
            runnerDir = environmentVariables.get(PYFLINK_UDF_RUNNER_DIR);
        } else {
            String[] commands = new String[] {pythonExecutable, "-c", GET_RUNNER_DIR_SCRIPT};
            String out = execute(commands, environmentVariables, false);
            runnerDir = out.trim();
        }
        String runnerScriptPath;
        if (OperatingSystem.isWindows()) {
            runnerScriptPath = String.join(File.separator, runnerDir, PYFLINK_UDF_RUNNER_BAT);
        } else {
            runnerScriptPath = String.join(File.separator, runnerDir, PYFLINK_UDF_RUNNER_SH);
        }
        return runnerScriptPath;
    }

    public static String getPythonVersion(String pythonExecutable) throws IOException {
        String[] commands = new String[] {pythonExecutable, "-c", GET_PYTHON_VERSION};
        String out = execute(commands, new HashMap<>(), false);
        return out.trim();
    }

    private static String getSitePackagesPath(
            String prefix, String pythonExecutable, Map<String, String> environmentVariables)
            throws IOException {
        String[] commands =
                new String[] {pythonExecutable, "-c", GET_SITE_PACKAGES_PATH_SCRIPT, prefix};
        String out = execute(commands, environmentVariables, false);
        return String.join(File.pathSeparator, out.trim().split("\n"));
    }

    private static String execute(
            String[] commands, Map<String, String> environmentVariables, boolean logDetails)
            throws IOException {
        ProcessBuilder pb = new ProcessBuilder(commands);
        if (environmentVariables.containsKey(PYTHON_WORKING_DIR)) {
            pb.directory(new File(environmentVariables.get(PYTHON_WORKING_DIR)));
        }
        pb.environment().putAll(environmentVariables);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        InputStream in = new BufferedInputStream(p.getInputStream());
        StringBuilder out = new StringBuilder();
        String s;
        if (logDetails) {
            LOG.info(String.format("Executing command: %s", String.join(" ", commands)));
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            while ((s = br.readLine()) != null) {
                out.append(s).append("\n");
                if (logDetails) {
                    LOG.info(s);
                }
            }
        }
        try {
            if (p.waitFor() != 0) {
                throw new IOException(
                        String.format(
                                "Failed to execute the command: %s\noutput: %s",
                                String.join(" ", commands), out));
            }
        } catch (InterruptedException e) {
            // Ignored. The subprocess is dead after "br.readLine()" returns null, so the call of
            // "waitFor" should return intermediately.
        }
        return out.toString();
    }

    private static void appendToEnvironmentVariable(
            String key, String value, Map<String, String> env) {
        if (env.containsKey(key)) {
            env.put(key, String.join(File.pathSeparator, value, env.get(key)));
        } else {
            env.put(key, value);
        }
    }
}
