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

package org.apache.flink.python.env.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.python.env.AbstractPythonEnvironmentManager;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironment;
import org.apache.flink.python.util.PythonEnvironmentManagerUtils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The ProcessPythonEnvironmentManager is used to prepare the working dir of python UDF worker and
 * create ProcessPythonEnvironment object of Beam Fn API. It's used when the python function runner
 * is configured to run python UDF in process mode.
 */
@Internal
public final class ProcessPythonEnvironmentManager extends AbstractPythonEnvironmentManager {

    public ProcessPythonEnvironmentManager(
            PythonDependencyInfo dependencyInfo,
            String[] tmpDirectories,
            Map<String, String> systemEnv,
            JobID jobID) {
        super(dependencyInfo, tmpDirectories, systemEnv, jobID);
    }

    @Override
    public PythonEnvironment createEnvironment() throws Exception {
        HashMap<String, String> env = new HashMap<>(resource.env);

        String runnerScript =
                PythonEnvironmentManagerUtils.getPythonUdfRunnerScript(
                        dependencyInfo.getPythonExec(), env);

        return new ProcessPythonEnvironment(runnerScript, env);
    }

    /**
     * Returns an empty RetrievalToken because no files will be transmit via ArtifactService in
     * process mode.
     *
     * @return The path of empty RetrievalToken.
     */
    public String createRetrievalToken() throws IOException {
        File retrievalToken =
                new File(
                        resource.baseDirectory,
                        "retrieval_token_" + UUID.randomUUID().toString() + ".json");
        if (retrievalToken.createNewFile()) {
            final DataOutputStream dos = new DataOutputStream(new FileOutputStream(retrievalToken));
            dos.writeBytes("{\"manifest\": {}}");
            dos.flush();
            dos.close();
            return retrievalToken.getAbsolutePath();
        } else {
            throw new IOException(
                    "Could not create the RetrievalToken file: "
                            + retrievalToken.getAbsolutePath());
        }
    }

    /** Returns the boot log of the Python Environment. */
    public String getBootLog() throws Exception {
        File bootLogFile =
                new File(resource.baseDirectory + File.separator + "flink-python-udf-boot.log");
        String msg = "Failed to create stage bundle factory!";
        if (bootLogFile.exists()) {
            byte[] output = Files.readAllBytes(bootLogFile.toPath());
            msg += String.format(" %s", new String(output, Charset.defaultCharset()));
        }
        return msg;
    }
}
