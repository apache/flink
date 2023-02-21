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

package org.apache.flink.python.env.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.python.env.AbstractPythonEnvironmentManager;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironment;

import pemja.core.PythonInterpreterConfig;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * The base class of python environment manager which is used to create the PythonEnvironment
 * object. It's used to run python UDF in embedded Python environment.
 */
@Internal
public class EmbeddedPythonEnvironmentManager extends AbstractPythonEnvironmentManager {

    public EmbeddedPythonEnvironmentManager(
            PythonDependencyInfo dependencyInfo,
            String[] tmpDirectories,
            Map<String, String> systemEnv,
            JobID jobID) {
        super(dependencyInfo, tmpDirectories, systemEnv, jobID);
    }

    @Override
    public PythonEnvironment createEnvironment() throws Exception {
        Map<String, String> env = new HashMap<>(getPythonEnv());

        PythonInterpreterConfig.ExecType execType;

        String executionMode = dependencyInfo.getExecutionMode();

        if (executionMode.equalsIgnoreCase("thread")) {
            execType = PythonInterpreterConfig.ExecType.MULTI_THREAD;
        } else {
            throw new RuntimeException(
                    String.format("Unsupported execution mode %s.", executionMode));
        }

        if (env.containsKey("FLINK_TESTING")) {
            String flinkHome = env.get("FLINK_HOME");
            String sourceRootDir = new File(flinkHome, "../../../../").getCanonicalPath();
            String flinkPython = sourceRootDir + "/flink-python";
            // add flink-python of source code to PYTHONPATH
            env.put(
                    "PYTHONPATH",
                    flinkPython + File.pathSeparator + env.getOrDefault("PYTHONPATH", ""));
        }

        PythonInterpreterConfig interpreterConfig =
                PythonInterpreterConfig.newBuilder()
                        .setPythonExec(dependencyInfo.getPythonExec())
                        .setExcType(execType)
                        .addPythonPaths(env.getOrDefault("PYTHONPATH", ""))
                        .build();

        return new EmbeddedPythonEnvironment(interpreterConfig, env);
    }
}
