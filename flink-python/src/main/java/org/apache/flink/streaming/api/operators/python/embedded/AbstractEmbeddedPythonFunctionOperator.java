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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.embedded.EmbeddedPythonEnvironment;
import org.apache.flink.python.env.embedded.EmbeddedPythonEnvironmentManager;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;

import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.python.env.AbstractPythonEnvironmentManager.PYTHON_WORKING_DIR;

/**
 * Abstract class for all stream operators to execute Python functions in embedded Python
 * environment.
 */
@Internal
public abstract class AbstractEmbeddedPythonFunctionOperator<OUT>
        extends AbstractPythonFunctionOperator<OUT> {

    private static final long serialVersionUID = 1L;

    private static final ReentrantLock lock = new ReentrantLock();

    private static final Map<JobID, Tuple2<String, Integer>> workingDirectories = new HashMap<>();

    private transient EmbeddedPythonEnvironmentManager pythonEnvironmentManager;

    /** Every operator will hold the only python interpreter. */
    protected transient PythonInterpreter interpreter;

    public AbstractEmbeddedPythonFunctionOperator(Configuration config) {
        super(config);
    }

    @Override
    public void open() throws Exception {
        super.open();
        pythonEnvironmentManager = createPythonEnvironmentManager();
        pythonEnvironmentManager.open();

        EmbeddedPythonEnvironment environment =
                (EmbeddedPythonEnvironment) pythonEnvironmentManager.createEnvironment();

        PythonInterpreterConfig interpreterConfig = environment.getConfig();
        interpreter = new PythonInterpreter(interpreterConfig);
        Map<String, String> env = environment.getEnv();

        if (env.containsKey(PYTHON_WORKING_DIR)) {
            lock.lockInterruptibly();

            try {
                JobID jobId = getRuntimeContext().getJobId();
                Tuple2<String, Integer> dirAndNums;

                if (workingDirectories.containsKey(jobId)) {
                    dirAndNums = workingDirectories.get(jobId);
                } else {
                    dirAndNums = Tuple2.of(null, 0);
                    workingDirectories.put(jobId, dirAndNums);
                }
                dirAndNums.f1 += 1;

                if (dirAndNums.f0 == null) {
                    // get current directory.
                    interpreter.exec("import os;cwd = os.getcwd();");
                    dirAndNums.f0 = interpreter.get("cwd", String.class);
                    String workingDirectory = env.get(PYTHON_WORKING_DIR);
                    // set working directory
                    interpreter.exec(String.format("import os;os.chdir('%s')", workingDirectory));
                }
            } finally {
                lock.unlock();
            }
        }

        openPythonInterpreter();
    }

    @Override
    public void close() throws Exception {
        try {
            JobID jobId = getRuntimeContext().getJobId();
            if (workingDirectories.containsKey(jobId)) {
                lock.lockInterruptibly();

                try {
                    Tuple2<String, Integer> dirAndNums = workingDirectories.get(jobId);
                    dirAndNums.f1 -= 1;
                    if (dirAndNums.f1 == 0) {
                        // change to previous working directory.
                        interpreter.exec(String.format("import os;os.chdir('%s')", dirAndNums.f0));
                    }
                } finally {
                    lock.unlock();
                }
            }

            if (interpreter != null) {
                interpreter.close();
            }

            if (pythonEnvironmentManager != null) {
                pythonEnvironmentManager.close();
            }
        } finally {
            super.close();
        }
    }

    @Override
    protected EmbeddedPythonEnvironmentManager createPythonEnvironmentManager() {
        PythonDependencyInfo dependencyInfo =
                PythonDependencyInfo.create(config, getRuntimeContext().getDistributedCache());
        return new EmbeddedPythonEnvironmentManager(
                dependencyInfo,
                getContainingTask().getEnvironment().getTaskManagerInfo().getTmpDirectories(),
                systemEnvEnabled ? new HashMap<>(System.getenv()) : new HashMap<>(),
                getRuntimeContext().getJobId());
    }

    @Override
    protected void invokeFinishBundle() throws Exception {
        // TODO: Support batches invoking.
    }

    /** Setup method for Python Interpreter. */
    public abstract void openPythonInterpreter();
}
