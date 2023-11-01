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

package org.apache.flink.streaming.api.operators.python.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonFunctionRunner;
import org.apache.flink.table.functions.python.PythonEnv;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Abstract class for all stream operators to execute Python functions in external environment. */
@Internal
public abstract class AbstractExternalPythonFunctionOperator<OUT>
        extends AbstractPythonFunctionOperator<OUT> {
    /**
     * The {@link PythonFunctionRunner} which is responsible for Python user-defined function
     * execution.
     */
    protected transient PythonFunctionRunner pythonFunctionRunner;

    private transient ExecutorService flushThreadPool;

    public AbstractExternalPythonFunctionOperator(Configuration config) {
        super(config);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.pythonFunctionRunner = createPythonFunctionRunner();
        this.pythonFunctionRunner.open(config);
        this.flushThreadPool = Executors.newSingleThreadExecutor();
    }

    @Override
    public void close() throws Exception {
        try {
            if (pythonFunctionRunner != null) {
                pythonFunctionRunner.close();
                pythonFunctionRunner = null;
            }

            if (flushThreadPool != null) {
                flushThreadPool.shutdown();
                flushThreadPool = null;
            }
        } finally {
            super.close();
        }
    }

    @Override
    protected void invokeFinishBundle() throws Exception {
        if (elementCount > 0) {
            AtomicBoolean flushThreadFinish = new AtomicBoolean(false);
            AtomicReference<Throwable> exceptionReference = new AtomicReference<>();
            flushThreadPool.submit(
                    () -> {
                        try {
                            pythonFunctionRunner.flush();
                        } catch (Throwable e) {
                            exceptionReference.set(e);
                        } finally {
                            flushThreadFinish.set(true);
                            // interrupt the progress of takeResult to avoid the main thread is
                            // blocked forever.
                            ((BeamPythonFunctionRunner) pythonFunctionRunner).notifyNoMoreResults();
                        }
                    });
            Tuple3<String, byte[], Integer> resultTuple;
            while (!flushThreadFinish.get()) {
                resultTuple = pythonFunctionRunner.takeResult();
                if (resultTuple.f2 != 0) {
                    emitResult(resultTuple);
                    emitResults();
                }
            }
            emitResults();
            Throwable flushThreadThrowable = exceptionReference.get();
            if (flushThreadThrowable != null) {
                throw new RuntimeException(
                        "Error while waiting for BeamPythonFunctionRunner flush",
                        flushThreadThrowable);
            }
            elementCount = 0;
            lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
            // callback only after current bundle was fully finalized
            if (bundleFinishedCallback != null) {
                bundleFinishedCallback.run();
                bundleFinishedCallback = null;
            }
        }
    }

    @Override
    protected ProcessPythonEnvironmentManager createPythonEnvironmentManager() {
        PythonDependencyInfo dependencyInfo =
                PythonDependencyInfo.create(config, getRuntimeContext().getDistributedCache());
        PythonEnv pythonEnv = getPythonEnv();
        if (pythonEnv.getExecType() == PythonEnv.ExecType.PROCESS) {
            return new ProcessPythonEnvironmentManager(
                    dependencyInfo,
                    getContainingTask().getEnvironment().getTaskManagerInfo().getTmpDirectories(),
                    systemEnvEnabled ? new HashMap<>(System.getenv()) : new HashMap<>(),
                    getRuntimeContext().getJobId());
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Execution type '%s' is not supported.", pythonEnv.getExecType()));
        }
    }

    protected void emitResults() throws Exception {
        Tuple3<String, byte[], Integer> resultTuple;
        while ((resultTuple = pythonFunctionRunner.pollResult()) != null && resultTuple.f2 != 0) {
            emitResult(resultTuple);
        }
    }

    /** Returns the {@link PythonEnv} used to create PythonEnvironmentManager.. */
    public abstract PythonEnv getPythonEnv();

    /** Sends the execution result to the downstream operator. */
    public abstract void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws Exception;

    /**
     * Creates the {@link PythonFunctionRunner} which is responsible for Python user-defined
     * function execution.
     */
    public abstract PythonFunctionRunner createPythonFunctionRunner() throws Exception;
}
