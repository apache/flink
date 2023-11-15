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

package org.apache.flink.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.client.program.rest.retry.ExponentialWaitStrategy;
import org.apache.flink.client.program.rest.retry.WaitStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.HttpHeader;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utility functions for Flink client. */
public enum ClientUtils {
    ;

    private static final Logger LOG = LoggerFactory.getLogger(ClientUtils.class);

    public static URLClassLoader buildUserCodeClassLoader(
            List<URL> jars, List<URL> classpaths, ClassLoader parent, Configuration configuration) {
        URL[] urls = new URL[jars.size() + classpaths.size()];
        for (int i = 0; i < jars.size(); i++) {
            urls[i] = jars.get(i);
        }
        for (int i = 0; i < classpaths.size(); i++) {
            urls[i + jars.size()] = classpaths.get(i);
        }
        return FlinkUserCodeClassLoaders.create(urls, parent, configuration);
    }

    public static void executeProgram(
            PipelineExecutorServiceLoader executorServiceLoader,
            Configuration configuration,
            PackagedProgram program,
            boolean enforceSingleJobExecution,
            boolean suppressSysout)
            throws ProgramInvocationException {
        checkNotNull(executorServiceLoader);
        final ClassLoader userCodeClassLoader = program.getUserCodeClassLoader();
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(userCodeClassLoader);

            LOG.info(
                    "Starting program (detached: {})",
                    !configuration.getBoolean(DeploymentOptions.ATTACHED));

            ContextEnvironment.setAsContext(
                    executorServiceLoader,
                    configuration,
                    userCodeClassLoader,
                    enforceSingleJobExecution,
                    suppressSysout);

            StreamContextEnvironment.setAsContext(
                    executorServiceLoader,
                    configuration,
                    userCodeClassLoader,
                    enforceSingleJobExecution,
                    suppressSysout);

            try {
                program.invokeInteractiveModeForExecution();
            } finally {
                ContextEnvironment.unsetAsContext();
                StreamContextEnvironment.unsetAsContext();
            }
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    /**
     * This method blocks until the job status is not INITIALIZING anymore.
     *
     * @param jobStatusSupplier supplier returning the job status.
     * @param jobResultSupplier supplier returning the job result. This will only be called if the
     *     job reaches the FAILED state.
     * @throws JobInitializationException If the initialization failed
     */
    public static void waitUntilJobInitializationFinished(
            SupplierWithException<JobStatus, Exception> jobStatusSupplier,
            SupplierWithException<JobResult, Exception> jobResultSupplier,
            ClassLoader userCodeClassloader)
            throws JobInitializationException {
        LOG.debug("Wait until job initialization is finished");
        WaitStrategy waitStrategy = new ExponentialWaitStrategy(50, 2000);
        try {
            JobStatus status = jobStatusSupplier.get();
            long attempt = 0;
            while (status == JobStatus.INITIALIZING) {
                Thread.sleep(waitStrategy.sleepTime(attempt++));
                status = jobStatusSupplier.get();
            }
            if (status == JobStatus.FAILED) {
                JobResult result = jobResultSupplier.get();
                Optional<SerializedThrowable> throwable = result.getSerializedThrowable();
                if (throwable.isPresent()) {
                    Throwable t = throwable.get().deserializeError(userCodeClassloader);
                    if (t instanceof JobInitializationException) {
                        throw t;
                    }
                }
            }
        } catch (JobInitializationException initializationException) {
            throw initializationException;
        } catch (Throwable throwable) {
            ExceptionUtils.checkInterrupted(throwable);
            throw new RuntimeException("Error while waiting for job to be initialized", throwable);
        }
    }

    /**
     * The client reports the heartbeat to the dispatcher for aliveness.
     *
     * @param jobClient The job client.
     * @param interval The heartbeat interval.
     * @param timeout The heartbeat timeout.
     * @return The ScheduledExecutorService which reports heartbeat periodically.
     */
    public static ScheduledExecutorService reportHeartbeatPeriodically(
            JobClient jobClient, long interval, long timeout) {
        checkArgument(
                interval < timeout,
                "The client's heartbeat interval "
                        + "should be less than the heartbeat timeout. Please adjust the param '"
                        + ClientOptions.CLIENT_HEARTBEAT_INTERVAL
                        + "' or '"
                        + ClientOptions.CLIENT_HEARTBEAT_TIMEOUT
                        + "'");

        JobID jobID = jobClient.getJobID();
        LOG.info("Begin to report client's heartbeat for the job {}.", jobID);

        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleAtFixedRate(
                () -> {
                    LOG.debug("Report client's heartbeat for the job {}.", jobID);
                    jobClient.reportHeartbeat(System.currentTimeMillis() + timeout);
                },
                interval,
                interval,
                TimeUnit.MILLISECONDS);
        return scheduledExecutor;
    }

    public static Collection<HttpHeader> readHeadersFromEnvironmentVariable(String envVarName) {
        List<HttpHeader> headers = new ArrayList<>();
        String rawHeaders = System.getenv(envVarName);

        if (rawHeaders != null) {
            String[] lines = rawHeaders.split("\n");
            for (String line : lines) {
                String[] keyValue = line.split(":", 2);
                if (keyValue.length == 2) {
                    headers.add(new HttpHeader(keyValue[0], keyValue[1]));
                } else {
                    LOG.info(
                            "Skipped a malformed header {} from FLINK_REST_CLIENT_HEADERS env variable. Expecting newline-separated headers in format header_name:header_value.",
                            line);
                }
            }
        }
        return headers;
    }
}
