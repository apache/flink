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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS;

/** Util class for {@link JobStatusChangedListener}. */
@Internal
public final class JobStatusChangedListenerUtils {
    /**
     * Create job status changed listeners from configuration for job.
     *
     * @param configuration The job configuration.
     * @return the job status changed listeners.
     */
    public static List<JobStatusChangedListener> createJobStatusChangedListeners(
            ClassLoader userClassLoader, Configuration configuration, Executor ioExecutor) {
        List<String> jobStatusChangedListeners = configuration.get(JOB_STATUS_CHANGED_LISTENERS);
        if (jobStatusChangedListeners == null || jobStatusChangedListeners.isEmpty()) {
            return Collections.emptyList();
        }
        return jobStatusChangedListeners.stream()
                .map(
                        fac -> {
                            try {
                                return InstantiationUtil.instantiate(
                                                fac,
                                                JobStatusChangedListenerFactory.class,
                                                userClassLoader)
                                        .createListener(
                                                new JobStatusChangedListenerFactory.Context() {
                                                    @Override
                                                    public Configuration getConfiguration() {
                                                        return configuration;
                                                    }

                                                    @Override
                                                    public ClassLoader getUserClassLoader() {
                                                        return userClassLoader;
                                                    }

                                                    @Override
                                                    public Executor getIOExecutor() {
                                                        return ioExecutor;
                                                    }
                                                });
                            } catch (FlinkException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .collect(Collectors.toList());
    }
}
