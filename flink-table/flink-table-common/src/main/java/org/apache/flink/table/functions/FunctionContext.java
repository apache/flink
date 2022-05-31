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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.TableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Map;
import java.util.Set;

/**
 * A {@link FunctionContext} allows to obtain global runtime information about the context in which
 * the user-defined function is executed.
 *
 * <p>The information includes the metric group, distributed cache files, and global job parameters.
 *
 * <p>Note: Depending on the call location of a function, not all information might be available.
 * For example, during constant expression reduction the function is executed locally with minimal
 * information.
 */
@PublicEvolving
public class FunctionContext {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionContext.class);

    private static final UnregisteredMetricsGroup defaultMetricsGroup =
            new UnregisteredMetricsGroup();

    private final @Nullable RuntimeContext context;

    private final @Nullable ClassLoader userClassLoader;

    private final @Nullable Map<String, String> jobParameters;

    public FunctionContext(
            @Nullable RuntimeContext context,
            @Nullable ClassLoader userClassLoader,
            @Nullable Configuration jobParameters) {
        this.context = context;
        this.userClassLoader = userClassLoader;
        this.jobParameters = jobParameters != null ? jobParameters.toMap() : null;
    }

    public FunctionContext(RuntimeContext context) {
        this(context, null, null);
    }

    /**
     * Returns the metric group for this parallel subtask.
     *
     * @return metric group for this parallel subtask.
     */
    public MetricGroup getMetricGroup() {
        if (context == null) {
            LOG.warn(
                    "Calls to FunctionContext.getMetricGroup will have no effect "
                            + "at the current location.");
            return defaultMetricsGroup;
        }
        return context.getMetricGroup();
    }

    /**
     * Gets the local temporary file copy of a distributed cache files.
     *
     * @param name distributed cache file name
     * @return local temporary file copy of a distributed cache file.
     */
    public File getCachedFile(String name) {
        if (context == null) {
            throw new TableException(
                    "Calls to FunctionContext.getCachedFile are not available "
                            + "at the current location.");
        }
        return context.getDistributedCache().getFile(name);
    }

    /**
     * Gets the global job parameter value associated with the given key as a string.
     *
     * @param key key pointing to the associated value
     * @param defaultValue default value which is returned in case global job parameter is null or
     *     there is no value associated with the given key
     * @return (default) value associated with the given key
     */
    public String getJobParameter(String key, String defaultValue) {
        if (context == null && jobParameters == null) {
            throw new TableException(
                    "Calls to FunctionContext.getJobParameter are not available "
                            + "at the current location.");
        } else if (context == null) {
            return jobParameters.getOrDefault(key, defaultValue);
        }

        final GlobalJobParameters conf = context.getExecutionConfig().getGlobalJobParameters();
        if (conf != null) {
            return conf.toMap().getOrDefault(key, defaultValue);
        }
        return defaultValue;
    }

    /** Get the external resource information. */
    public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
        if (context == null) {
            throw new TableException(
                    "Calls to FunctionContext.getExternalResourceInfos are not available "
                            + "at the current location.");
        }
        return context.getExternalResourceInfos(resourceName);
    }

    /**
     * Gets the {@link ClassLoader} to load classes that are not in system's classpath, but are part
     * of the JAR file of a user job.
     */
    public ClassLoader getUserCodeClassLoader() {
        if (context == null && userClassLoader == null) {
            throw new TableException(
                    "Calls to FunctionContext.getUserCodeClassLoader are not available "
                            + "at the current location.");
        } else if (context == null) {
            return userClassLoader;
        }
        return context.getUserCodeClassLoader();
    }
}
