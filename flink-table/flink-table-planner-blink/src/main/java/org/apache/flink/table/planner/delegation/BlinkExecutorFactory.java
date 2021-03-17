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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory to create an implementation of {@link Executor} to use in a {@link
 * org.apache.flink.table.api.TableEnvironment}. The {@link
 * org.apache.flink.table.api.TableEnvironment} should use {@link #create(Map)} method that does not
 * bind to any particular environment, whereas {@link
 * org.apache.flink.table.api.bridge.scala.StreamTableEnvironment} should use {@link #create(Map,
 * StreamExecutionEnvironment)} as it is always backed by some {@link StreamExecutionEnvironment}
 */
@Internal
public class BlinkExecutorFactory implements ExecutorFactory {

    /**
     * Creates a corresponding {@link ExecutorBase}.
     *
     * @param properties Static properties of the {@link Executor}, the same that were used for
     *     factory lookup.
     * @param executionEnvironment a {@link StreamExecutionEnvironment} to use while executing Table
     *     programs.
     * @return instance of a {@link Executor}
     */
    public Executor create(
            Map<String, String> properties, StreamExecutionEnvironment executionEnvironment) {
        if (Boolean.valueOf(properties.getOrDefault(EnvironmentSettings.STREAMING_MODE, "true"))) {
            return new StreamExecutor(executionEnvironment);
        } else {
            return new BatchExecutor(executionEnvironment);
        }
    }

    @Override
    public Executor create(Map<String, String> properties) {
        return create(properties, StreamExecutionEnvironment.getExecutionEnvironment());
    }

    @Override
    public Map<String, String> requiredContext() {
        DescriptorProperties properties = new DescriptorProperties();
        return properties.asMap();
    }

    @Override
    public List<String> supportedProperties() {
        return Arrays.asList(EnvironmentSettings.STREAMING_MODE, EnvironmentSettings.CLASS_NAME);
    }

    @Override
    public Map<String, String> optionalContext() {
        Map<String, String> context = new HashMap<>();
        context.put(EnvironmentSettings.CLASS_NAME, this.getClass().getCanonicalName());
        return context;
    }
}
