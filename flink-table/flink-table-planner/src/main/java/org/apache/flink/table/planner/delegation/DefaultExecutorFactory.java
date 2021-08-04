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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;

import java.util.Collections;
import java.util.Set;

/**
 * Factory to create the default implementation of an {@link Executor} for {@link TableEnvironment}.
 *
 * <p>The unified {@link TableEnvironment} will use {@link #create(Configuration)} method that does
 * not bind to any particular execution environment.
 *
 * <p>{@link org.apache.flink.table.api.bridge.scala.StreamTableEnvironment} and {@link
 * org.apache.flink.table.api.bridge.java.StreamTableEnvironment} will use {@link
 * #create(StreamExecutionEnvironment)}.
 */
@Internal
public final class DefaultExecutorFactory implements ExecutorFactory {

    @Override
    public Executor create(Configuration configuration) {
        return create(StreamExecutionEnvironment.getExecutionEnvironment(configuration));
    }

    public Executor create(StreamExecutionEnvironment executionEnvironment) {
        return new DefaultExecutor(executionEnvironment);
    }

    @Override
    public String factoryIdentifier() {
        return ExecutorFactory.DEFAULT_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
