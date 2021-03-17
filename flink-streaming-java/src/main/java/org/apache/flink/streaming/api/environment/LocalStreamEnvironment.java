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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.graph.StreamGraph;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The LocalStreamEnvironment is a StreamExecutionEnvironment that runs the program locally,
 * multi-threaded, in the JVM where the environment is instantiated. It spawns an embedded Flink
 * cluster in the background and executes the program on that cluster.
 */
@Public
public class LocalStreamEnvironment extends StreamExecutionEnvironment {

    /** Creates a new mini cluster stream environment that uses the default configuration. */
    public LocalStreamEnvironment() {
        this(new Configuration());
    }

    /**
     * Creates a new mini cluster stream environment that configures its local executor with the
     * given configuration.
     *
     * @param configuration The configuration used to configure the local executor.
     */
    public LocalStreamEnvironment(@Nonnull Configuration configuration) {
        super(validateAndGetConfiguration(configuration));
    }

    private static Configuration validateAndGetConfiguration(final Configuration configuration) {
        if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
            throw new InvalidProgramException(
                    "The LocalStreamEnvironment cannot be used when submitting a program through a client, "
                            + "or running in a TestEnvironment context.");
        }
        final Configuration effectiveConfiguration = new Configuration(checkNotNull(configuration));
        effectiveConfiguration.set(DeploymentOptions.TARGET, "local");
        effectiveConfiguration.set(DeploymentOptions.ATTACHED, true);
        return effectiveConfiguration;
    }

    @Override
    public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
        return super.execute(streamGraph);
    }
}
