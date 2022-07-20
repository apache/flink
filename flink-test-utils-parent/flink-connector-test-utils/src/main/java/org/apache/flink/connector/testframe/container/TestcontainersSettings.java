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

package org.apache.flink.connector.testframe.container;

import org.slf4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/** The configuration holder for Testcontainers. */
public class TestcontainersSettings {

    private final Network network;
    private final Logger logger;
    private final String baseImage;
    private final Map<String, String> envVars;
    private final Collection<GenericContainer<?>> dependencies;

    private TestcontainersSettings(Builder builder) {
        network = builder.network;
        baseImage = builder.baseImage;
        logger = builder.logger;
        envVars = builder.envVars;
        dependencies = builder.dependingContainers;
    }

    /**
     * A new builder for {@code TestcontainersSettings}.
     *
     * @return The builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@code TestcontainersSettings} based on defaults.
     *
     * @return The Testcontainers config.
     */
    public static TestcontainersSettings defaultSettings() {
        return builder().build();
    }

    /** The {@code TestContainersSettings} builder. */
    public static final class Builder {
        private Network network = Network.newNetwork();
        private String baseImage;
        private Logger logger;
        private final Map<String, String> envVars = new HashMap<>();
        private final Collection<GenericContainer<?>> dependingContainers = new ArrayList<>();

        private Builder() {}

        /**
         * Sets an environment variable and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param name The name of the environment variable.
         * @param value The value of the environment variable.
         * @return A reference to this Builder.
         */
        public Builder environmentVariable(String name, String value) {
            this.envVars.put(name, value);
            return this;
        }

        /**
         * Lets Flink cluster depending on another container, and bind the network of Flink cluster
         * to the dependent one.
         */
        public Builder dependsOn(GenericContainer<?> container) {
            container.withNetwork(this.network);
            this.dependingContainers.add(container);
            return this;
        }

        /**
         * Sets the {@code network} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param network The {@code network} to set.
         * @return A reference to this Builder.
         */
        public Builder network(Network network) {
            this.network = network;
            return this;
        }

        /**
         * Sets the {@code baseImage} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param baseImage The {@code baseImage} to set.
         * @return A reference to this Builder.
         */
        public Builder baseImage(String baseImage) {
            this.baseImage = baseImage;
            return this;
        }

        /**
         * Sets the {@code baseImage} and returns a reference to this Builder enabling method
         * chaining.
         *
         * @param logger The {@code logger} to set.
         * @return A reference to this Builder.
         */
        public Builder logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        /**
         * Returns a {@code TestContainersSettings} built from the parameters previously set.
         *
         * @return A {@code TestContainersSettings} built with parameters of this {@code
         *     TestContainersSettings.Builder}
         */
        public TestcontainersSettings build() {
            return new TestcontainersSettings(this);
        }
    }

    /** @return The network. */
    public Network getNetwork() {
        return network;
    }

    /** @return The logger. */
    public Logger getLogger() {
        return logger;
    }

    /** @return The base image. */
    public String getBaseImage() {
        return baseImage;
    }

    /** @return The environment variables. */
    public Map<String, String> getEnvVars() {
        return envVars;
    }

    /** @return The dependencies (other containers). */
    public Collection<GenericContainer<?>> getDependencies() {
        return dependencies;
    }
}
