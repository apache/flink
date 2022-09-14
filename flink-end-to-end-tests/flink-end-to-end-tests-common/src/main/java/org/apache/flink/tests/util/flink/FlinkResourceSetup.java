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

package org.apache.flink.tests.util.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/** Setup instructions for a {@link FlinkResource}. */
public class FlinkResourceSetup {

    @Nullable private final Configuration config;
    private final Collection<JarOperation> jarOperations;
    private final Collection<JarAddition> jarAdditions;

    private FlinkResourceSetup(
            @Nullable Configuration config,
            Collection<JarOperation> jarOperations,
            Collection<JarAddition> jarAdditions) {
        this.config = config;
        this.jarOperations = Preconditions.checkNotNull(jarOperations);
        this.jarAdditions = Preconditions.checkNotNull(jarAdditions);
    }

    public Optional<Configuration> getConfig() {
        return Optional.ofNullable(config);
    }

    public Collection<JarOperation> getJarOperations() {
        return jarOperations;
    }

    public Collection<JarAddition> getJarAdditions() {
        return jarAdditions;
    }

    public static FlinkResourceSetupBuilder builder() {
        return new FlinkResourceSetupBuilder();
    }

    /** Builder for {@link FlinkResourceSetup}. */
    public static class FlinkResourceSetupBuilder {

        private Configuration config;
        private final Collection<JarOperation> jarOperations = new ArrayList<>();
        private final Collection<JarAddition> jarAdditions = new ArrayList<>();

        private FlinkResourceSetupBuilder() {}

        public FlinkResourceSetupBuilder addConfiguration(Configuration config) {
            this.config = config;
            return this;
        }

        public FlinkResourceSetupBuilder addJar(Path jar, JarLocation target) {
            this.jarAdditions.add(new JarAddition(jar, target));
            return this;
        }

        public FlinkResourceSetupBuilder moveJar(
                String jarNamePrefix, JarLocation source, JarLocation target) {
            this.jarOperations.add(
                    new JarOperation(
                            jarNamePrefix, source, target, JarOperation.OperationType.MOVE));
            return this;
        }

        public FlinkResourceSetupBuilder copyJar(
                String jarNamePrefix, JarLocation source, JarLocation target) {
            this.jarOperations.add(
                    new JarOperation(
                            jarNamePrefix, source, target, JarOperation.OperationType.COPY));
            return this;
        }

        public FlinkResourceSetup build() {
            return new FlinkResourceSetup(
                    config,
                    Collections.unmodifiableCollection(jarOperations),
                    Collections.unmodifiableCollection(jarAdditions));
        }
    }
}
