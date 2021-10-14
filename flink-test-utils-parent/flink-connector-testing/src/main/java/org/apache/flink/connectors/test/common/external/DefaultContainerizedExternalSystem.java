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

package org.apache.flink.connectors.test.common.external;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.connectors.test.common.TestResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

/**
 * Default implementation of external system based on container.
 *
 * @param <C> Type of underlying container
 */
@Experimental
public class DefaultContainerizedExternalSystem<C extends GenericContainer<C>>
        implements TestResource {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultContainerizedExternalSystem.class);

    private final C container;

    /**
     * Get a builder for {@link DefaultContainerizedExternalSystem}.
     *
     * @param <C> Type of underlying container
     * @return An instance of builder
     */
    public static <C extends GenericContainer<C>> Builder<C> builder() {
        return new Builder<>();
    }

    private DefaultContainerizedExternalSystem(C container) {
        this.container = container;
    }

    @Override
    public void startUp() throws Exception {
        if (container.isRunning()) {
            return;
        }
        container.start();
    }

    @Override
    public void tearDown() throws Exception {
        if (!container.isRunning()) {
            return;
        }
        container.stop();
    }

    public C getContainer() {
        return this.container;
    }

    /**
     * Builder for {@link DefaultContainerizedExternalSystem}.
     *
     * @param <C> Type of underlying container
     */
    public static class Builder<C extends GenericContainer<C>> {

        private C container;
        private GenericContainer<?> flinkContainer;

        public <T extends GenericContainer<T>> Builder<T> fromContainer(T container) {
            @SuppressWarnings("unchecked")
            Builder<T> self = (Builder<T>) this;
            self.container = container;
            return self;
        }

        public Builder<C> bindWithFlinkContainer(GenericContainer<?> flinkContainer) {
            this.flinkContainer = flinkContainer;
            container.dependsOn(flinkContainer).withNetwork(flinkContainer.getNetwork());
            return this;
        }

        public DefaultContainerizedExternalSystem<C> build() {
            if (flinkContainer == null) {
                LOG.warn(
                        "External system container is not bound with Flink container. "
                                + "This might lead to network isolation between external system and Flink");
            }
            return new DefaultContainerizedExternalSystem<>(container);
        }
    }
}
