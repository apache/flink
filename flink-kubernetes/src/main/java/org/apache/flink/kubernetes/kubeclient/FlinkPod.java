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

package org.apache.flink.kubernetes.kubeclient;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A collection of variables that composes a JobManager/TaskManager Pod. This can include the Pod,
 * the main Container, and the InitContainer, etc.
 */
public class FlinkPod {

    private final Pod podWithoutMainContainer;

    private final Container mainContainer;

    public FlinkPod(Pod podWithoutMainContainer, Container mainContainer) {
        this.podWithoutMainContainer = podWithoutMainContainer;
        this.mainContainer = mainContainer;
    }

    public Pod getPodWithoutMainContainer() {
        return podWithoutMainContainer;
    }

    public Container getMainContainer() {
        return mainContainer;
    }

    public FlinkPod copy() {
        return new FlinkPod(
                new PodBuilder(this.getPodWithoutMainContainer()).build(),
                new ContainerBuilder(this.getMainContainer()).build());
    }

    /** Builder for creating a {@link FlinkPod}. */
    public static class Builder {

        private Pod podWithoutMainContainer;
        private Container mainContainer;

        public Builder() {
            this.podWithoutMainContainer =
                    new PodBuilder()
                            .withNewMetadata()
                            .endMetadata()
                            .withNewSpec()
                            .endSpec()
                            .build();

            this.mainContainer = new ContainerBuilder().build();
        }

        public Builder(FlinkPod flinkPod) {
            checkNotNull(flinkPod);
            this.podWithoutMainContainer = checkNotNull(flinkPod.getPodWithoutMainContainer());
            this.mainContainer = checkNotNull(flinkPod.getMainContainer());
        }

        public Builder withPod(Pod pod) {
            this.podWithoutMainContainer = checkNotNull(pod);
            return this;
        }

        public Builder withMainContainer(Container mainContainer) {
            this.mainContainer = checkNotNull(mainContainer);
            return this;
        }

        public FlinkPod build() {
            return new FlinkPod(this.podWithoutMainContainer, this.mainContainer);
        }
    }
}
