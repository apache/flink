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

package org.apache.flink.runtime.clusterframework.overlays;

import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** A composite overlay that delegates to a set of inner overlays. */
public class CompositeContainerOverlay implements ContainerOverlay {

    private final List<ContainerOverlay> overlays;

    public CompositeContainerOverlay(ContainerOverlay... overlays) {
        this(Arrays.asList(overlays));
    }

    public CompositeContainerOverlay(List<ContainerOverlay> overlays) {
        this.overlays = Collections.unmodifiableList(overlays);
    }

    @Override
    public void configure(ContainerSpecification containerConfig) throws IOException {
        for (ContainerOverlay overlay : overlays) {
            overlay.configure(containerConfig);
        }
    }
}
