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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;

import java.io.Serializable;

/** Wrapper of serializedShuffleDescriptors, used for {@link InputGateDeploymentDescriptor}. */
public class SerializedShuffleDescriptorAndIndices implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Serialized value of shuffle descriptors with index. */
    private final MaybeOffloaded<ShuffleDescriptorAndIndex[]> serializedShuffleDescriptors;

    private final SerializedShuffleDescriptorAndIndicesID serializedShuffleDescriptorAndIndicesId;

    public SerializedShuffleDescriptorAndIndices(
            TaskDeploymentDescriptor.MaybeOffloaded<
                            TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[]>
                    serializedShuffleDescriptors) {
        this.serializedShuffleDescriptors = serializedShuffleDescriptors;
        this.serializedShuffleDescriptorAndIndicesId =
                new SerializedShuffleDescriptorAndIndicesID();
    }

    public MaybeOffloaded<ShuffleDescriptorAndIndex[]> getSerializedShuffleDescriptors() {
        return serializedShuffleDescriptors;
    }

    public SerializedShuffleDescriptorAndIndicesID getSerializedShuffleDescriptorsId() {
        return serializedShuffleDescriptorAndIndicesId;
    }
}
