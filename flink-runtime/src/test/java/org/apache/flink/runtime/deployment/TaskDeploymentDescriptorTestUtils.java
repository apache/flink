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
 * limitations under the License
 */

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.NonOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.Offloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.CompressedSerializedValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A collection of utility methods for testing the TaskDeploymentDescriptor and its related classes.
 */
public class TaskDeploymentDescriptorTestUtils {

    public static ShuffleDescriptor[] deserializeShuffleDescriptors(
            List<MaybeOffloaded<ShuffleDescriptorGroup>> maybeOffloaded,
            JobID jobId,
            TestingBlobWriter blobWriter)
            throws IOException, ClassNotFoundException {
        Map<Integer, ShuffleDescriptor> shuffleDescriptorsMap = new HashMap<>();
        int maxIndex = 0;
        for (MaybeOffloaded<ShuffleDescriptorGroup> sd : maybeOffloaded) {
            ShuffleDescriptorGroup shuffleDescriptorGroup;
            if (sd instanceof NonOffloaded) {
                shuffleDescriptorGroup =
                        ((NonOffloaded<ShuffleDescriptorGroup>) sd)
                                .serializedValue.deserializeValue(
                                        ClassLoader.getSystemClassLoader());

            } else {
                final CompressedSerializedValue<ShuffleDescriptorGroup> compressedSerializedValue =
                        CompressedSerializedValue.fromBytes(
                                blobWriter.getBlob(
                                        jobId,
                                        ((Offloaded<ShuffleDescriptorGroup>) sd)
                                                .serializedValueKey));
                shuffleDescriptorGroup =
                        compressedSerializedValue.deserializeValue(
                                ClassLoader.getSystemClassLoader());
            }
            for (ShuffleDescriptorAndIndex shuffleDescriptorAndIndex :
                    shuffleDescriptorGroup.getShuffleDescriptors()) {
                int index = shuffleDescriptorAndIndex.getIndex();
                maxIndex = Math.max(maxIndex, shuffleDescriptorAndIndex.getIndex());
                shuffleDescriptorsMap.put(index, shuffleDescriptorAndIndex.getShuffleDescriptor());
            }
        }
        ShuffleDescriptor[] shuffleDescriptors = new ShuffleDescriptor[maxIndex + 1];
        shuffleDescriptorsMap.forEach((key, value) -> shuffleDescriptors[key] = value);
        return shuffleDescriptors;
    }
}
