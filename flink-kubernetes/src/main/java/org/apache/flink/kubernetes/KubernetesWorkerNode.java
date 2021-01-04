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

package org.apache.flink.kubernetes;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A stored Kubernetes worker, which contains the kubernetes pod. */
public class KubernetesWorkerNode implements ResourceIDRetrievable {
    private final ResourceID resourceID;

    /**
     * This pattern should be updated when {@link
     * KubernetesResourceManagerDriver#TASK_MANAGER_POD_FORMAT} changed.
     */
    private static final Pattern TASK_MANAGER_POD_PATTERN =
            Pattern.compile("\\S+-taskmanager-([\\d]+)-([\\d]+)");

    KubernetesWorkerNode(ResourceID resourceID) {
        this.resourceID = checkNotNull(resourceID);
    }

    @Override
    public ResourceID getResourceID() {
        return resourceID;
    }

    public long getAttempt() throws ResourceManagerException {
        Matcher matcher = TASK_MANAGER_POD_PATTERN.matcher(resourceID.toString());
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        } else {
            throw new ResourceManagerException(
                    "Error to parse KubernetesWorkerNode from " + resourceID + ".");
        }
    }
}
