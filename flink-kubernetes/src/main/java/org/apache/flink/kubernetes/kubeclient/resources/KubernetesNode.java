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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.annotation.VisibleForTesting;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeCondition;

import java.util.List;

/** Represent KubernetesNode resource in kubernetes. */
public class KubernetesNode extends KubernetesResource<Node> {
    public KubernetesNode(Node internalResource) {
        super(internalResource);
    }

    public boolean isNodeNotReady() {
        List<NodeCondition> nodeConditions = this.getInternalResource().getStatus().getConditions();
        for (NodeCondition nodeCondition : nodeConditions) {
            if (NodeConditions.Ready.name().equals(nodeCondition.getType())) {
                return NodeConditionStatus.False.name().equals(nodeCondition.getStatus())
                        || NodeConditionStatus.Unknown.name().equals(nodeCondition.getStatus());
            }
        }
        return false;
    }

    @VisibleForTesting
    enum NodeConditions {
        /**
         * True if the node is healthy and ready to accept pods, False if the node is not healthy
         * and is not accepting pods, and Unknown if the node controller has not heard from the node
         * in the last node-monitor-grace-period (default is 40 seconds)
         */
        Ready,
        /**
         * True if pressure exists on the disk size—that is, if the disk capacity is low; otherwise
         * False
         */
        DiskPressure,
        /**
         * True if pressure exists on the node memory—that is, if the node memory is low; otherwise
         * False
         */
        MemoryPressure,
        /**
         * True if pressure exists on the processes—that is, if there are too many processes on the
         * node; otherwise False
         */
        PIDPressure,
        /** True if the network for the node is not correctly configured, otherwise False */
        NetworkUnavailable
    }

    @VisibleForTesting
    enum NodeConditionStatus {
        /** The NodeCondition is normal. */
        True,
        /** The NodeCondition is out of whack. */
        False,
        /** The NodeCondition is unknown. */
        Unknown
    }
}
