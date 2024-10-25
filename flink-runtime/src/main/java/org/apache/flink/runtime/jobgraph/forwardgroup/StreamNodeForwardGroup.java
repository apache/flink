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

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Stream node level implement for {@link ForwardGroup}. */
public class StreamNodeForwardGroup implements ForwardGroup {

    private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

    private int maxParallelism = JobVertex.MAX_PARALLELISM_DEFAULT;

    private final Map<StreamNode, List<StreamNode>> chainedStreamNodeGroups = new HashMap<>();

    // For a group of chained stream nodes, their parallelism is consistent. In order to make
    // calculation and usage easier, we only use the start node to calculate forward group.
    public StreamNodeForwardGroup(
            final Set<StreamNode> startNodeGroup,
            final Map<StreamNode, List<StreamNode>> chainedNodesMap) {
        checkNotNull(startNodeGroup);

        Set<Integer> configuredParallelisms =
                startNodeGroup.stream()
                        .map(StreamNode::getParallelism)
                        .filter(val -> val > 0)
                        .collect(Collectors.toSet());

        checkState(configuredParallelisms.size() <= 1);

        if (configuredParallelisms.size() == 1) {
            this.parallelism = configuredParallelisms.iterator().next();
        }

        Set<Integer> configuredMaxParallelisms =
                startNodeGroup.stream()
                        .map(StreamNode::getMaxParallelism)
                        .filter(val -> val > 0)
                        .collect(Collectors.toSet());

        if (!configuredMaxParallelisms.isEmpty()) {
            this.maxParallelism = Collections.min(configuredMaxParallelisms);
            checkState(
                    parallelism == ExecutionConfig.PARALLELISM_DEFAULT
                            || maxParallelism >= parallelism,
                    "There is a start node in the forward group whose maximum parallelism is smaller than the group's parallelism");
        }

        startNodeGroup.forEach(
                node -> this.chainedStreamNodeGroups.put(node, chainedNodesMap.get(node)));
    }

    public void setParallelism(int parallelism) {
        checkState(this.parallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        this.parallelism = parallelism;
    }

    public boolean isParallelismDecided() {
        return parallelism > 0;
    }

    public int getParallelism() {
        checkState(isParallelismDecided());
        return parallelism;
    }

    public boolean isMaxParallelismDecided() {
        return maxParallelism > 0;
    }

    public int getMaxParallelism() {
        checkState(isMaxParallelismDecided());
        return maxParallelism;
    }

    public int size() {
        return chainedStreamNodeGroups.size();
    }

    public Iterable<StreamNode> getStartNodes() {
        return chainedStreamNodeGroups.keySet();
    }

    public Iterable<List<StreamNode>> getChainedStreamNodeGroups() {
        return chainedStreamNodeGroups.values();
    }

    public boolean mergeForwardGroup(StreamNodeForwardGroup targetForwardGroup) {
        checkNotNull(targetForwardGroup);
        if (!ForwardGroupComputeUtil.canTargetMergeIntoSourceForwardGroup(
                this, targetForwardGroup)) {
            return false;
        }

        if (targetForwardGroup == this) {
            return true;
        }

        if (!this.isParallelismDecided() && targetForwardGroup.isParallelismDecided()) {
            this.parallelism = targetForwardGroup.parallelism;
            this.getChainedStreamNodeGroups()
                    .forEach(
                            streamNodes ->
                                    streamNodes.forEach(
                                            streamNode -> streamNode.setParallelism(parallelism)));
        }

        if (!this.isMaxParallelismDecided() && targetForwardGroup.isMaxParallelismDecided()) {
            checkState(
                    this.parallelism == ExecutionConfig.PARALLELISM_DEFAULT
                            || targetForwardGroup.maxParallelism >= this.parallelism);
            this.maxParallelism = targetForwardGroup.maxParallelism;
            this.getChainedStreamNodeGroups()
                    .forEach(
                            streamNodes ->
                                    streamNodes.forEach(
                                            streamNode ->
                                                    streamNode.setMaxParallelism(maxParallelism)));
        }

        if (this.isParallelismDecided() || this.isMaxParallelismDecided()) {
            targetForwardGroup
                    .getChainedStreamNodeGroups()
                    .forEach(
                            streamNodes ->
                                    streamNodes.forEach(
                                            streamNode -> {
                                                if (this.parallelism > 0) {
                                                    streamNode.setParallelism(parallelism);
                                                }
                                                if (this.maxParallelism > 0) {
                                                    streamNode.setMaxParallelism(maxParallelism);
                                                }
                                            }));
        }

        this.chainedStreamNodeGroups.putAll(targetForwardGroup.chainedStreamNodeGroups);

        return true;
    }
}
