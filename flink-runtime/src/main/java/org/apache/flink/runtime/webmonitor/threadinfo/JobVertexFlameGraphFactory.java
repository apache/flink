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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.runtime.messages.ThreadInfoSample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Factory class for creating Flame Graph representations. */
public class JobVertexFlameGraphFactory {

    /**
     * Converts {@link JobVertexThreadInfoStats} into a FlameGraph.
     *
     * @param sample Thread details sample containing stack traces.
     * @return FlameGraph data structure
     */
    public static JobVertexFlameGraph createFullFlameGraphFrom(JobVertexThreadInfoStats sample) {
        EnumSet<Thread.State> included = EnumSet.allOf(Thread.State.class);
        return createFlameGraphFromSample(sample, included);
    }

    /**
     * Converts {@link JobVertexThreadInfoStats} into a FlameGraph representing blocked (Off-CPU)
     * threads.
     *
     * <p>Includes threads in states Thread.State.[TIMED_WAITING, BLOCKED, WAITING].
     *
     * @param sample Thread details sample containing stack traces.
     * @return FlameGraph data structure.
     */
    public static JobVertexFlameGraph createOffCpuFlameGraph(JobVertexThreadInfoStats sample) {
        EnumSet<Thread.State> included =
                EnumSet.of(Thread.State.TIMED_WAITING, Thread.State.BLOCKED, Thread.State.WAITING);
        return createFlameGraphFromSample(sample, included);
    }

    /**
     * Converts {@link JobVertexThreadInfoStats} into a FlameGraph representing actively running
     * (On-CPU) threads.
     *
     * <p>Includes threads in states Thread.State.[RUNNABLE, NEW].
     *
     * @param sample Thread details sample containing stack traces.
     * @return FlameGraph data structure
     */
    public static JobVertexFlameGraph createOnCpuFlameGraph(JobVertexThreadInfoStats sample) {
        EnumSet<Thread.State> included = EnumSet.of(Thread.State.RUNNABLE, Thread.State.NEW);
        return createFlameGraphFromSample(sample, included);
    }

    private static JobVertexFlameGraph createFlameGraphFromSample(
            JobVertexThreadInfoStats sample, Set<Thread.State> threadStates) {
        final NodeBuilder root = new NodeBuilder("root");
        for (List<ThreadInfoSample> threadInfoSubSamples : sample.getSamplesBySubtask().values()) {
            for (ThreadInfoSample threadInfo : threadInfoSubSamples) {
                if (threadStates.contains(threadInfo.getThreadState())) {
                    StackTraceElement[] traces = threadInfo.getStackTrace();
                    root.incrementHitCount();
                    NodeBuilder parent = root;
                    for (int i = traces.length - 1; i >= 0; i--) {
                        final String name =
                                traces[i].getClassName()
                                        + "."
                                        + traces[i].getMethodName()
                                        + ":"
                                        + traces[i].getLineNumber();
                        parent = parent.addChild(name);
                    }
                }
            }
        }
        return new JobVertexFlameGraph(sample.getEndTime(), root.toNode());
    }

    private static class NodeBuilder {

        private final Map<String, NodeBuilder> children = new HashMap<>();

        private final String stackTraceLocation;

        private int hitCount = 0;

        NodeBuilder(String stackTraceLocation) {
            this.stackTraceLocation = stackTraceLocation;
        }

        NodeBuilder addChild(String name) {
            final NodeBuilder child = children.computeIfAbsent(name, NodeBuilder::new);
            child.incrementHitCount();
            return child;
        }

        void incrementHitCount() {
            hitCount++;
        }

        private JobVertexFlameGraph.Node toNode() {
            final List<JobVertexFlameGraph.Node> childrenNodes = new ArrayList<>(children.size());
            for (NodeBuilder builderChild : children.values()) {
                childrenNodes.add(builderChild.toNode());
            }
            return new JobVertexFlameGraph.Node(
                    stackTraceLocation, hitCount, Collections.unmodifiableList(childrenNodes));
        }
    }
}
