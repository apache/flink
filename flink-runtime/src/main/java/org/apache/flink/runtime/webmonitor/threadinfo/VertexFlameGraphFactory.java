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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Factory class for creating Flame Graph representations. */
public class VertexFlameGraphFactory {

    /**
     * Converts {@link VertexThreadInfoStats} into a FlameGraph.
     *
     * @param sample Thread details sample containing stack traces.
     * @return FlameGraph data structure
     */
    public static VertexFlameGraph createFullFlameGraphFrom(VertexThreadInfoStats sample) {
        EnumSet<Thread.State> included = EnumSet.allOf(Thread.State.class);
        return createFlameGraphFromSample(sample, included);
    }

    /**
     * Converts {@link VertexThreadInfoStats} into a FlameGraph representing blocked (Off-CPU)
     * threads.
     *
     * <p>Includes threads in states Thread.State.[TIMED_WAITING, BLOCKED, WAITING].
     *
     * @param sample Thread details sample containing stack traces.
     * @return FlameGraph data structure.
     */
    public static VertexFlameGraph createOffCpuFlameGraph(VertexThreadInfoStats sample) {
        EnumSet<Thread.State> included =
                EnumSet.of(Thread.State.TIMED_WAITING, Thread.State.BLOCKED, Thread.State.WAITING);
        return createFlameGraphFromSample(sample, included);
    }

    /**
     * Converts {@link VertexThreadInfoStats} into a FlameGraph representing actively running
     * (On-CPU) threads.
     *
     * <p>Includes threads in states Thread.State.[RUNNABLE, NEW].
     *
     * @param sample Thread details sample containing stack traces.
     * @return FlameGraph data structure
     */
    public static VertexFlameGraph createOnCpuFlameGraph(VertexThreadInfoStats sample) {
        EnumSet<Thread.State> included = EnumSet.of(Thread.State.RUNNABLE, Thread.State.NEW);
        return createFlameGraphFromSample(sample, included);
    }

    private static VertexFlameGraph createFlameGraphFromSample(
            VertexThreadInfoStats sample, Set<Thread.State> threadStates) {
        final NodeBuilder root = new NodeBuilder("root");
        for (Collection<ThreadInfoSample> threadInfoSubSamples :
                sample.getSamplesBySubtask().values()) {
            for (ThreadInfoSample threadInfo : threadInfoSubSamples) {
                if (threadStates.contains(threadInfo.getThreadState())) {
                    StackTraceElement[] traces = cleanLambdaNames(threadInfo.getStackTrace());
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
        return new VertexFlameGraph(sample.getEndTime(), root.toNode());
    }

    // Matches class names like
    //   org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$773/0x00000001007f84a0
    //   org.apache.flink.runtime.taskexecutor.IdleTestTask$$Lambda$351/605293351
    private static final Pattern LAMBDA_CLASS_NAME =
            Pattern.compile("(\\$Lambda\\$)\\d+/(0x)?\\p{XDigit}+$");

    private static final Pattern JDK21_LAMBDA_CLASS_NAME =
            Pattern.compile("(\\$\\$Lambda)/(0x)?\\p{XDigit}+$");

    // Drops stack trace elements with class names matching the above regular expression.
    // These elements are useless, because they don't provide any additional information
    // except the fact that a lambda is used (they don't have source information, for example),
    // and also the lambda "class names" can be different across different JVMs, which pollutes
    // flame graphs.
    // Note that Thread.getStackTrace() performs a similar logic - the stack trace returned
    // by this method will not contain lambda references with it. But ThreadMXBean does collect
    // lambdas, so we have to clean them up explicitly.
    private static StackTraceElement[] cleanLambdaNames(StackTraceElement[] stackTrace) {
        StackTraceElement[] result = new StackTraceElement[stackTrace.length];
        final String javaVersion = System.getProperty("java.version");
        final Pattern lambdaClassName =
                javaVersion.compareTo("21") >= 0 ? JDK21_LAMBDA_CLASS_NAME : LAMBDA_CLASS_NAME;
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement element = stackTrace[i];
            Matcher matcher = lambdaClassName.matcher(element.getClassName());
            if (matcher.find()) {
                // org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$773/0x00000001007f84a0
                //  -->
                // org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$0/0x0
                // This ensures that the name is stable across JVMs, but at the same time
                // keeps the stack frame in the call since it has the method name, which
                // may be useful for analysis.
                String newClassName = matcher.replaceFirst("$10/$20");
                result[i] =
                        new StackTraceElement(
                                newClassName,
                                element.getMethodName(),
                                element.getFileName(),
                                element.getLineNumber());
            } else {
                result[i] = element;
            }
        }
        return result;
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

        private VertexFlameGraph.Node toNode() {
            final List<VertexFlameGraph.Node> childrenNodes = new ArrayList<>(children.size());
            for (NodeBuilder builderChild : children.values()) {
                childrenNodes.add(builderChild.toNode());
            }
            return new VertexFlameGraph.Node(
                    stackTraceLocation, hitCount, Collections.unmodifiableList(childrenNodes));
        }
    }
}
