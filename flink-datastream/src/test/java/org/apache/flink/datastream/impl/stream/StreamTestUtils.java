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

package org.apache.flink.datastream.impl.stream;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for steam. */
public final class StreamTestUtils {
    public static StreamGraph getStreamGraph(ExecutionEnvironment env) {
        assertThat(env).isInstanceOf(ExecutionEnvironmentImpl.class);
        ExecutionEnvironmentImpl envImpl = (ExecutionEnvironmentImpl) env;
        return envImpl.getStreamGraph();
    }

    /** Assert a transformation has a specific class and type information. */
    public static <OUT> void assertProcessType(
            Transformation<?> transformation,
            Class<?> transformationClass,
            TypeInformation<OUT> typeInformation) {
        assertThat(transformation)
                .isInstanceOf(transformationClass)
                .extracting(Transformation::getOutputType)
                .isEqualTo(typeInformation);
    }

    public static ExecutionEnvironmentImpl getEnv() throws Exception {
        return (ExecutionEnvironmentImpl) ExecutionEnvironment.getInstance();
    }

    /** An implementation of the {@link OneInputStreamProcessFunction} that does nothing. */
    public static class NoOpOneInputStreamProcessFunction
            implements OneInputStreamProcessFunction<Integer, Long> {

        private final Set<StateDeclaration> stateDeclarationSet;

        public NoOpOneInputStreamProcessFunction(Set<StateDeclaration> stateDeclarationSet) {
            this.stateDeclarationSet = stateDeclarationSet;
        }

        public NoOpOneInputStreamProcessFunction() {
            this(new HashSet<>());
        }

        @Override
        public Set<StateDeclaration> usesStates() {
            return stateDeclarationSet;
        }

        @Override
        public void processRecord(Integer record, Collector<Long> output, PartitionedContext ctx) {
            // do nothing.
        }
    }

    /** An implementation of the {@link TwoOutputStreamProcessFunction} that does nothing. */
    public static class NoOpTwoOutputStreamProcessFunction
            implements TwoOutputStreamProcessFunction<Integer, Integer, Long> {

        private final Set<StateDeclaration> stateDeclarationSet;

        public NoOpTwoOutputStreamProcessFunction(Set<StateDeclaration> stateDeclarationSet) {
            this.stateDeclarationSet = stateDeclarationSet;
        }

        public NoOpTwoOutputStreamProcessFunction() {
            this(new HashSet<>());
        }

        @Override
        public Set<StateDeclaration> usesStates() {
            return stateDeclarationSet;
        }

        @Override
        public void processRecord(
                Integer record,
                Collector<Integer> output1,
                Collector<Long> output2,
                TwoOutputPartitionedContext ctx) {
            //  do nothing.
        }
    }

    /**
     * An implementation of the {@link TwoInputNonBroadcastStreamProcessFunction} that does nothing.
     */
    public static class NoOpTwoInputNonBroadcastStreamProcessFunction
            implements TwoInputNonBroadcastStreamProcessFunction<Integer, Long, Long> {

        private final Set<StateDeclaration> stateDeclarationSet;

        public NoOpTwoInputNonBroadcastStreamProcessFunction(
                Set<StateDeclaration> stateDeclarationSet) {
            this.stateDeclarationSet = stateDeclarationSet;
        }

        public NoOpTwoInputNonBroadcastStreamProcessFunction() {
            this(new HashSet<>());
        }

        @Override
        public Set<StateDeclaration> usesStates() {
            return stateDeclarationSet;
        }

        @Override
        public void processRecordFromFirstInput(
                Integer record, Collector<Long> output, PartitionedContext ctx) {
            // do nothing.
        }

        @Override
        public void processRecordFromSecondInput(
                Long record, Collector<Long> output, PartitionedContext ctx) throws Exception {
            // do nothing.
        }
    }

    /**
     * An implementation of the {@link TwoInputBroadcastStreamProcessFunction} that does nothing.
     */
    public static class NoOpTwoInputBroadcastStreamProcessFunction
            implements TwoInputBroadcastStreamProcessFunction<Long, Integer, Long> {

        private final Set<StateDeclaration> stateDeclarationSet;

        public NoOpTwoInputBroadcastStreamProcessFunction(
                Set<StateDeclaration> stateDeclarationSet) {
            this.stateDeclarationSet = stateDeclarationSet;
        }

        public NoOpTwoInputBroadcastStreamProcessFunction() {
            this(new HashSet<>());
        }

        @Override
        public Set<StateDeclaration> usesStates() {
            return stateDeclarationSet;
        }

        @Override
        public void processRecordFromNonBroadcastInput(
                Long record, Collector<Long> output, PartitionedContext ctx) {
            // do nothing.
        }

        @Override
        public void processRecordFromBroadcastInput(
                Integer record, NonPartitionedContext<Long> ctx) {
            // do nothing.
        }
    }
}
