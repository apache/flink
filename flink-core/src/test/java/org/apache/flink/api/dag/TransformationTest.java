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

package org.apache.flink.api.dag;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Transformation}. */
class TransformationTest {

    private Transformation<Void> transformation;

    @BeforeEach
    public void setUp() {
        transformation = new TestTransformation<>("t", null, 1);
    }

    @Test
    void testGetNewNodeIdIsThreadSafe() throws Exception {
        final int numThreads = 10;
        final int numIdsPerThread = 100;

        final List<CheckedThread> threads = new ArrayList<>();

        final OneShotLatch startLatch = new OneShotLatch();

        final List<List<Integer>> idLists = Collections.synchronizedList(new ArrayList<>());
        for (int x = 0; x < numThreads; x++) {
            threads.add(
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            startLatch.await();

                            final List<Integer> ids = new ArrayList<>();
                            for (int c = 0; c < numIdsPerThread; c++) {
                                ids.add(Transformation.getNewNodeId());
                            }
                            idLists.add(ids);
                        }
                    });
        }
        threads.forEach(Thread::start);

        startLatch.trigger();

        for (CheckedThread thread : threads) {
            thread.sync();
        }

        final Set<Integer> deduplicatedIds =
                idLists.stream().flatMap(List::stream).collect(Collectors.toSet());

        assertThat(numThreads * numIdsPerThread).isEqualTo(deduplicatedIds.size());
    }

    @Test
    void testDeclareManagedMemoryUseCase() {
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, 123);
        transformation.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.STATE_BACKEND);
        assertThat(
                        transformation
                                .getManagedMemoryOperatorScopeUseCaseWeights()
                                .get(ManagedMemoryUseCase.OPERATOR))
                .isEqualTo(123);
        assertThat(transformation.getManagedMemorySlotScopeUseCases())
                .contains(ManagedMemoryUseCase.STATE_BACKEND);
    }

    @Test
    void testDeclareManagedMemoryOperatorScopeUseCaseFailWrongScope() {
        assertThatThrownBy(
                        () ->
                                transformation.declareManagedMemoryUseCaseAtOperatorScope(
                                        ManagedMemoryUseCase.PYTHON, 123))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDeclareManagedMemoryOperatorScopeUseCaseFailZeroWeight() {
        assertThatThrownBy(
                        () ->
                                transformation.declareManagedMemoryUseCaseAtOperatorScope(
                                        ManagedMemoryUseCase.OPERATOR, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDeclareManagedMemoryOperatorScopeUseCaseFailNegativeWeight() {
        assertThatThrownBy(
                        () ->
                                transformation.declareManagedMemoryUseCaseAtOperatorScope(
                                        ManagedMemoryUseCase.OPERATOR, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDeclareManagedMemorySlotScopeUseCaseFailWrongScope() {
        assertThatThrownBy(
                        () ->
                                transformation.declareManagedMemoryUseCaseAtSlotScope(
                                        ManagedMemoryUseCase.OPERATOR))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetResourcesUseCaseFailNullResources() {
        ResourceSpec resourceSpec = ResourceSpec.newBuilder(1.0, 100).build();

        assertThatThrownBy(() -> transformation.setResources(null, resourceSpec))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> transformation.setResources(resourceSpec, null))
                .isInstanceOf(NullPointerException.class);
    }

    /** A test implementation of {@link Transformation}. */
    private static class TestTransformation<T> extends Transformation<T> {

        public TestTransformation(String name, TypeInformation<T> outputType, int parallelism) {
            super(name, outputType, parallelism);
        }

        @Override
        protected List<Transformation<?>> getTransitivePredecessorsInternal() {
            return Collections.emptyList();
        }

        @Override
        public List<Transformation<?>> getInputs() {
            return Collections.emptyList();
        }
    }
}
