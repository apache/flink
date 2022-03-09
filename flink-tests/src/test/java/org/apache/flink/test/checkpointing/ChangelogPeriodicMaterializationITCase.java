/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This verifies that checkpointing works correctly for Changelog state backend with materialized
 * state / non-materialized state.
 */
public class ChangelogPeriodicMaterializationITCase
        extends ChangelogPeriodicMaterializationTestBase {

    public ChangelogPeriodicMaterializationITCase(AbstractStateBackend delegatedStateBackend) {
        super(delegatedStateBackend);
    }

    /** Recovery from checkpoint only containing non-materialized state. */
    @Test
    public void testNonMaterialization() throws Exception {
        File checkpointFolder = TEMPORARY_FOLDER.newFolder();
        SharedReference<AtomicBoolean> hasMaterialization =
                sharedObjects.add(new AtomicBoolean(true));
        StreamExecutionEnvironment env =
                getEnv(delegatedStateBackend, checkpointFolder, 1000, 1, Long.MAX_VALUE, 0);
        waitAndAssert(
                buildJobGraph(
                        env,
                        new ControlledSource() {
                            @Override
                            protected void beforeElement(SourceContext<Integer> ctx)
                                    throws Exception {
                                if (getRuntimeContext().getAttemptNumber() == 0
                                        && currentIndex == TOTAL_ELEMENTS / 2) {
                                    waitWhile(() -> completedCheckpointNum.get() <= 0);
                                    hasMaterialization
                                            .get()
                                            .compareAndSet(
                                                    true,
                                                    !getAllStateHandleId(checkpointFolder)
                                                            .isEmpty());
                                    throwArtificialFailure();
                                }
                            }
                        },
                        generateJobID()));
        Preconditions.checkState(!hasMaterialization.get().get());
    }

    /** Recovery from checkpoint containing non-materialized state and materialized state. */
    @Test
    public void testMaterialization() throws Exception {
        File checkpointFolder = TEMPORARY_FOLDER.newFolder();
        SharedReference<AtomicInteger> currentCheckpointNum =
                sharedObjects.add(new AtomicInteger());
        SharedReference<Set<StateHandleID>> currentMaterializationId =
                sharedObjects.add(ConcurrentHashMap.newKeySet());
        StreamExecutionEnvironment env =
                getEnv(delegatedStateBackend, checkpointFolder, 100, 2, 50, 0);
        waitAndAssert(
                buildJobGraph(
                        env,
                        new ControlledSource() {
                            @Override
                            protected void beforeElement(SourceContext<Integer> ctx)
                                    throws Exception {
                                Preconditions.checkState(
                                        getRuntimeContext().getAttemptNumber() <= 2);
                                if (getRuntimeContext().getAttemptNumber() == 0
                                        && currentIndex == TOTAL_ELEMENTS / 4) {
                                    waitWhile(
                                            () -> {
                                                if (completedCheckpointNum.get() <= 0) {
                                                    return true;
                                                }
                                                Set<StateHandleID> allMaterializationId =
                                                        getAllStateHandleId(checkpointFolder);
                                                if (!allMaterializationId.isEmpty()) {
                                                    currentMaterializationId
                                                            .get()
                                                            .addAll(allMaterializationId);
                                                    currentCheckpointNum
                                                            .get()
                                                            .compareAndSet(
                                                                    0,
                                                                    completedCheckpointNum.get());
                                                    return false;
                                                }
                                                return true;
                                            });

                                    throwArtificialFailure();
                                } else if (getRuntimeContext().getAttemptNumber() == 1
                                        && currentIndex == TOTAL_ELEMENTS / 2) {
                                    waitWhile(
                                            () -> {
                                                if (completedCheckpointNum.get()
                                                        <= currentCheckpointNum.get().get()) {
                                                    return true;
                                                }
                                                Set<StateHandleID> allMaterializationId =
                                                        getAllStateHandleId(checkpointFolder);
                                                return allMaterializationId.isEmpty()
                                                        || currentMaterializationId
                                                                .get()
                                                                .equals(allMaterializationId);
                                            });
                                    throwArtificialFailure();
                                }
                            }
                        },
                        generateJobID()));
    }

    @Test
    public void testFailedMaterialization() throws Exception {
        File checkpointFolder = TEMPORARY_FOLDER.newFolder();
        SharedReference<AtomicBoolean> hasFailed = sharedObjects.add(new AtomicBoolean());
        SharedReference<Set<StateHandleID>> currentMaterializationId =
                sharedObjects.add(ConcurrentHashMap.newKeySet());
        StreamExecutionEnvironment env =
                getEnv(
                        new DelegatedStateBackendWrapper(
                                delegatedStateBackend,
                                snapshotResultFuture -> {
                                    if (hasFailed.get().compareAndSet(false, true)) {
                                        throw new RuntimeException();
                                    } else {
                                        return snapshotResultFuture;
                                    }
                                }),
                        checkpointFolder,
                        100,
                        1,
                        10,
                        1);
        env.setParallelism(1);
        waitAndAssert(
                buildJobGraph(
                        env,
                        new ControlledSource() {
                            @Override
                            protected void beforeElement(SourceContext<Integer> ctx)
                                    throws Exception {
                                if (currentIndex == TOTAL_ELEMENTS / 8) {
                                    waitWhile(() -> !hasFailed.get().get());
                                } else if (currentIndex == TOTAL_ELEMENTS / 4) {
                                    waitWhile(
                                            () -> {
                                                Set<StateHandleID> allMaterializationId =
                                                        getAllStateHandleId(checkpointFolder);
                                                if (!allMaterializationId.isEmpty()) {
                                                    currentMaterializationId
                                                            .get()
                                                            .addAll(allMaterializationId);
                                                    return false;
                                                }
                                                return true;
                                            });
                                } else if (currentIndex == TOTAL_ELEMENTS / 2) {
                                    waitWhile(
                                            () -> {
                                                Set<StateHandleID> allMaterializationId =
                                                        getAllStateHandleId(checkpointFolder);
                                                return allMaterializationId.isEmpty()
                                                        || currentMaterializationId
                                                                .get()
                                                                .equals(allMaterializationId);
                                            });
                                }
                            }
                        },
                        generateJobID()));
    }
}
