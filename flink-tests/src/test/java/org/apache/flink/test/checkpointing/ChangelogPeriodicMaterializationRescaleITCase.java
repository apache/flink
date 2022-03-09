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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.test.util.TestUtils.getMostRecentCompletedCheckpoint;

/**
 * This verifies that rescale works correctly for Changelog state backend with materialized state /
 * non-materialized state.
 */
public class ChangelogPeriodicMaterializationRescaleITCase
        extends ChangelogPeriodicMaterializationTestBase {

    public ChangelogPeriodicMaterializationRescaleITCase(
            AbstractStateBackend delegatedStateBackend) {
        super(delegatedStateBackend);
    }

    @Test
    public void testRescaleOut() throws Exception {
        testRescale(NUM_SLOTS / 2, NUM_SLOTS);
    }

    @Test
    public void testRescaleIn() throws Exception {
        testRescale(NUM_SLOTS, NUM_SLOTS / 2);
    }

    private void testRescale(int firstParallelism, int secondParallelism) throws Exception {
        File firstCheckpointFolder = TEMPORARY_FOLDER.newFolder();
        JobID firstJobID = generateJobID();
        SharedReference<Set<StateHandleID>> currentMaterializationId =
                sharedObjects.add(ConcurrentHashMap.newKeySet());

        JobGraph firstJobGraph =
                buildJobGraph(
                        getEnv(firstCheckpointFolder, firstParallelism),
                        new ControlledSource() {
                            @Override
                            protected void beforeElement(SourceContext<Integer> ctx)
                                    throws Exception {
                                if (currentIndex == TOTAL_ELEMENTS / 4) {
                                    waitWhile(
                                            () -> {
                                                if (completedCheckpointNum.get() <= 0) {
                                                    return true;
                                                }
                                                Set<StateHandleID> allMaterializationId =
                                                        getAllStateHandleId(firstCheckpointFolder);
                                                if (!allMaterializationId.isEmpty()) {
                                                    currentMaterializationId
                                                            .get()
                                                            .addAll(allMaterializationId);
                                                    return false;
                                                }
                                                return true;
                                            });
                                } else if (currentIndex > TOTAL_ELEMENTS / 4) {
                                    throwArtificialFailure();
                                }
                            }
                        },
                        firstJobID);

        try {
            cluster.getMiniCluster().submitJob(firstJobGraph).get();
            cluster.getMiniCluster().requestJobResult(firstJobGraph.getJobID()).get();
        } catch (Exception ex) {
            Preconditions.checkState(
                    ExceptionUtils.findThrowable(ex, ArtificialFailure.class).isPresent());
        }

        File secondCheckpointFolder = TEMPORARY_FOLDER.newFolder();
        JobID secondJobId = generateJobID();
        JobGraph jobGraph =
                buildJobGraph(
                        getEnv(secondCheckpointFolder, secondParallelism),
                        new ControlledSource() {
                            @Override
                            protected void beforeElement(SourceContext<Integer> ctx)
                                    throws Exception {
                                if (currentIndex == TOTAL_ELEMENTS / 2) {
                                    waitWhile(
                                            () -> {
                                                Set<StateHandleID> allMaterializationId =
                                                        getAllStateHandleId(secondCheckpointFolder);
                                                return allMaterializationId.isEmpty()
                                                        || currentMaterializationId
                                                                .get()
                                                                .equals(allMaterializationId);
                                            });
                                }
                            }
                        },
                        secondJobId);
        File checkpoint = getMostRecentCompletedCheckpoint(firstCheckpointFolder);
        jobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(checkpoint.getPath()));
        waitAndAssert(jobGraph);
    }

    private StreamExecutionEnvironment getEnv(File checkpointFolder, int parallelism) {
        StreamExecutionEnvironment env =
                getEnv(delegatedStateBackend, checkpointFolder, 50, 0, 20, 0);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(parallelism);
        return env;
    }
}
