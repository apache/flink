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
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.test.util.TestUtils.getMostRecentCompletedCheckpoint;

/** Base class for tests related to switching environment for Changelog state backend. */
public abstract class ChangelogRecoverySwitchEnvTestBase extends ChangelogRecoveryITCaseBase {

    public ChangelogRecoverySwitchEnvTestBase(AbstractStateBackend delegatedStateBackend) {
        super(delegatedStateBackend);
    }

    protected void testSwitchEnv(
            StreamExecutionEnvironment firstEnv, StreamExecutionEnvironment secondEnv)
            throws Exception {
        File firstCheckpointFolder = TEMPORARY_FOLDER.newFolder();
        SharedReference<MiniCluster> miniCluster = sharedObjects.add(cluster.getMiniCluster());
        SharedReference<Set<StateHandleID>> currentMaterializationId =
                sharedObjects.add(ConcurrentHashMap.newKeySet());
        firstEnv.getCheckpointConfig().setCheckpointStorage(firstCheckpointFolder.toURI());
        JobGraph firstJobGraph =
                firstNormalJobGraph(firstEnv, miniCluster, currentMaterializationId);

        try {
            cluster.getMiniCluster().submitJob(firstJobGraph).get();
            cluster.getMiniCluster().requestJobResult(firstJobGraph.getJobID()).get();
        } catch (Exception ex) {
            Preconditions.checkState(
                    ExceptionUtils.findThrowable(ex, ArtificialFailure.class).isPresent());
        }

        File secondCheckpointFolder = TEMPORARY_FOLDER.newFolder();
        secondEnv.getCheckpointConfig().setCheckpointStorage(secondCheckpointFolder.toURI());
        JobGraph jobGraph = nextNormalJobGraph(secondEnv, miniCluster, currentMaterializationId);
        File checkpointFile = getMostRecentCompletedCheckpoint(firstCheckpointFolder);
        jobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(checkpointFile.getPath()));
        waitAndAssert(jobGraph);
    }

    private JobGraph firstNormalJobGraph(
            StreamExecutionEnvironment env,
            SharedReference<MiniCluster> miniCluster,
            SharedReference<Set<StateHandleID>> currentMaterializationId) {
        SharedReference<JobID> jobID = sharedObjects.add(generateJobID());
        return buildJobGraph(
                env,
                new ControlledSource() {
                    @Override
                    protected void beforeElement(SourceContext<Integer> ctx) throws Exception {
                        if (currentIndex == TOTAL_ELEMENTS / 4) {
                            waitWhile(
                                    () -> {
                                        if (completedCheckpointNum.get() <= 0) {
                                            return true;
                                        }
                                        Set<StateHandleID> allMaterializationId =
                                                getAllStateHandleId(jobID.get(), miniCluster.get());
                                        if (!allMaterializationId.isEmpty()) {
                                            currentMaterializationId
                                                    .get()
                                                    .addAll(allMaterializationId);
                                            return false;
                                        }
                                        return true;
                                    });
                        } else if (currentIndex > TOTAL_ELEMENTS / 3) {
                            throwArtificialFailure();
                        }
                    }
                },
                jobID.get());
    }

    private JobGraph nextNormalJobGraph(
            StreamExecutionEnvironment env,
            SharedReference<MiniCluster> miniCluster,
            SharedReference<Set<StateHandleID>> currentMaterializationId) {
        SharedReference<JobID> jobID = sharedObjects.add(generateJobID());
        return buildJobGraph(
                env,
                new ControlledSource() {
                    @Override
                    protected void beforeElement(SourceContext<Integer> ctx) throws Exception {
                        if (currentIndex == TOTAL_ELEMENTS / 2) {
                            waitWhile(
                                    () -> {
                                        Set<StateHandleID> allMaterializationId =
                                                getAllStateHandleId(jobID.get(), miniCluster.get());
                                        return allMaterializationId.isEmpty()
                                                || currentMaterializationId
                                                        .get()
                                                        .equals(allMaterializationId);
                                    });
                        }
                    }
                },
                jobID.get());
    }

    protected JobGraph buildJobGraph(
            StreamExecutionEnvironment env,
            int waitingOnIndex,
            int failIndex,
            SharedReference<MiniCluster> miniCluster) {
        SharedReference<JobID> jobID = sharedObjects.add(generateJobID());
        return buildJobGraph(
                env,
                new ControlledSource() {
                    @Override
                    protected void beforeElement(SourceContext<Integer> ctx) throws Exception {
                        if (currentIndex == waitingOnIndex) {
                            waitWhile(
                                    () ->
                                            getAllStateHandleId(jobID.get(), miniCluster.get())
                                                    .isEmpty());
                        } else if (currentIndex > failIndex) {
                            throwArtificialFailure();
                        }
                    }
                },
                jobID.get());
    }
}
