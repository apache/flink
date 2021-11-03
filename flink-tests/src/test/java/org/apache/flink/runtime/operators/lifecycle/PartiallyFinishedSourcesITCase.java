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

package org.apache.flink.runtime.operators.lifecycle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.TestingGraphBuilder;
import org.apache.flink.runtime.operators.lifecycle.validation.DrainingValidator;
import org.apache.flink.runtime.operators.lifecycle.validation.FinishingValidator;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FINISH_SOURCES;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope.ALL_SUBTASKS;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope.SINGLE_SUBTASK;
import static org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.COMPLEX_GRAPH_BUILDER;
import static org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.SIMPLE_GRAPH_BUILDER;
import static org.apache.flink.runtime.operators.lifecycle.validation.TestJobDataFlowValidator.checkDataFlow;
import static org.apache.flink.runtime.operators.lifecycle.validation.TestOperatorLifecycleValidator.checkOperatorsLifecycle;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH;

/**
 * A test suite to check that the operator methods are called according to contract when sources are
 * finishing partially; i.e. either some subtasks of a source have finished; or all subtasks of a
 * single source have finished. The feature was implemented in FLIP-147.
 *
 * <p>The checks are similar to those in {@link StopWithSavepointITCase} {@link
 * StopWithSavepointITCase#withDrain withDrain} except that the final checkpoint doesn't have to be
 * the same.
 */
@RunWith(Parameterized.class)
public class PartiallyFinishedSourcesITCase extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    private MiniClusterWithClientResource miniClusterResource;

    @Before
    public void init() throws Exception {
        Configuration configuration = new Configuration();
        // set failover strategy on the cluster level
        // choose it from the parameter because it may affect the test
        // - "region" is currently the default
        // - "full" is enforced by Adaptive/Reactive scheduler (even when parameterized)
        configuration.set(EXECUTION_FAILOVER_STRATEGY, failoverStrategy);
        miniClusterResource =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(4)
                                .build());
        miniClusterResource.before();
    }

    @After
    public void tearDown() {
        if (miniClusterResource != null) {
            miniClusterResource.after();
        }
    }

    @Parameter(0)
    public TestingGraphBuilder graphBuilder;

    @Parameter(1)
    public TestCommandScope subtaskScope;

    @Parameter(2)
    public boolean failover;

    @Parameter(3)
    public String failoverStrategy;

    @Test
    public void test() throws Exception {
        TestJobWithDescription testJob = buildJob();

        // pick any source operator
        String finishingOperatorID = testJob.sources.iterator().next();
        JobVertexID finishingVertexID = findJobVertexID(testJob, finishingOperatorID);

        TestJobExecutor executor =
                TestJobExecutor.execute(testJob, miniClusterResource)
                        .waitForEvent(CheckpointCompletedEvent.class)
                        .sendOperatorCommand(finishingOperatorID, FINISH_SOURCES, subtaskScope)
                        .waitForSubtasksToFinish(finishingVertexID, subtaskScope)
                        // wait for a checkpoint to complete with a finished subtask(s)
                        // but skip one checkpoint that might be started before finishing
                        .waitForEvent(CheckpointCompletedEvent.class)
                        .waitForEvent(CheckpointCompletedEvent.class);
        if (failover) {
            executor.triggerFailover();
        }
        executor.sendBroadcastCommand(FINISH_SOURCES, ALL_SUBTASKS)
                .waitForTermination()
                .assertFinishedSuccessfully();

        checkOperatorsLifecycle(testJob, new DrainingValidator(), new FinishingValidator());
        checkDataFlow(testJob);
    }

    private TestJobWithDescription buildJob() throws Exception {
        return graphBuilder.build(
                sharedObjects,
                cfg -> cfg.setBoolean(ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true),
                env -> {
                    env.setRestartStrategy(fixedDelayRestart(1, 0));
                    // checkpoints can hang (because of not yet fixed bugs and triggering
                    // checkpoint while the source finishes), so we reduce the timeout to
                    // avoid hanging for too long.
                    env.getCheckpointConfig().setCheckpointTimeout(30000);
                    // but don't fail the job
                    env.getCheckpointConfig()
                            .setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
                    // explicitly set to one to ease avoiding race conditions
                    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
                    env.getCheckpointConfig()
                            // with unaligned checkpoints state size can grow beyond the default
                            // limits of in-memory storage
                            .setCheckpointStorage(TEMPORARY_FOLDER.newFolder().toURI());
                });
    }

    private JobVertexID findJobVertexID(
            TestJobWithDescription testJob, String finishingOperatorID) {
        return stream(testJob.jobGraph.getVertices().spliterator(), false)
                .filter(
                        v ->
                                v.getOperatorIDs().stream()
                                        .anyMatch(idPair -> matches(idPair, finishingOperatorID)))
                .findAny()
                .orElseThrow(() -> new RuntimeException("Vertex not found: " + finishingOperatorID))
                .getID();
    }

    private boolean matches(OperatorIDPair idPair, String operatorID) {
        return idPair.getUserDefinedOperatorID()
                .orElse(idPair.getGeneratedOperatorID())
                .toString()
                .equals(operatorID);
    }

    @Parameterized.Parameters(name = "{0} {1}, failover: {2}, strategy: {3}")
    public static List<Object[]> parameters() {
        List<String> failoverStrategies = asList("full", "region");
        List<List<Object>> rest =
                asList(
                        asList(SIMPLE_GRAPH_BUILDER, SINGLE_SUBTASK, true),
                        asList(COMPLEX_GRAPH_BUILDER, SINGLE_SUBTASK, true),
                        asList(COMPLEX_GRAPH_BUILDER, ALL_SUBTASKS, true),
                        asList(SIMPLE_GRAPH_BUILDER, SINGLE_SUBTASK, false),
                        asList(COMPLEX_GRAPH_BUILDER, SINGLE_SUBTASK, false),
                        asList(COMPLEX_GRAPH_BUILDER, ALL_SUBTASKS, false));
        List<Object[]> result = new ArrayList<>();
        for (String failoverStrategy : failoverStrategies) {
            for (List<Object> otherParams : rest) {
                List<Object> fullList = new ArrayList<>(otherParams);
                fullList.add(failoverStrategy);
                result.add(fullList.toArray());
            }
        }
        return result;
    }
}
