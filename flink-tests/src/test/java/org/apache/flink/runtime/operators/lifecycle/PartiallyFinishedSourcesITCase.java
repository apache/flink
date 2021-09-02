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

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.TestingGraphBuilder;
import org.apache.flink.runtime.operators.lifecycle.validation.DrainingValidator;
import org.apache.flink.runtime.operators.lifecycle.validation.FinishingValidator;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjects;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import static java.util.stream.StreamSupport.stream;
import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
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
public class PartiallyFinishedSourcesITCase extends AbstractTestBase {

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Parameter(0)
    public TestingGraphBuilder graphBuilder;

    @Parameter(1)
    public TestCommandScope subtaskScope;

    @Parameter(2)
    public boolean failover;

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

    private TestJobWithDescription buildJob() {
        return graphBuilder.build(
                sharedObjects,
                cfg -> cfg.setBoolean(ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true),
                env -> {
                    env.setRestartStrategy(fixedDelayRestart(1, 0));
                    // checkpoints can hang (because of not yet fixed bugs and triggering
                    // checkpoint while the source finishes), so let them timeout quickly
                    env.getCheckpointConfig().setCheckpointTimeout(5000);
                    // but don't fail the job
                    env.getCheckpointConfig()
                            .setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
                    // explicitly set to one to ease avoiding race conditions
                    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
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

    @Parameterized.Parameters(name = "{0} {1}, failover: {2}")
    public static Object[] parameters() {
        return new Object[] {
            new Object[] {SIMPLE_GRAPH_BUILDER, SINGLE_SUBTASK, true},
            new Object[] {COMPLEX_GRAPH_BUILDER, SINGLE_SUBTASK, true},
            new Object[] {COMPLEX_GRAPH_BUILDER, ALL_SUBTASKS, true},
            new Object[] {SIMPLE_GRAPH_BUILDER, SINGLE_SUBTASK, false},
            new Object[] {COMPLEX_GRAPH_BUILDER, SINGLE_SUBTASK, false},
            new Object[] {COMPLEX_GRAPH_BUILDER, ALL_SUBTASKS, false},
        };
    }
}
