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

import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FINISH_SOURCES;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.TestCommandScope.ALL_SUBTASKS;
import static org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.COMPLEX_GRAPH_BUILDER;
import static org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.SIMPLE_GRAPH_BUILDER;
import static org.apache.flink.runtime.operators.lifecycle.validation.TestJobDataFlowValidator.checkDataFlow;
import static org.apache.flink.runtime.operators.lifecycle.validation.TestOperatorLifecycleValidator.checkOperatorsLifecycle;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH;

/**
 * A test suite to check that the operator methods are called according to contract when sources are
 * finishing normally. The contract was refined in FLIP-147.
 *
 * <p>The checks are similar to those in {@link StopWithSavepointITCase} {@link
 * StopWithSavepointITCase#withDrain withDrain} except that final checkpoint doesn't have to be the
 * same.
 */
@RunWith(Parameterized.class)
public class BoundedSourceITCase extends AbstractTestBase {

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Parameter public TestingGraphBuilder graphBuilder;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] {SIMPLE_GRAPH_BUILDER, COMPLEX_GRAPH_BUILDER};
    }

    @Test
    public void test() throws Exception {
        TestJobWithDescription testJob =
                graphBuilder.build(
                        sharedObjects,
                        cfg -> cfg.setBoolean(ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true),
                        env ->
                                env.getCheckpointConfig()
                                        .setCheckpointStorage(
                                                TEMPORARY_FOLDER.newFolder().toURI()));

        TestJobExecutor.execute(testJob, miniClusterResource)
                .waitForEvent(CheckpointCompletedEvent.class)
                .sendBroadcastCommand(FINISH_SOURCES, ALL_SUBTASKS)
                .waitForTermination()
                .assertFinishedSuccessfully();

        checkOperatorsLifecycle(testJob, new DrainingValidator(), new FinishingValidator());
        checkDataFlow(testJob);
    }
}
