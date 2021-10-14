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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.WatermarkReceivedEvent;
import org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.TestingGraphBuilder;
import org.apache.flink.runtime.operators.lifecycle.validation.DrainingValidator;
import org.apache.flink.runtime.operators.lifecycle.validation.FinishingValidator;
import org.apache.flink.runtime.operators.lifecycle.validation.SameCheckpointValidator;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjects;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.List;

import static org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.COMPLEX_GRAPH_BUILDER;
import static org.apache.flink.runtime.operators.lifecycle.graph.TestJobBuilders.SIMPLE_GRAPH_BUILDER;
import static org.apache.flink.runtime.operators.lifecycle.validation.TestJobDataFlowValidator.checkDataFlow;
import static org.apache.flink.runtime.operators.lifecycle.validation.TestOperatorLifecycleValidator.checkOperatorsLifecycle;

/**
 * A test suite to check that the operator methods are called according to contract when the job is
 * stopped with savepoint. The contract was refined in FLIP-147.
 *
 * <p>Checked assumptions:
 *
 * <ol>
 *   <li>Downstream should only be "finished" after all of its the upstreams are
 *   <li>Order of events when finishing an operator:
 *       <ol>
 *         <li>(last data element)
 *         <li>{@link Watermark#MAX_WATERMARK MAX_WATERMARK} (if with drain)
 *         <li>{@link BoundedMultiInput#endInput endInput} (if with drain)
 *         <li>timer service quiesced
 *         <li>{@link StreamOperator#finish() finish} (if with drain; support is planned for
 *             no-drain)
 *         <li>{@link AbstractStreamOperator#snapshotState(StateSnapshotContext) snapshotState} (for
 *             the respective checkpoint)
 *         <li>{@link CheckpointListener#notifyCheckpointComplete notifyCheckpointComplete} (for the
 *             respective checkpoint)
 *         <li>(task termination)
 *       </ol>
 *   <li>Timers can be registered until the operator is finished (though may not fire) (simply
 *       register every 1ms and don't expect any exception)
 *   <li>The same watermark is received
 * </ol>
 *
 * <p>Variants:
 *
 * <ul>
 *   <li>command - with or without drain (MAX_WATERMARK and endInput should be iff drain)
 *   <li>graph - different exchanges (keyBy, forward)
 *   <li>graph - multi-inputs, unions
 *   <li>graph - FLIP-27 and regular sources (should work for both) - FLIP-27 not implemented
 * </ul>
 *
 * <p>Not checked:
 *
 * <ul>
 *   <li>state distribution on recovery (when a new job started from the taken savepoint) (a
 *       separate IT case for partial finishing and state distribution)
 *   <li>re-taking a savepoint after one fails (and job fails over) (as it should not affect
 *       savepoints)
 *   <li>taking a savepoint after recovery (as it should not affect savepoints)
 *   <li>taking a savepoint on a partially completed graph (a separate IT case)
 * </ul>
 */
@RunWith(Parameterized.class)
public class StopWithSavepointITCase extends AbstractTestBase {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Parameter(0)
    public boolean withDrain;

    @Parameter(1)
    public TestingGraphBuilder graphBuilder;

    @Test
    public void test() throws Exception {
        TestJobWithDescription testJob =
                graphBuilder.build(
                        sharedObjects,
                        cfg -> {},
                        env ->
                                env.getCheckpointConfig()
                                        .setCheckpointStorage(
                                                TEMPORARY_FOLDER.newFolder().toURI()));

        TestJobExecutor.execute(testJob, miniClusterResource)
                .waitForEvent(WatermarkReceivedEvent.class)
                .stopWithSavepoint(temporaryFolder, withDrain);

        SameCheckpointValidator sameCheckpointValidator =
                // note: using highest checkpoint will not work with partially finishing tasks
                // in that case, savepoint ID can be inferred from savepoint path
                new SameCheckpointValidator(getHighestCheckpoint(testJob.eventQueue.getAll()));

        if (withDrain) {
            checkOperatorsLifecycle(
                    testJob,
                    sameCheckpointValidator,
                    new DrainingValidator(),
                    /* Currently (1.14), finish is only called with drain; todo: enable after updating production code */
                    new FinishingValidator());
        } else {
            checkOperatorsLifecycle(testJob, sameCheckpointValidator);
        }

        if (withDrain) {
            // currently (1.14), sources do not stop before taking a savepoint and continue emission
            // todo: enable after updating production code
            checkDataFlow(testJob);
        }
    }

    @Parameterized.Parameters(name = "withDrain: {0}, {1}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {true, SIMPLE_GRAPH_BUILDER}, new Object[] {false, SIMPLE_GRAPH_BUILDER},
            new Object[] {true, COMPLEX_GRAPH_BUILDER}, new Object[] {false, COMPLEX_GRAPH_BUILDER},
        };
    }

    private static long getHighestCheckpoint(List<TestEvent> events) {
        return events.stream()
                .filter(e -> e instanceof CheckpointCompletedEvent)
                .mapToLong(e -> ((CheckpointCompletedEvent) e).checkpointID)
                .max()
                .getAsLong();
    }
}
