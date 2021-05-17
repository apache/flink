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

package org.apache.flink.runtime.iterative.task;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetUpdateBarrier;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetUpdateBarrierBroker;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatch;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatchBroker;
import org.apache.flink.runtime.iterative.io.WorksetUpdateOutputCollector;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iteration tail, which runs a driver inside.
 *
 * <p>If the iteration state is updated, the output of this task will be send back to the {@link
 * IterationHeadTask} via a BackChannel for the workset -OR- a HashTable for the solution set.
 * Therefore this task must be scheduled on the same instance as the head. It's also possible for
 * the tail to update *both* the workset and the solution set.
 *
 * <p>If there is a separate solution set tail, the iteration head has to make sure to wait for it
 * to finish.
 */
public class IterationTailTask<S extends Function, OT> extends AbstractIterativeTask<S, OT> {

    private static final Logger log = LoggerFactory.getLogger(IterationTailTask.class);

    private SolutionSetUpdateBarrier solutionSetUpdateBarrier;

    private WorksetUpdateOutputCollector<OT> worksetUpdateOutputCollector;

    // --------------------------------------------------------------------------------------------

    /**
     * Create an Invokable task and set its environment.
     *
     * @param environment The environment assigned to this invokable.
     */
    public IterationTailTask(Environment environment) {
        super(environment);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected void initialize() throws Exception {
        super.initialize();

        // sanity check: the tail has to update either the workset or the solution set
        if (!isWorksetUpdate && !isSolutionSetUpdate) {
            throw new RuntimeException(
                    "The iteration tail doesn't update workset or the solution set.");
        }

        // set the last output collector of this task to reflect the iteration tail state update:
        // a) workset update,
        // b) solution set update, or
        // c) merged workset and solution set update

        Collector<OT> outputCollector = null;
        if (isWorksetUpdate) {
            outputCollector = createWorksetUpdateOutputCollector();

            // we need the WorksetUpdateOutputCollector separately to count the collected elements
            if (isWorksetIteration) {
                worksetUpdateOutputCollector = (WorksetUpdateOutputCollector<OT>) outputCollector;
            }
        }

        if (isSolutionSetUpdate) {
            if (isWorksetIteration) {
                outputCollector = createSolutionSetUpdateOutputCollector(outputCollector);
            }
            // Bulk iteration with termination criterion
            else {
                outputCollector =
                        new Collector<OT>() {
                            @Override
                            public void collect(OT record) {}

                            @Override
                            public void close() {}
                        };
            }

            if (!isWorksetUpdate) {
                solutionSetUpdateBarrier =
                        SolutionSetUpdateBarrierBroker.instance().get(brokerKey());
            }
        }

        setLastOutputCollector(outputCollector);
    }

    @Override
    public void run() throws Exception {

        try {
            SuperstepKickoffLatch nextSuperStepLatch =
                    SuperstepKickoffLatchBroker.instance().get(brokerKey());

            while (this.running && !terminationRequested()) {

                if (log.isInfoEnabled()) {
                    log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
                }

                super.run();

                // check if termination was requested
                verifyEndOfSuperstepState();

                if (isWorksetUpdate && isWorksetIteration) {
                    // aggregate workset update element count
                    long numCollected = worksetUpdateOutputCollector.getElementsCollectedAndReset();
                    worksetAggregator.aggregate(numCollected);
                }

                if (log.isInfoEnabled()) {
                    log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
                }

                if (isWorksetUpdate) {
                    // notify iteration head if responsible for workset update
                    worksetBackChannel.notifyOfEndOfSuperstep();
                } else if (isSolutionSetUpdate) {
                    // notify iteration head if responsible for solution set update
                    solutionSetUpdateBarrier.notifySolutionSetUpdate();
                }

                boolean terminate =
                        nextSuperStepLatch.awaitStartOfSuperstepOrTermination(
                                currentIteration() + 1);
                if (terminate) {
                    requestTermination();
                } else {
                    incrementIterationCounter();
                }
            }
        } finally {
            terminationCompleted();
        }
    }
}
