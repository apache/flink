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

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorWithName;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.iterative.event.AllWorkersDoneEvent;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.iterative.event.WorkerDoneEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The task responsible for synchronizing all iteration heads, implemented as an output task. This
 * task will never see any data. In each superstep, it simply waits until it has received a {@link
 * WorkerDoneEvent} from each head and will send back an {@link AllWorkersDoneEvent} to signal that
 * the next superstep can begin.
 */
public class IterationSynchronizationSinkTask extends AbstractInvokable implements Terminable {

    private static final Logger log =
            LoggerFactory.getLogger(IterationSynchronizationSinkTask.class);

    private MutableRecordReader<IntValue> headEventReader;

    private SyncEventHandler eventHandler;

    private ConvergenceCriterion<Value> convergenceCriterion;

    private ConvergenceCriterion<Value> implicitConvergenceCriterion;

    private Map<String, Aggregator<?>> aggregators;

    private String convergenceAggregatorName;

    private String implicitConvergenceAggregatorName;

    private int currentIteration = 1;

    private int maxNumberOfIterations;

    private final AtomicBoolean terminated = new AtomicBoolean(false);

    // --------------------------------------------------------------------------------------------

    /**
     * Create an Invokable task and set its environment.
     *
     * @param environment The environment assigned to this invokable.
     */
    public IterationSynchronizationSinkTask(Environment environment) {
        super(environment);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void invoke() throws Exception {
        this.headEventReader =
                new MutableRecordReader<>(
                        getEnvironment().getInputGate(0),
                        getEnvironment().getTaskManagerInfo().getTmpDirectories());

        TaskConfig taskConfig = new TaskConfig(getTaskConfiguration());

        // store all aggregators
        this.aggregators = new HashMap<>();
        for (AggregatorWithName<?> aggWithName :
                taskConfig.getIterationAggregators(getUserCodeClassLoader())) {
            aggregators.put(aggWithName.getName(), aggWithName.getAggregator());
        }

        // store the aggregator convergence criterion
        if (taskConfig.usesConvergenceCriterion()) {
            convergenceCriterion = taskConfig.getConvergenceCriterion(getUserCodeClassLoader());
            convergenceAggregatorName = taskConfig.getConvergenceCriterionAggregatorName();
            Preconditions.checkNotNull(convergenceAggregatorName);
        }

        // store the default aggregator convergence criterion
        if (taskConfig.usesImplicitConvergenceCriterion()) {
            implicitConvergenceCriterion =
                    taskConfig.getImplicitConvergenceCriterion(getUserCodeClassLoader());
            implicitConvergenceAggregatorName =
                    taskConfig.getImplicitConvergenceCriterionAggregatorName();
            Preconditions.checkNotNull(implicitConvergenceAggregatorName);
        }

        maxNumberOfIterations = taskConfig.getNumberOfIterations();

        // set up the event handler
        int numEventsTillEndOfSuperstep =
                taskConfig.getNumberOfEventsUntilInterruptInIterativeGate(0);
        eventHandler =
                new SyncEventHandler(
                        numEventsTillEndOfSuperstep,
                        aggregators,
                        getEnvironment().getUserCodeClassLoader().asClassLoader());
        headEventReader.registerTaskEventListener(eventHandler, WorkerDoneEvent.class);

        IntValue dummy = new IntValue();

        while (!terminationRequested()) {

            if (log.isInfoEnabled()) {
                log.info(formatLogString("starting iteration [" + currentIteration + "]"));
            }

            // this call listens for events until the end-of-superstep is reached
            readHeadEventChannel(dummy);

            if (log.isInfoEnabled()) {
                log.info(formatLogString("finishing iteration [" + currentIteration + "]"));
            }

            if (checkForConvergence()) {
                if (log.isInfoEnabled()) {
                    log.info(
                            formatLogString(
                                    "signaling that all workers are to terminate in iteration ["
                                            + currentIteration
                                            + "]"));
                }

                requestTermination();
                sendToAllWorkers(new TerminationEvent());
            } else {
                if (log.isInfoEnabled()) {
                    log.info(
                            formatLogString(
                                    "signaling that all workers are done in iteration ["
                                            + currentIteration
                                            + "]"));
                }

                AllWorkersDoneEvent allWorkersDoneEvent = new AllWorkersDoneEvent(aggregators);
                sendToAllWorkers(allWorkersDoneEvent);

                // reset all aggregators
                for (Aggregator<?> agg : aggregators.values()) {
                    agg.reset();
                }
                currentIteration++;
            }
        }
    }

    private boolean checkForConvergence() {
        if (maxNumberOfIterations == currentIteration) {
            if (log.isInfoEnabled()) {
                log.info(
                        formatLogString(
                                "maximum number of iterations ["
                                        + currentIteration
                                        + "] reached, terminating..."));
            }
            return true;
        }

        if (convergenceAggregatorName != null) {
            @SuppressWarnings("unchecked")
            Aggregator<Value> aggregator =
                    (Aggregator<Value>) aggregators.get(convergenceAggregatorName);
            if (aggregator == null) {
                throw new RuntimeException("Error: Aggregator for convergence criterion was null.");
            }

            Value aggregate = aggregator.getAggregate();

            if (convergenceCriterion.isConverged(currentIteration, aggregate)) {
                if (log.isInfoEnabled()) {
                    log.info(
                            formatLogString(
                                    "convergence reached after ["
                                            + currentIteration
                                            + "] iterations, terminating..."));
                }
                return true;
            }
        }

        if (implicitConvergenceAggregatorName != null) {
            @SuppressWarnings("unchecked")
            Aggregator<Value> aggregator =
                    (Aggregator<Value>) aggregators.get(implicitConvergenceAggregatorName);
            if (aggregator == null) {
                throw new RuntimeException(
                        "Error: Aggregator for default convergence criterion was null.");
            }

            Value aggregate = aggregator.getAggregate();

            if (implicitConvergenceCriterion.isConverged(currentIteration, aggregate)) {
                if (log.isInfoEnabled()) {
                    log.info(
                            formatLogString(
                                    "empty workset convergence reached after ["
                                            + currentIteration
                                            + "] iterations, terminating..."));
                }
                return true;
            }
        }

        return false;
    }

    private void readHeadEventChannel(IntValue rec) throws IOException {
        // reset the handler
        eventHandler.resetEndOfSuperstep();

        // read (and thereby process all events in the handler's event handling functions)
        try {
            if (this.headEventReader.next(rec)) {
                throw new RuntimeException("Synchronization task must not see any records!");
            }
        } catch (InterruptedException iex) {
            // sanity check
            if (!(eventHandler.isEndOfSuperstep())) {
                throw new RuntimeException(
                        "Event handler interrupted without reaching end-of-superstep.");
            }
        }
    }

    private void sendToAllWorkers(TaskEvent event) throws IOException, InterruptedException {
        headEventReader.sendTaskEvent(event);
    }

    private String formatLogString(String message) {
        return BatchTask.constructLogString(
                message, getEnvironment().getTaskInfo().getTaskName(), this);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public boolean terminationRequested() {
        return terminated.get();
    }

    @Override
    public void requestTermination() {
        terminated.set(true);
    }
}
