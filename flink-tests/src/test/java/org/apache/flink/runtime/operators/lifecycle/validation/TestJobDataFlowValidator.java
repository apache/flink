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

package org.apache.flink.runtime.operators.lifecycle.validation;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.operators.lifecycle.TestJobWithDescription;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Check that all data from the upstream reached the respective downstreams. */
public class TestJobDataFlowValidator {
    private static final Logger LOG = LoggerFactory.getLogger(TestJobDataFlowValidator.class);

    public static void checkDataFlow(TestJobWithDescription testJob) {
        Map<String, Map<Integer, OperatorFinishedEvent>> finishEvents = new HashMap<>();
        for (TestEvent ev : testJob.eventQueue.getAll()) {
            if (ev instanceof OperatorFinishedEvent) {
                finishEvents
                        .computeIfAbsent(ev.operatorId, ign -> new HashMap<>())
                        .put(ev.subtaskIndex, ((OperatorFinishedEvent) ev));
            }
        }

        for (JobVertex upstream : testJob.jobGraph.getVertices()) {
            for (IntermediateDataSet produced : upstream.getProducedDataSets()) {
                for (JobEdge edge : produced.getConsumers()) {
                    Optional<String> upstreamID = getTrackedOperatorID(upstream, true, testJob);
                    Optional<String> downstreamID =
                            getTrackedOperatorID(edge.getTarget(), false, testJob);
                    if (upstreamID.isPresent() && downstreamID.isPresent()) {
                        checkDataFlow(upstreamID.get(), downstreamID.get(), edge, finishEvents);
                    } else {
                        LOG.debug("Ignoring edge (untracked operator): {}", edge);
                    }
                }
            }
        }
    }

    /**
     * Check that for each upstream subtask there exists a downstream subtask that received it's
     * latest emitted element.
     */
    private static void checkDataFlow(
            String upstreamID,
            String downstreamID,
            JobEdge edge,
            Map<String, Map<Integer, OperatorFinishedEvent>> finishEvents) {
        LOG.debug(
                "Checking {} edge\n  from {} ({})\n  to {} ({})",
                edge.getDistributionPattern(),
                edge.getSource().getProducer().getName(),
                upstreamID,
                edge.getTarget().getName(),
                downstreamID);

        Map<Integer, OperatorFinishedEvent> downstreamFinishInfo =
                getForOperator(downstreamID, finishEvents);

        Map<Integer, OperatorFinishedEvent> upstreamFinishInfo =
                getForOperator(upstreamID, finishEvents);

        upstreamFinishInfo.forEach(
                (upstreamIndex, upstreamInfo) ->
                        assertTrue(
                                String.format(
                                        "No downstream received %s from %s[%d]; received: %s",
                                        upstreamInfo.lastSent,
                                        upstreamID,
                                        upstreamIndex,
                                        downstreamFinishInfo),
                                anySubtaskReceived(
                                        upstreamID,
                                        upstreamIndex,
                                        upstreamInfo.lastSent,
                                        downstreamFinishInfo.values())));
    }

    private static boolean anySubtaskReceived(
            String upstreamID,
            int upstreamIndex,
            long upstreamValue,
            Collection<OperatorFinishedEvent> downstreamFinishInfo) {
        return downstreamFinishInfo.stream()
                .anyMatch(
                        event -> event.getLastReceived(upstreamID, upstreamIndex) == upstreamValue);
    }

    private static Map<Integer, OperatorFinishedEvent> getForOperator(
            String operatorId, Map<String, Map<Integer, OperatorFinishedEvent>> finishEvents) {
        Map<Integer, OperatorFinishedEvent> events = finishEvents.get(operatorId);
        assertNotNull(
                format(
                        "Operator finish info wasn't collected: %s (collected: %s)",
                        operatorId, finishEvents),
                events);
        return events;
    }

    /**
     * Traverse operators in the chain in the vertex and return the first tracked operator ID. For
     * upstream, start with head, for downstream - with tail (see {@link
     * JobVertex#getOperatorIDs()}). If a chain doesn't contain any tracked operators return
     * nothing.
     */
    private static Optional<String> getTrackedOperatorID(
            JobVertex vertex, boolean upstream, TestJobWithDescription testJob) {
        ListIterator<OperatorIDPair> iterator =
                vertex.getOperatorIDs().listIterator(upstream ? 0 : vertex.getOperatorIDs().size());
        while (upstream ? iterator.hasNext() : iterator.hasPrevious()) {
            OperatorIDPair idPair = upstream ? iterator.next() : iterator.previous();
            String id =
                    idPair.getUserDefinedOperatorID()
                            .orElse(idPair.getGeneratedOperatorID())
                            .toString();
            if (testJob.operatorsWithDataFlowTracking.contains(id)) {
                return Optional.of(id);
            }
        }
        return Optional.empty();
    }
}
