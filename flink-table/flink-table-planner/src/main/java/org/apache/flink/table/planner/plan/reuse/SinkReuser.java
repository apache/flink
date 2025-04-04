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

package org.apache.flink.table.planner.plan.reuse;

import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.calcite.Sink;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.utils.RelExplainUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This checks each sink node to see if it can be reused with another sink node. If so, we will
 * reuse all reusable sink to one instance. This only used in the STATEMENT SET clause with multiple
 * INSERT INTO.
 *
 * <p>Examples in SQL look like:
 *
 * <pre>{@code
 * BEGIN STATEMENT SET;
 * INSERT INTO sink1 SELECT * FROM source1;
 * INSERT INTO sink1 SELECT * FROM source2;
 * INSERT INTO sink2 SELECT * FROM source3;
 * END;
 * }</pre>
 *
 * <p>The plan is as follows:
 *
 * <pre>{@code
 * TableScan1 —— Sink1
 * TableScan2 —— Sink1
 * TableScan3 —— Sink2
 * }</pre>
 *
 * <p>After reused, the plan will be changed as follows:
 *
 * <pre>{@code
 * TableScan1 --\
 *               Union -- Sink1
 * TableScan2 --/
 *
 * TableScan3 —— Sink3
 * }</pre>
 */
public class SinkReuser {
    private final boolean isStreamingMode;

    public SinkReuser(boolean isStreamingMode) {
        this.isStreamingMode = isStreamingMode;
    }

    public List<RelNode> reuseDuplicatedSink(List<RelNode> relNodes) {
        // Find all sinks
        List<Sink> allSinkNodes =
                relNodes.stream()
                        .filter(node -> node instanceof Sink)
                        .map(node -> (Sink) node)
                        .collect(Collectors.toList());
        List<ReusableSinkGroup> reusableSinkGroups = groupReusableSink(allSinkNodes);

        Set<Sink> reusedSinkNodes = reuseSinkAndAddUnion(reusableSinkGroups);

        // Remove all unused sink nodes
        return relNodes.stream()
                .filter(root -> !(root instanceof Sink) || reusedSinkNodes.contains(root))
                .collect(Collectors.toList());
    }

    private Set<Sink> reuseSinkAndAddUnion(List<ReusableSinkGroup> reusableSinkGroups) {
        final Set<Sink> reusedSinkNodes = Collections.newSetFromMap(new IdentityHashMap<>());
        reusableSinkGroups.forEach(
                group -> {
                    List<Sink> originalSinks = group.originalSinks;
                    if (originalSinks.size() <= 1) {
                        Preconditions.checkState(originalSinks.size() == 1);
                        reusedSinkNodes.add(originalSinks.get(0));
                        return;
                    }
                    List<RelNode> allSinkInputs = new ArrayList<>();
                    for (Sink sinkNode : originalSinks) {
                        allSinkInputs.add(sinkNode.getInput());
                    }
                    // Use the first sink node as the final reused sink node
                    Sink reusedSink = originalSinks.get(0);

                    Union unionForReusedSinks;

                    if (isStreamingMode) {
                        unionForReusedSinks =
                                new StreamPhysicalUnion(
                                        reusedSink.getCluster(),
                                        group.inputTraitSet,
                                        allSinkInputs,
                                        true,
                                        // use sink input row type
                                        reusedSink.getRowType());
                    } else {
                        unionForReusedSinks =
                                new BatchPhysicalUnion(
                                        reusedSink.getCluster(),
                                        group.inputTraitSet,
                                        allSinkInputs,
                                        true,
                                        // use sink input row type
                                        reusedSink.getRowType());
                    }

                    reusedSink.replaceInput(0, unionForReusedSinks);
                    reusedSinkNodes.add(reusedSink);
                });
        return reusedSinkNodes;
    }

    /**
     * Grouping sinks that can be reused with each other.
     *
     * @param allSinkNodes in the plan.
     * @return a list contains all grouped sink.
     */
    private List<ReusableSinkGroup> groupReusableSink(List<Sink> allSinkNodes) {
        List<ReusableSinkGroup> reusableSinkGroups = new ArrayList<>();

        for (Sink currentSinkNode : allSinkNodes) {
            boolean canBeReused = false;
            String currentSinkDigest = getDigest(currentSinkNode);
            SinkAbilitySpec[] currentSinkSpecs;
            if (isStreamingMode) {
                currentSinkSpecs = ((StreamPhysicalSink) currentSinkNode).abilitySpecs();
            } else {
                currentSinkSpecs = ((BatchPhysicalSink) currentSinkNode).abilitySpecs();
            }
            RelTraitSet currentInputTraitSet = currentSinkNode.getInput().getTraitSet();
            for (ReusableSinkGroup group : reusableSinkGroups) {
                // Only table sink with the same digest, specs and input trait set can be reused
                if (!(group.digest.equals(currentSinkDigest)
                        && Arrays.equals(group.sinkAbilitySpecs, currentSinkSpecs)
                        && group.inputTraitSet.equals(currentInputTraitSet))) {
                    continue;
                }

                canBeReused = true;
                group.originalSinks.add(currentSinkNode);
                break;
            }

            if (!canBeReused) {
                reusableSinkGroups.add(
                        new ReusableSinkGroup(
                                currentSinkNode,
                                currentSinkDigest,
                                currentSinkSpecs,
                                currentInputTraitSet));
            }
        }
        return reusableSinkGroups;
    }

    public String getDigest(Sink sink) {
        List<String> digest = new ArrayList<>();
        digest.add(sink.contextResolvedTable().getIdentifier().asSummaryString());

        int[][] targetColumns = sink.targetColumns();
        if (targetColumns != null && targetColumns.length > 0) {
            digest.add(
                    "targetColumns=["
                            + Arrays.stream(targetColumns)
                                    .map(Arrays::toString)
                                    .collect(Collectors.joining(","))
                            + "]");
        }

        String fields =
                sink.getRowType().getFieldList().stream()
                        .map(f -> f.getType().toString())
                        .collect(Collectors.joining(", "));
        digest.add("fields=[" + fields + "]");
        if (!sink.hints().isEmpty()) {
            digest.add("hints=" + RelExplainUtil.hintsToString(sink.hints()));
        }

        if (isStreamingMode) {
            digest.add("upsertMaterialize=" + ((StreamPhysicalSink) sink).upsertMaterialize());
        }

        return digest.toString();
    }

    private static class ReusableSinkGroup {
        private final List<Sink> originalSinks = new ArrayList<>();

        private final SinkAbilitySpec[] sinkAbilitySpecs;

        private final RelTraitSet inputTraitSet;

        private final String digest;

        ReusableSinkGroup(
                Sink sink,
                String digest,
                SinkAbilitySpec[] sinkAbilitySpecs,
                RelTraitSet inputTraitSet) {
            this.originalSinks.add(sink);
            this.sinkAbilitySpecs = sinkAbilitySpecs;
            this.inputTraitSet = inputTraitSet;
            this.digest = digest;
        }
    }
}
