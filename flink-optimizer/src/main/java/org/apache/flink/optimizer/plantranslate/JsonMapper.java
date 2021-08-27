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

package org.apache.flink.optimizer.plantranslate;

import org.apache.flink.api.common.operators.CompilerHints;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;

import static org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator.formatNumber;

public class JsonMapper {

    public static String getOperatorStrategyString(DriverStrategy strategy) {
        return getOperatorStrategyString(strategy, "input 1", "input 2");
    }

    public static String getOperatorStrategyString(
            DriverStrategy strategy, String firstInputName, String secondInputName) {
        if (strategy == null) {
            return "(null)";
        }
        switch (strategy) {
            case SOURCE:
                return "Data Source";
            case SINK:
                return "Data Sink";

            case NONE:
                return "(none)";

            case BINARY_NO_OP:
            case UNARY_NO_OP:
                return "No-Op";

            case MAP:
                return "Map";

            case FLAT_MAP:
                return "FlatMap";

            case MAP_PARTITION:
                return "Map Partition";

            case ALL_REDUCE:
                return "Reduce All";

            case ALL_GROUP_REDUCE:
            case ALL_GROUP_REDUCE_COMBINE:
                return "Group Reduce All";

            case SORTED_REDUCE:
                return "Sorted Reduce";

            case SORTED_PARTIAL_REDUCE:
                return "Sorted Combine/Reduce";

            case SORTED_GROUP_REDUCE:
                return "Sorted Group Reduce";

            case SORTED_GROUP_COMBINE:
                return "Sorted Combine";

            case HYBRIDHASH_BUILD_FIRST:
                return "Hybrid Hash (build: " + firstInputName + ")";

            case HYBRIDHASH_BUILD_SECOND:
                return "Hybrid Hash (build: " + secondInputName + ")";

            case HYBRIDHASH_BUILD_FIRST_CACHED:
                return "Hybrid Hash (CACHED) (build: " + firstInputName + ")";

            case HYBRIDHASH_BUILD_SECOND_CACHED:
                return "Hybrid Hash (CACHED) (build: " + secondInputName + ")";

            case NESTEDLOOP_BLOCKED_OUTER_FIRST:
                return "Nested Loops (Blocked Outer: " + firstInputName + ")";
            case NESTEDLOOP_BLOCKED_OUTER_SECOND:
                return "Nested Loops (Blocked Outer: " + secondInputName + ")";
            case NESTEDLOOP_STREAMED_OUTER_FIRST:
                return "Nested Loops (Streamed Outer: " + firstInputName + ")";
            case NESTEDLOOP_STREAMED_OUTER_SECOND:
                return "Nested Loops (Streamed Outer: " + secondInputName + ")";

            case INNER_MERGE:
                return "Merge";
            case FULL_OUTER_MERGE:
                return "Full Outer Merge";
            case LEFT_OUTER_MERGE:
                return "Left Outer Merge";
            case RIGHT_OUTER_MERGE:
                return "Right Outer Merge";

            case CO_GROUP:
                return "Co-Group";

            default:
                return strategy.name();
        }
    }

    public static String getShipStrategyString(ShipStrategyType shipType) {
        if (shipType == null) {
            return "(null)";
        }
        switch (shipType) {
            case NONE:
                return "(none)";
            case FORWARD:
                return "Forward";
            case BROADCAST:
                return "Broadcast";
            case PARTITION_HASH:
                return "Hash Partition";
            case PARTITION_RANGE:
                return "Range Partition";
            case PARTITION_RANDOM:
                return "Redistribute";
            case PARTITION_FORCED_REBALANCE:
                return "Rebalance";
            case PARTITION_CUSTOM:
                return "Custom Partition";
            default:
                return shipType.name();
        }
    }

    public static String getLocalStrategyString(LocalStrategy localStrategy) {
        if (localStrategy == null) {
            return "(null)";
        }
        switch (localStrategy) {
            case NONE:
                return "(none)";
            case SORT:
                return "Sort";
            case COMBININGSORT:
                return "Sort (combining)";
            default:
                return localStrategy.name();
        }
    }

    public static String getOptimizerPropertiesJson(JsonFactory jsonFactory, PlanNode node) {
        try {
            final StringWriter writer = new StringWriter(256);
            final JsonGenerator gen = jsonFactory.createGenerator(writer);

            final OptimizerNode optNode = node.getOptimizerNode();

            gen.writeStartObject();

            // global properties
            if (node.getGlobalProperties() != null) {
                GlobalProperties gp = node.getGlobalProperties();
                gen.writeArrayFieldStart("global_properties");

                addProperty(gen, "Partitioning", gp.getPartitioning().name());
                if (gp.getPartitioningFields() != null) {
                    addProperty(gen, "Partitioned on", gp.getPartitioningFields().toString());
                }
                if (gp.getPartitioningOrdering() != null) {
                    addProperty(gen, "Partitioning Order", gp.getPartitioningOrdering().toString());
                } else {
                    addProperty(gen, "Partitioning Order", "(none)");
                }
                if (optNode.getUniqueFields() == null || optNode.getUniqueFields().size() == 0) {
                    addProperty(gen, "Uniqueness", "not unique");
                } else {
                    addProperty(gen, "Uniqueness", optNode.getUniqueFields().toString());
                }

                gen.writeEndArray();
            }

            // local properties
            if (node.getLocalProperties() != null) {
                LocalProperties lp = node.getLocalProperties();
                gen.writeArrayFieldStart("local_properties");

                if (lp.getOrdering() != null) {
                    addProperty(gen, "Order", lp.getOrdering().toString());
                } else {
                    addProperty(gen, "Order", "(none)");
                }
                if (lp.getGroupedFields() != null && lp.getGroupedFields().size() > 0) {
                    addProperty(gen, "Grouped on", lp.getGroupedFields().toString());
                } else {
                    addProperty(gen, "Grouping", "not grouped");
                }
                if (optNode.getUniqueFields() == null || optNode.getUniqueFields().size() == 0) {
                    addProperty(gen, "Uniqueness", "not unique");
                } else {
                    addProperty(gen, "Uniqueness", optNode.getUniqueFields().toString());
                }

                gen.writeEndArray();
            }

            // output size estimates
            {
                gen.writeArrayFieldStart("estimates");

                addProperty(
                        gen,
                        "Est. Output Size",
                        optNode.getEstimatedOutputSize() == -1
                                ? "(unknown)"
                                : formatNumber(optNode.getEstimatedOutputSize(), "B"));

                addProperty(
                        gen,
                        "Est. Cardinality",
                        optNode.getEstimatedNumRecords() == -1
                                ? "(unknown)"
                                : formatNumber(optNode.getEstimatedNumRecords()));
                gen.writeEndArray();
            }

            // output node cost
            if (node.getNodeCosts() != null) {
                gen.writeArrayFieldStart("costs");

                addProperty(
                        gen,
                        "Network",
                        node.getNodeCosts().getNetworkCost() == -1
                                ? "(unknown)"
                                : formatNumber(node.getNodeCosts().getNetworkCost(), "B"));
                addProperty(
                        gen,
                        "Disk I/O",
                        node.getNodeCosts().getDiskCost() == -1
                                ? "(unknown)"
                                : formatNumber(node.getNodeCosts().getDiskCost(), "B"));
                addProperty(
                        gen,
                        "CPU",
                        node.getNodeCosts().getCpuCost() == -1
                                ? "(unknown)"
                                : formatNumber(node.getNodeCosts().getCpuCost(), ""));

                addProperty(
                        gen,
                        "Cumulative Network",
                        node.getCumulativeCosts().getNetworkCost() == -1
                                ? "(unknown)"
                                : formatNumber(node.getCumulativeCosts().getNetworkCost(), "B"));
                addProperty(
                        gen,
                        "Cumulative Disk I/O",
                        node.getCumulativeCosts().getDiskCost() == -1
                                ? "(unknown)"
                                : formatNumber(node.getCumulativeCosts().getDiskCost(), "B"));
                addProperty(
                        gen,
                        "Cumulative CPU",
                        node.getCumulativeCosts().getCpuCost() == -1
                                ? "(unknown)"
                                : formatNumber(node.getCumulativeCosts().getCpuCost(), ""));

                gen.writeEndArray();
            }

            // compiler hints
            if (optNode.getOperator().getCompilerHints() != null) {
                CompilerHints hints = optNode.getOperator().getCompilerHints();
                CompilerHints defaults = new CompilerHints();

                String size =
                        hints.getOutputSize() == defaults.getOutputSize()
                                ? "(none)"
                                : String.valueOf(hints.getOutputSize());
                String card =
                        hints.getOutputCardinality() == defaults.getOutputCardinality()
                                ? "(none)"
                                : String.valueOf(hints.getOutputCardinality());
                String width =
                        hints.getAvgOutputRecordSize() == defaults.getAvgOutputRecordSize()
                                ? "(none)"
                                : String.valueOf(hints.getAvgOutputRecordSize());
                String filter =
                        hints.getFilterFactor() == defaults.getFilterFactor()
                                ? "(none)"
                                : String.valueOf(hints.getFilterFactor());

                gen.writeArrayFieldStart("compiler_hints");

                addProperty(gen, "Output Size (bytes)", size);
                addProperty(gen, "Output Cardinality", card);
                addProperty(gen, "Avg. Output Record Size (bytes)", width);
                addProperty(gen, "Filter Factor", filter);

                gen.writeEndArray();
            }

            gen.writeEndObject();

            gen.close();
            return writer.toString();
        } catch (Exception e) {
            return "{}";
        }
    }

    private static void addProperty(JsonGenerator gen, String name, String value)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField("name", name);
        gen.writeStringField("value", value);
        gen.writeEndObject();
    }
}
