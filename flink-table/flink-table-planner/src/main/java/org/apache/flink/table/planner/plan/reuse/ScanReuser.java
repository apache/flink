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

import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.rules.logical.PushProjectIntoTableSourceScanRule;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgramBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.abilitySpecsWithoutEscaped;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.concatProjectedFields;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.createCalcForScan;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.getAdjustedWatermarkSpec;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.indexOf;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.metadataKeys;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.pickScanWithWatermark;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.projectedFields;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.reusableWithoutAdjust;

/**
 * Reuse sources.
 *
 * <p>When there are projection and metadata push down, the generated source cannot be reused
 * because of the difference of digest. To make source reusable, this class does the following:
 *
 * <ul>
 *   <li>First, find the same source, regardless of their projection and metadata push down.
 *   <li>Union projections for different instances of the same source and create a new instance.
 *   <li>Generate different Calc nodes for different instances.
 *   <li>Replace instances.
 * </ul>
 *
 * <p>For example, plan:
 *
 * <pre>{@code
 * Calc(select=[a, b, c])
 * +- Join(joinType=[InnerJoin], where=[(a = a0)], select=[a, b, a0, c])
 *    :- Exchange(distribution=[hash[a]])
 *    :  +- TableSourceScan(table=[[MyTable, project=[a, b]]], fields=[a, b])
 *    +- Exchange(distribution=[hash[a]])
 *    :  +- TableSourceScan(table=[[MyTable, project=[a, c]]], fields=[a, c])
 * }</pre>
 *
 * <p>Unified to:
 *
 * <pre>{@code
 * Calc(select=[a, b, c])
 * +- Join(joinType=[InnerJoin], where=[(a = a0)], select=[a, b, a0, c])
 *    :- Exchange(distribution=[hash[a]])
 *    :  +- Calc(select=[a, b])
 *    :     +- TableSourceScan(table=[[MyTable, project=[a, b, c]]], fields=[a, b, c])
 *    +- Exchange(distribution=[hash[a]])
 *       +- Calc(select=[a, c])
 *    :     +- TableSourceScan(table=[[MyTable, project=[a, b, c]]], fields=[a, b, c])
 * }</pre>
 *
 * <p>This class do not reuse all sources, sources with same digest will be reused by {@link
 * SubplanReuser}.
 *
 * <p>NOTE: This class not optimize expressions like "$0.child" and "$0", keep both. But {@link
 * PushProjectIntoTableSourceScanRule} will reduce them to only one projection "$0". This is because
 * the subsequent rewrite of watermark push down will become very troublesome. Not only need to
 * adjust the index, but also generate the getter of the nested field. So, connector must deal with
 * "$0.child" and "$0" projection.
 */
public class ScanReuser {

    private static final Comparator<int[]> INT_ARRAY_COMPARATOR =
            (v1, v2) -> {
                int lim = Math.min(v1.length, v2.length);
                int k = 0;
                while (k < lim) {
                    if (v1[k] != v2[k]) {
                        return v1[k] - v2[k];
                    }
                    k++;
                }
                return v1.length - v2.length;
            };

    private final Map<CommonPhysicalTableSourceScan, RelNode> replaceMap = new HashMap<>();

    private final FlinkContext flinkContext;
    private final FlinkTypeFactory flinkTypeFactory;

    public ScanReuser(FlinkContext flinkContext, FlinkTypeFactory flinkTypeFactory) {
        this.flinkContext = flinkContext;
        this.flinkTypeFactory = flinkTypeFactory;
    }

    public List<RelNode> reuseDuplicatedScan(List<RelNode> relNodes) {
        ReusableScanVisitor visitor = new ReusableScanVisitor();
        relNodes.forEach(visitor::go);

        for (List<CommonPhysicalTableSourceScan> reusableNodes :
                visitor.digestToReusableScans().values()) {
            if (reusableNodes.size() < 2 || reusableWithoutAdjust(reusableNodes)) {
                continue;
            }

            if (reusableNodes.stream()
                    .anyMatch(ScanReuserUtils::containsRexNodeSpecAfterProjection)) {
                continue;
            }

            CommonPhysicalTableSourceScan pickScan = pickScanWithWatermark(reusableNodes);
            TableSourceTable pickTable = pickScan.tableSourceTable();
            RexBuilder rexBuilder = pickScan.getCluster().getRexBuilder();

            // 1. Find union fields.
            // Input scan schema: physical projection fields + metadata fields.
            // (See DynamicSourceUtils.validateAndApplyMetadata)
            // So It is safe to collect physical projection fields + metadata fields.
            TreeSet<int[]> allProjectFieldSet = new TreeSet<>(INT_ARRAY_COMPARATOR);
            Set<String> allMetaKeySet = new HashSet<>();
            for (CommonPhysicalTableSourceScan scan : reusableNodes) {
                TableSourceTable source = scan.tableSourceTable();
                allProjectFieldSet.addAll(Arrays.asList(projectedFields(source)));
                allMetaKeySet.addAll(metadataKeys(source));
            }

            int[][] allProjectFields = allProjectFieldSet.toArray(new int[0][]);
            List<String> allMetaKeys = new ArrayList<>(allMetaKeySet);

            // 2. Create new source.
            List<SourceAbilitySpec> specs = abilitySpecsWithoutEscaped(pickTable);

            // 2.1 Create produced type.
            // The source produced type is the input type into the runtime. The format looks as:
            // PHYSICAL COLUMNS + METADATA COLUMNS. While re-compute the source ability specs with
            // source metadata, we need to distinguish between schema type and produced type, which
            // source ability specs use produced type instead of schema type.
            RowType originType =
                    DynamicSourceUtils.createProducedType(
                            pickTable.contextResolvedTable().getResolvedSchema(),
                            pickTable.tableSource());

            // 2.2 Apply projections
            List<SourceAbilitySpec> newSpecs = new ArrayList<>();
            RowType newSourceType =
                    applyPhysicalAndMetadataPushDown(
                            pickTable.tableSource(),
                            originType,
                            newSpecs,
                            concatProjectedFields(
                                    pickTable.contextResolvedTable().getResolvedSchema(),
                                    originType,
                                    allProjectFields,
                                    allMetaKeys),
                            allProjectFields,
                            allMetaKeys);
            specs.addAll(newSpecs);

            // 2.3 Watermark spec
            Optional<WatermarkPushDownSpec> watermarkSpec =
                    getAdjustedWatermarkSpec(pickTable, originType, newSourceType);
            if (watermarkSpec.isPresent()) {
                specs.add(watermarkSpec.get());
                newSourceType = watermarkSpec.get().getProducedType().get();
            }

            // 2.4 Create a new ScanTableSource. ScanTableSource can not be pushed down twice.
            DynamicTableSourceSpec tableSourceSpec =
                    new DynamicTableSourceSpec(pickTable.contextResolvedTable(), specs);
            ScanTableSource newTableSource =
                    tableSourceSpec.getScanTableSource(flinkContext, flinkTypeFactory);

            TableSourceTable newSourceTable =
                    pickTable.replace(
                            newTableSource,
                            ((FlinkTypeFactory) rexBuilder.getTypeFactory())
                                    .buildRelNodeRowType(newSourceType),
                            specs.toArray(new SourceAbilitySpec[0]));

            RelNode newScan = pickScan.copy(newSourceTable);

            // 3. Create projects.
            for (CommonPhysicalTableSourceScan scan : reusableNodes) {
                TableSourceTable source = scan.tableSourceTable();
                int[][] projectedFields = projectedFields(source);
                List<String> metaKeys = metadataKeys(source);

                // Don't need add calc
                if (Arrays.deepEquals(projectedFields, allProjectFields)
                        && metaKeys.equals(allMetaKeys)) {
                    // full project may be pushed into source, update to the new source
                    replaceMap.put(scan, newScan);
                    continue;
                }

                RexProgramBuilder builder = new RexProgramBuilder(newScan.getRowType(), rexBuilder);

                for (int[] field : projectedFields) {
                    int index = indexOf(allProjectFields, field);
                    builder.addProject(index, newScan.getRowType().getFieldNames().get(index));
                }

                for (String key : metaKeys) {
                    int index = allProjectFields.length + allMetaKeys.indexOf(key);
                    builder.addProject(index, newScan.getRowType().getFieldNames().get(index));
                }

                replaceMap.put(scan, createCalcForScan(newScan, builder.getProgram()));
            }
        }

        ReplaceScanWithCalcShuttle replaceShuttle = new ReplaceScanWithCalcShuttle(replaceMap);
        return relNodes.stream()
                .map(rel -> rel.accept(replaceShuttle))
                .collect(Collectors.toList());
    }

    /**
     * Generate sourceAbilitySpecs and newProducedType by projected physical fields and metadata
     * keys.
     */
    private static RowType applyPhysicalAndMetadataPushDown(
            DynamicTableSource source,
            RowType originType,
            List<SourceAbilitySpec> sourceAbilitySpecs,
            int[][] physicalAndMetaFields,
            int[][] projectedPhysicalFields,
            List<String> usedMetadataNames) {
        RowType newProducedType = originType;
        boolean supportsProjectPushDown = source instanceof SupportsProjectionPushDown;
        boolean supportsReadingMeta = source instanceof SupportsReadingMetadata;
        if (supportsProjectPushDown || supportsReadingMeta) {
            newProducedType = (RowType) Projection.of(physicalAndMetaFields).project(originType);
        }
        if (supportsProjectPushDown) {
            sourceAbilitySpecs.add(
                    new ProjectPushDownSpec(projectedPhysicalFields, newProducedType));
        }
        if (supportsReadingMeta) {
            sourceAbilitySpecs.add(new ReadingMetadataSpec(usedMetadataNames, newProducedType));
        }
        return newProducedType;
    }
}
