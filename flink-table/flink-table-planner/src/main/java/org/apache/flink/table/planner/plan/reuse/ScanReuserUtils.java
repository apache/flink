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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import scala.Option;

import static org.apache.flink.table.planner.connectors.DynamicSourceUtils.createRequiredMetadataColumns;

/** Utils for {@link ScanReuser}. */
public class ScanReuserUtils {

    private ScanReuserUtils() {}

    /**
     * Found the index of specific projected field in the nested array which is made up of all
     * projected fields index paths.
     */
    public static int indexOf(int[][] projectedFields, int[] fieldIndices) {
        for (int i = 0; i < projectedFields.length; i++) {
            int[] nested = projectedFields[i];
            if (Arrays.equals(nested, fieldIndices)) {
                return i;
            }
        }

        throw new TableException(
                String.format(
                        "Field index path %s is not found in all projected field index paths %s. This should not happen.",
                        fieldIndices, projectedFields));
    }

    /**
     * Returns a list of {@link SourceAbilitySpec} instances associated with a given {@link
     * TableSourceTable} instance, excluding some particular abilities, such as {@link
     * ProjectPushDownSpec}. These abilities don't need before do scan reuse.
     */
    public static List<SourceAbilitySpec> abilitySpecsWithoutEscaped(TableSourceTable table) {
        List<SourceAbilitySpec> ret = new ArrayList<>();
        SourceAbilitySpec[] specs = table.abilitySpecs();
        for (SourceAbilitySpec spec : specs) {
            if (!isEscapeDigest(spec)) {
                ret.add(spec);
            }
        }
        return ret;
    }

    /**
     * Ignore digest is different from Escape digest. Ignore digest indicates that some particular
     * specs, like 'filter=[]', Removing these specs will not affect scanReuse, and can make more
     * pattern match scanReuse. We consider the digest after ignoring to be equivalent to the
     * original digest before ignoring. On the other hand, escape digest means we temporarily shield
     * some specs during compare different scan, and the digest after escaping is not equivalent to
     * the original digest, we need to convert and restore this part of the digest after reusing.
     */
    private static boolean isIgnoreDigest(SourceAbilitySpec spec) {
        if (spec instanceof FilterPushDownSpec) {
            FilterPushDownSpec filterPushDownSpec = (FilterPushDownSpec) spec;
            return filterPushDownSpec.getPredicates().isEmpty();
        }
        return false;
    }

    public static boolean isEscapeDigest(SourceAbilitySpec spec) {
        // WatermarkPushDownSpec is based on index, which is unstable by projection push down.
        // We can ignore Watermark, because sources will produce same watermark.
        return spec instanceof ProjectPushDownSpec
                || spec instanceof ReadingMetadataSpec
                || spec instanceof WatermarkPushDownSpec;
    }

    private static List<String> extraDigestsWithoutEscapedAndIgnored(TableSourceTable table) {
        List<String> ret = new ArrayList<>();
        List<String> digests = table.getSpecDigests();
        SourceAbilitySpec[] specs = table.abilitySpecs();
        for (int i = 0; i < specs.length; i++) {
            SourceAbilitySpec spec = specs[i];
            if (!isEscapeDigest(spec) && !isIgnoreDigest(spec)) {
                ret.add(digests.get(i));
            }
        }
        return ret;
    }

    private static List<String> extraDigestsWithoutIgnored(TableSourceTable table) {
        List<String> ret = new ArrayList<>();
        List<String> digests = table.getSpecDigests();
        SourceAbilitySpec[] specs = table.abilitySpecs();
        for (int i = 0; i < specs.length; i++) {
            SourceAbilitySpec spec = specs[i];
            if (!isIgnoreDigest(spec)) {
                ret.add(digests.get(i));
            }
        }
        return ret;
    }

    /**
     * Contains {@link SourceAbilitySpec#needAdjustFieldReferenceAfterProjection()} spec after
     * projection push down except {@link WatermarkPushDownSpec}. We have customized the solution
     * for watermark push down.
     */
    public static boolean containsRexNodeSpecAfterProjection(CommonPhysicalTableSourceScan table) {
        SourceAbilitySpec[] specs = table.tableSourceTable().abilitySpecs();
        boolean hasProjection = false;
        for (SourceAbilitySpec spec : specs) {
            if (spec instanceof ProjectPushDownSpec || spec instanceof ReadingMetadataSpec) {
                hasProjection = true;
            } else if (hasProjection
                    && spec.needAdjustFieldReferenceAfterProjection()
                    && !(spec instanceof WatermarkPushDownSpec)) {
                return true;
            }
        }
        return false;
    }

    /** Watermark push down must be after projection push down, so we need to adjust its index. */
    public static Optional<WatermarkPushDownSpec> getAdjustedWatermarkSpec(
            TableSourceTable table, RowType oldSourceType, RowType newSourceType) {
        for (SourceAbilitySpec spec : table.abilitySpecs()) {
            if (spec instanceof WatermarkPushDownSpec) {
                return Optional.of(
                        adjustWatermarkIndex(
                                table.contextResolvedTable().getResolvedSchema(),
                                oldSourceType,
                                newSourceType,
                                (WatermarkPushDownSpec) spec));
            }
            if (spec.getProducedType().isPresent()) {
                oldSourceType = spec.getProducedType().get();
            }
        }
        return Optional.empty();
    }

    private static WatermarkPushDownSpec adjustWatermarkIndex(
            ResolvedSchema tableSchema,
            RowType oldSourceType,
            RowType newSourceType,
            WatermarkPushDownSpec spec) {
        List<String> newFieldNames = newSourceType.getFieldNames();
        String rowtimeColumn = tableSchema.getWatermarkSpecs().get(0).getRowtimeAttribute();
        if (newFieldNames.contains(rowtimeColumn)) {
            List<RowType.RowField> fields = new ArrayList<>();
            for (int i = 0; i < newSourceType.getFieldCount(); i++) {
                String name = newFieldNames.get(i);
                LogicalType type = newSourceType.getTypeAt(i);
                if (name.equals(rowtimeColumn)) {
                    type = new TimestampType(type.isNullable(), TimestampKind.ROWTIME, 3);
                }
                fields.add(new RowType.RowField(name, type));
            }
            newSourceType = new RowType(fields);
        }
        RexNode newExpr =
                adjustRexNodeIndex(
                                oldSourceType,
                                newSourceType,
                                Collections.singletonList(spec.getWatermarkExpr()))
                        .get(0);
        return spec.copy(newExpr, newSourceType);
    }

    private static List<RexNode> adjustRexNodeIndex(
            RowType oldSourceType, RowType newSourceType, List<RexNode> rexNodes) {
        List<String> oldFieldNames = oldSourceType.getFieldNames();
        List<String> newFieldNames = newSourceType.getFieldNames();
        RexShuttle shuttle =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        String name = oldFieldNames.get(inputRef.getIndex());
                        int newIndex = newFieldNames.indexOf(name);
                        return new RexInputRef(newIndex, inputRef.getType());
                    }
                };
        return rexNodes.stream().map(node -> node.accept(shuttle)).collect(Collectors.toList());
    }

    public static Calc createCalcForScan(RelNode input, RexProgram program) {
        return input instanceof StreamPhysicalTableSourceScan
                ? new StreamPhysicalCalc(
                        input.getCluster(),
                        input.getTraitSet(),
                        input,
                        program,
                        program.getOutputRowType())
                : new BatchPhysicalCalc(
                        input.getCluster(),
                        input.getTraitSet(),
                        input,
                        program,
                        program.getOutputRowType());
    }

    public static int[][] projectedFields(TableSourceTable source) {
        ResolvedSchema schema = source.contextResolvedTable().getResolvedSchema();
        ProjectPushDownSpec project =
                getAbilitySpec(source.abilitySpecs(), ProjectPushDownSpec.class);
        if (project == null) {
            return IntStream.range(0, schema.toPhysicalRowDataType().getChildren().size())
                    .mapToObj(value -> new int[] {value})
                    .toArray(int[][]::new);
        } else {
            return project.getProjectedFields();
        }
    }

    public static List<String> metadataKeys(TableSourceTable source) {
        ResolvedSchema schema = source.contextResolvedTable().getResolvedSchema();
        ReadingMetadataSpec meta = getAbilitySpec(source.abilitySpecs(), ReadingMetadataSpec.class);
        return meta == null
                ? createRequiredMetadataColumns(schema, source.tableSource()).stream()
                        .map(col -> col.getMetadataKey().orElse(col.getName()))
                        .collect(Collectors.toList())
                : meta.getMetadataKeys();
    }

    public static int[][] concatProjectedFields(
            ResolvedSchema schema,
            RowType originType,
            int[][] physicalFields,
            List<String> metaKeys) {
        // Because of metadata column support alias name, we cannot use metaKeys to find the index
        // of metadataColumn in origin table rowType. So we create a map from metadataKeys to
        // metadataColumns to find the metadata column name by key.
        Map<String, Column.MetadataColumn> metadataKeysToMetadataColumns =
                DynamicSourceUtils.createMetadataKeysToMetadataColumnsMap(schema);

        List<String> producedNames = originType.getFieldNames();
        return Stream.concat(
                        Arrays.stream(physicalFields),
                        metaKeys.stream()
                                .map(
                                        metaKey ->
                                                metadataKeysToMetadataColumns
                                                        .get(metaKey)
                                                        .getName())
                                .map(producedNames::indexOf)
                                .map(i -> new int[] {i}))
                .toArray(int[][]::new);
    }

    public static boolean reusableWithoutAdjust(List<? extends RelNode> reusableNodes) {
        String digest = null;
        for (RelNode scan : reusableNodes) {
            String currentDigest = getDigest((CommonPhysicalTableSourceScan) scan, false);
            if (digest == null) {
                digest = currentDigest;
            } else {
                if (!digest.equals(currentDigest)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Get the digest of {@link CommonPhysicalTableSourceScan}, which ignoring certain {@link
     * SourceAbilitySpec}.
     *
     * @param scan input {@link CommonPhysicalTableSourceScan}.
     * @param withoutEscape Whether to include these escape {@link SourceAbilitySpec}s in returned
     *     digest.
     * @return the digest that ignore certain {@link SourceAbilitySpec}.
     */
    public static String getDigest(CommonPhysicalTableSourceScan scan, boolean withoutEscape) {
        TableSourceTable table = scan.tableSourceTable();
        List<String> digest = new ArrayList<>();
        digest.addAll(table.getNames());

        // input should be the first item
        if (!scan.getInputs().isEmpty()) {
            digest.add(
                    "input=["
                            + scan.getInputs().stream()
                                    .map(RelNode::getDigest)
                                    .collect(Collectors.joining(","))
                            + "]");
        }

        if (withoutEscape) {
            digest.addAll(extraDigestsWithoutEscapedAndIgnored(table));
        } else {
            digest.addAll(extraDigestsWithoutIgnored(table));
        }

        if (!scan.getHints().isEmpty()) {
            digest.add("hints=[" + scan.hintsDigest() + "]");
        }

        Option<String> snapshot = scan.extractSnapshotVersion();
        if (snapshot.isDefined()) {
            digest.add("version=" + snapshot.getOrElse(() -> ""));
        }

        return digest.toString();
    }

    @SuppressWarnings("unchecked")
    public static <T extends SourceAbilitySpec> T getAbilitySpec(
            SourceAbilitySpec[] abilitySpecs, Class<T> specClass) {
        return (T)
                Arrays.stream(abilitySpecs)
                        .filter(spec -> spec.getClass().equals(specClass))
                        .findFirst()
                        .orElse(null);
    }

    public static CommonPhysicalTableSourceScan pickScanWithWatermark(
            List<CommonPhysicalTableSourceScan> scans) {
        Predicate<CommonPhysicalTableSourceScan> containsWatermark =
                scan ->
                        Arrays.stream(scan.tableSourceTable().abilitySpecs())
                                .anyMatch(spec -> spec instanceof WatermarkPushDownSpec);
        return scans.stream().filter(containsWatermark).findFirst().orElse(scans.get(0));
    }
}
