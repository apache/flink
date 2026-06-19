/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Planner rule that pushes a {@link Project} past a {@link MultiJoin} by splitting the projection
 * into a projection on top of each child of the {@link MultiJoin}.
 *
 * <p>This rule transforms a pattern like:
 *
 * <pre>
 * Project
 *   MultiJoin
 *     Input1
 *     Input2
 *     ...
 * </pre>
 *
 * <p>Into:
 *
 * <pre>
 * Project
 *   MultiJoin
 *     Project(Input1)
 *     Project(Input2)
 *     ...
 * </pre>
 *
 * <p>This transformation allows the optimizer to push projections down to individual inputs,
 * potentially reducing the amount of data processed in the join operation.
 */
@Value.Enclosing
public class ProjectMultiJoinTransposeRule
        extends RelRule<ProjectMultiJoinTransposeRule.ProjectMultiJoinTransposeRuleConfig> {

    public static final ProjectMultiJoinTransposeRule INSTANCE =
            ProjectMultiJoinTransposeRuleConfig.DEFAULT.toRule();

    public ProjectMultiJoinTransposeRule(ProjectMultiJoinTransposeRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Project originalProject = call.rel(0);
        final MultiJoin multiJoin = call.rel(1);

        // Return if we project all fields of the multi join, as no transformation is needed
        if (RexUtil.isIdentity(originalProject.getProjects(), multiJoin.getRowType())) {
            return false;
        }

        // Check if projections were already pushed down to inputs
        for (RelNode input : multiJoin.getInputs()) {
            if (isProject(input)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Project originalProject = call.rel(0);
        final MultiJoin multiJoin = call.rel(1);
        final RelBuilder relBuilder = call.builder();

        // Gather all referenced fields in projection and join conditions
        final ImmutableBitSet referencedFields =
                collectReferencedFields(originalProject, multiJoin);

        // Create new inputs with projections pushed down
        final TransformedInputs transformedInputs =
                createTransformedInputs(multiJoin, referencedFields, relBuilder);

        // Create field mapping from old to new field positions in the multi join
        final Mappings.TargetMapping fieldMapping =
                createFieldMapping(multiJoin, transformedInputs);

        final MultiJoin newMultiJoin =
                createMultiJoinWithAdjustedParams(multiJoin, transformedInputs, fieldMapping);

        // Update the projection on top of the multi join with the new field mapping
        final List<RexNode> newProjects =
                RexUtil.apply(fieldMapping, originalProject.getProjects());

        relBuilder.push(newMultiJoin);
        relBuilder.project(newProjects, originalProject.getRowType().getFieldNames());
        call.transformTo(relBuilder.build());
    }

    /** Collects all field references from the projection and join conditions. */
    private ImmutableBitSet collectReferencedFields(Project project, MultiJoin multiJoin) {
        final ImmutableBitSet.Builder referencedFieldsBuilder = ImmutableBitSet.builder();
        final RexShuttle fieldCollector =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        referencedFieldsBuilder.set(inputRef.getIndex());
                        return inputRef;
                    }
                };

        // Collect references from projection expressions
        fieldCollector.apply(project.getProjects());

        // Collect references from join filter
        fieldCollector.apply(multiJoin.getJoinFilter());

        // Collect references from post-join filter
        if (multiJoin.getPostJoinFilter() != null) {
            fieldCollector.apply(multiJoin.getPostJoinFilter());
        }

        // Collect references from outer join conditions
        multiJoin.getOuterJoinConditions().forEach(fieldCollector::apply);

        return referencedFieldsBuilder.build();
    }

    /** Creates transformed inputs with projections pushed down to individual inputs. */
    private TransformedInputs createTransformedInputs(
            MultiJoin multiJoin, ImmutableBitSet referencedFields, RelBuilder relBuilder) {

        final List<RelNode> newInputs = new ArrayList<>();
        final List<ImmutableBitSet> newProjFields = new ArrayList<>();
        final List<Integer> newFieldCounts = new ArrayList<>();

        int fieldOffset = 0;
        for (RelNode input : multiJoin.getInputs()) {
            final int inputFieldCount = input.getRowType().getFieldCount();
            final ImmutableBitSet requiredFields =
                    extractRequiredFieldsForInput(referencedFields, fieldOffset, inputFieldCount);

            if (requiredFields.cardinality() == inputFieldCount) {
                // All fields are required, no projection needed
                newInputs.add(input);
                newProjFields.add(null);
                newFieldCounts.add(inputFieldCount);
            } else {
                // Create projection for this input
                final RelNode projectedInput =
                        createProjectionForInput(input, requiredFields, relBuilder);
                newInputs.add(projectedInput);
                newProjFields.add(requiredFields);
                newFieldCounts.add(requiredFields.cardinality());
            }
            fieldOffset += inputFieldCount;
        }

        return new TransformedInputs(newInputs, newProjFields, newFieldCounts);
    }

    /** Extracts the fields required for a specific input from the global referenced fields. */
    private ImmutableBitSet extractRequiredFieldsForInput(
            ImmutableBitSet referencedFields, int fieldOffset, int inputFieldCount) {

        final ImmutableBitSet.Builder requiredFieldsBuilder = ImmutableBitSet.builder();
        for (int bit : referencedFields) {
            if (bit >= fieldOffset && bit < fieldOffset + inputFieldCount) {
                requiredFieldsBuilder.set(bit - fieldOffset);
            }
        }
        return requiredFieldsBuilder.build();
    }

    /** Creates a projection for a specific input based on required fields. */
    private RelNode createProjectionForInput(
            RelNode input, ImmutableBitSet requiredFields, RelBuilder relBuilder) {

        final List<RexNode> newProjects = new ArrayList<>();
        final List<String> newNames = new ArrayList<>();
        final List<RelDataTypeField> inputFields = input.getRowType().getFieldList();

        relBuilder.push(input);
        requiredFields.forEach(
                i -> {
                    newProjects.add(relBuilder.field(i));
                    newNames.add(inputFields.get(i).getName());
                });

        return relBuilder.project(newProjects, newNames).build();
    }

    /** Creates a mapping from old field positions to new field positions. */
    private Mappings.TargetMapping createFieldMapping(
            MultiJoin multiJoin, TransformedInputs transformedInputs) {

        final int[] adjustments = new int[multiJoin.getRowType().getFieldCount()];
        Arrays.fill(adjustments, -1);

        int newFieldOffset = 0;
        int oldFieldOffset = 0;

        for (int inputIndex = 0; inputIndex < transformedInputs.newInputs.size(); inputIndex++) {
            final ImmutableBitSet projection = transformedInputs.newProjFields.get(inputIndex);
            final int oldInputFieldCount =
                    multiJoin.getInputs().get(inputIndex).getRowType().getFieldCount();

            if (projection == null) {
                // No projection on this input, map all fields directly
                for (int fieldIndex = 0; fieldIndex < oldInputFieldCount; fieldIndex++) {
                    adjustments[oldFieldOffset + fieldIndex] = newFieldOffset + fieldIndex;
                }
            } else {
                // Map only projected fields
                for (int fieldIndex = 0; fieldIndex < oldInputFieldCount; fieldIndex++) {
                    if (projection.get(fieldIndex)) {
                        adjustments[oldFieldOffset + fieldIndex] =
                                newFieldOffset + projection.indexOf(fieldIndex);
                    }
                }
            }

            oldFieldOffset += oldInputFieldCount;
            newFieldOffset += transformedInputs.newFieldCounts.get(inputIndex);
        }

        // Convert adjustments array to mapping
        final Map<Integer, Integer> oldToNewMapping = new HashMap<>();
        for (int i = 0; i < adjustments.length; i++) {
            if (adjustments[i] != -1) {
                oldToNewMapping.put(i, adjustments[i]);
            }
        }

        return Mappings.target(
                oldToNewMapping,
                multiJoin.getRowType().getFieldCount(),
                transformedInputs.newInputs.stream()
                        .mapToInt(input -> input.getRowType().getFieldCount())
                        .sum());
    }

    /** Creates a new MultiJoin with updated inputs and adjusted parameters. */
    private MultiJoin createMultiJoinWithAdjustedParams(
            MultiJoin originalMultiJoin,
            TransformedInputs transformedInputs,
            Mappings.TargetMapping fieldMapping) {

        final RelOptCluster cluster = originalMultiJoin.getCluster();

        // Build new row type based on the field mapping
        final RelDataType newRowType = buildNewRowType(originalMultiJoin, fieldMapping);

        // Apply field mapping to all filter conditions
        final RexNode newJoinFilter =
                applyMappingToRexNode(originalMultiJoin.getJoinFilter(), fieldMapping);
        final RexNode newPostJoinFilter =
                applyMappingToRexNode(originalMultiJoin.getPostJoinFilter(), fieldMapping);

        // Apply field mapping to outer join conditions
        final List<RexNode> newOuterJoinConditions =
                applyMappingToOuterJoinConditions(
                        originalMultiJoin.getOuterJoinConditions(), fieldMapping);

        // Apply field mapping to join field reference counts
        final Map<Integer, ImmutableIntList> newJoinFieldRefCountsMap =
                createNewJoinFieldRefCountsMap(originalMultiJoin, transformedInputs, fieldMapping);

        return new MultiJoin(
                cluster,
                originalMultiJoin.getHints(),
                transformedInputs.newInputs,
                newJoinFilter,
                newRowType,
                originalMultiJoin.isFullOuterJoin(),
                newOuterJoinConditions,
                originalMultiJoin.getJoinTypes(),
                transformedInputs.newProjFields,
                com.google.common.collect.ImmutableMap.copyOf(newJoinFieldRefCountsMap),
                newPostJoinFilter);
    }

    /** Builds the new row type for the transformed MultiJoin. */
    private RelDataType buildNewRowType(
            MultiJoin originalMultiJoin, Mappings.TargetMapping fieldMapping) {
        final RelDataTypeFactory typeFactory = originalMultiJoin.getCluster().getTypeFactory();
        final List<RelDataTypeField> newFields = new ArrayList<>();
        final List<RelNode> originalInputs = originalMultiJoin.getInputs();
        final List<RelDataTypeField> originalMultiJoinFields =
                originalMultiJoin.getRowType().getFieldList();

        int globalFieldId = 0;
        for (int inputIndex = 0; inputIndex < originalInputs.size(); inputIndex++) {
            final RelNode originalInput = originalInputs.get(inputIndex);
            final List<RelDataTypeField> inputFields = originalInput.getRowType().getFieldList();

            for (int localFieldIndex = 0; localFieldIndex < inputFields.size(); localFieldIndex++) {
                final int newGlobalFieldId = fieldMapping.getTargetOpt(globalFieldId);

                if (newGlobalFieldId != -1) {
                    // Field exists in the mapping, include it in the new row type
                    final RelDataTypeField originalField =
                            originalMultiJoinFields.get(globalFieldId);
                    newFields.add(
                            new RelDataTypeFieldImpl(
                                    originalField.getName(),
                                    newFields.size(), // Use sequential index for new row type
                                    originalField.getType()));
                }

                globalFieldId++;
            }
        }

        return typeFactory.createStructType(newFields);
    }

    /** Applies field mapping to a RexNode, returning null if the input is null. */
    private RexNode applyMappingToRexNode(RexNode rexNode, Mappings.TargetMapping fieldMapping) {
        return rexNode != null ? RexUtil.apply(fieldMapping, rexNode) : null;
    }

    /** Applies field mapping to outer join conditions. */
    private List<RexNode> applyMappingToOuterJoinConditions(
            List<RexNode> outerJoinConditions, Mappings.TargetMapping fieldMapping) {

        final List<RexNode> newOuterJoinConditions = new ArrayList<>();
        for (RexNode condition : outerJoinConditions) {
            newOuterJoinConditions.add(applyMappingToRexNode(condition, fieldMapping));
        }
        return newOuterJoinConditions;
    }

    /** Creates new join field reference counts map with adjusted field indices. */
    private Map<Integer, ImmutableIntList> createNewJoinFieldRefCountsMap(
            MultiJoin originalMultiJoin,
            TransformedInputs transformedInputs,
            Mappings.TargetMapping fieldMapping) {

        final Map<Integer, ImmutableIntList> newJoinFieldRefCountsMap = new HashMap<>();
        final Map<Integer, ImmutableIntList> originalJoinFieldRefCountsMap =
                originalMultiJoin.getJoinFieldRefCountsMap();

        for (Map.Entry<Integer, ImmutableIntList> entry :
                originalJoinFieldRefCountsMap.entrySet()) {
            final Integer inputIndex = entry.getKey();
            final ImmutableIntList originalRefCounts = entry.getValue();
            final RelNode newInput = transformedInputs.newInputs.get(inputIndex);
            final int newFieldCount = newInput.getRowType().getFieldCount();

            // Create new ref counts array for this input's new field count
            final int[] newRefCounts = new int[newFieldCount];

            // Map the original field indices to new field indices for this input
            for (int originalFieldIndex = 0;
                    originalFieldIndex < originalRefCounts.size();
                    originalFieldIndex++) {
                // Calculate the global field index for this input's field
                final int globalFieldIndex =
                        calculateGlobalFieldIndex(
                                inputIndex, originalFieldIndex, originalMultiJoin.getInputs());
                final int newGlobalFieldIndex = fieldMapping.getTargetOpt(globalFieldIndex);

                if (newGlobalFieldIndex != -1) {
                    // Calculate the new local field index within this input
                    final int newLocalFieldIndex =
                            calculateLocalFieldIndex(
                                    newGlobalFieldIndex, transformedInputs.newInputs, inputIndex);
                    if (newLocalFieldIndex >= 0 && newLocalFieldIndex < newFieldCount) {
                        newRefCounts[newLocalFieldIndex] =
                                originalRefCounts.get(originalFieldIndex);
                    }
                }
            }

            newJoinFieldRefCountsMap.put(inputIndex, ImmutableIntList.of(newRefCounts));
        }

        return newJoinFieldRefCountsMap;
    }

    /** Safely checks if a RelNode is a Project, handling both HepRelVertex. */
    private boolean isProject(RelNode relNode) {
        if (relNode instanceof Project) {
            return true;
        }

        // Handle HepRelVertex (HEP planner)
        if (relNode instanceof HepRelVertex) {
            return ((HepRelVertex) relNode).getCurrentRel() instanceof Project;
        }

        return false;
    }

    /** Calculates the global field index for a field within a specific input. */
    private int calculateGlobalFieldIndex(
            int inputIndex, int localFieldIndex, List<RelNode> inputs) {
        final int globalFieldIndex = calculateFieldOffset(inputIndex, inputs);
        return globalFieldIndex + localFieldIndex;
    }

    /** Calculates the local field index within a specific input from a global field index. */
    private int calculateLocalFieldIndex(
            int globalFieldIndex, List<RelNode> inputs, int currentInputIndex) {
        final int offset = calculateFieldOffset(currentInputIndex, inputs);
        return globalFieldIndex - offset;
    }

    /** Calculates the field offset for a given input index. */
    private int calculateFieldOffset(int inputIndex, List<RelNode> inputs) {
        int offset = 0;
        for (int i = 0; i < inputIndex; i++) {
            offset += inputs.get(i).getRowType().getFieldCount();
        }
        return offset;
    }

    /** Container class for transformed inputs and their metadata. */
    private static class TransformedInputs {
        final List<RelNode> newInputs;
        final List<ImmutableBitSet> newProjFields;
        final List<Integer> newFieldCounts;

        TransformedInputs(
                List<RelNode> newInputs,
                List<ImmutableBitSet> newProjFields,
                List<Integer> newFieldCounts) {
            this.newInputs = newInputs;
            this.newProjFields = newProjFields;
            this.newFieldCounts = newFieldCounts;
        }
    }

    /** Configuration for {@link ProjectMultiJoinTransposeRule}. */
    @Value.Immutable
    public interface ProjectMultiJoinTransposeRuleConfig extends RelRule.Config {
        ProjectMultiJoinTransposeRuleConfig DEFAULT =
                ImmutableProjectMultiJoinTransposeRule.ProjectMultiJoinTransposeRuleConfig.builder()
                        .build()
                        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .as(ProjectMultiJoinTransposeRuleConfig.class)
                        .withOperandFor(Project.class, MultiJoin.class);

        @Override
        default ProjectMultiJoinTransposeRule toRule() {
            return new ProjectMultiJoinTransposeRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default ProjectMultiJoinTransposeRuleConfig withOperandFor(
                Class<? extends Project> projectClass, Class<? extends MultiJoin> multiJoinClass) {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(projectClass)
                                            .oneInput(b1 -> b1.operand(multiJoinClass).anyInputs()))
                    .as(ProjectMultiJoinTransposeRuleConfig.class);
        }
    }
}
