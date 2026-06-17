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

package org.apache.flink.table.planner.plan.optimize.program;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.trait.DeleteKindTrait;
import org.apache.flink.table.planner.plan.trait.ModifyKind;
import org.apache.flink.table.planner.plan.trait.ModifyKindSet;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTrait;
import org.apache.flink.table.planner.plan.trait.UpdateKindTrait;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** An optimize program to infer ChangelogMode for every physical node. */
public class FlinkChangelogModeInferenceProgram
        implements FlinkOptimizeProgram<StreamOptimizeContext> {

    @Override
    public RelNode optimize(RelNode root, StreamOptimizeContext context) {
        // step1: satisfy ModifyKindSet trait
        StreamPhysicalRel physicalRoot = (StreamPhysicalRel) root;
        StreamPhysicalRel rootWithModifyKindSet =
                new SatisfyModifyKindSetTraitVisitor()
                        .visit(
                                physicalRoot,
                                // we do not propagate the ModifyKindSet requirement and requester
                                // among blocks; set default ModifyKindSet requirement and requester
                                // for root
                                ModifyKindSetTrait.ALL_CHANGES(),
                                "ROOT");

        // step2: satisfy UpdateKind trait
        ModifyKindSet rootModifyKindSet =
                ChangelogModeInferenceUtils.getModifyKindSet(rootWithModifyKindSet);
        // use the required UpdateKindTrait from parent blocks
        final List<UpdateKindTrait> requiredUpdateKindTraits;
        if (rootModifyKindSet.contains(ModifyKind.UPDATE)) {
            if (context.isUpdateBeforeRequired()) {
                requiredUpdateKindTraits =
                        Collections.singletonList(UpdateKindTrait.BEFORE_AND_AFTER());
            } else {
                // update_before is not required, and input contains updates
                // try ONLY_UPDATE_AFTER first, and then BEFORE_AND_AFTER
                requiredUpdateKindTraits =
                        Arrays.asList(
                                UpdateKindTrait.ONLY_UPDATE_AFTER(),
                                UpdateKindTrait.BEFORE_AND_AFTER());
            }
        } else {
            // there is no updates
            requiredUpdateKindTraits = Collections.singletonList(UpdateKindTrait.NONE());
        }

        SatisfyUpdateKindTraitVisitor updateKindTraitVisitor =
                new SatisfyUpdateKindTraitVisitor(context);
        List<StreamPhysicalRel> updateRoot = new ArrayList<>();
        for (UpdateKindTrait requiredUpdateKindTrait : requiredUpdateKindTraits) {
            updateKindTraitVisitor
                    .visit(rootWithModifyKindSet, requiredUpdateKindTrait)
                    .ifPresent(updateRoot::add);
        }

        // step3: satisfy DeleteKind trait
        final List<DeleteKindTrait> requiredDeleteKindTraits;
        if (rootModifyKindSet.contains(ModifyKind.DELETE)) {
            if (root instanceof StreamPhysicalSink) {
                // try DELETE_BY_KEY first, and then FULL_DELETE
                requiredDeleteKindTraits =
                        Arrays.asList(
                                DeleteKindTrait.DELETE_BY_KEY(), DeleteKindTrait.FULL_DELETE());
            } else {
                // for non-sink nodes prefer full deletes
                requiredDeleteKindTraits = Collections.singletonList(DeleteKindTrait.FULL_DELETE());
            }
        } else {
            // there is no deletes
            requiredDeleteKindTraits = Collections.singletonList(DeleteKindTrait.NONE());
        }

        SatisfyDeleteKindTraitVisitor deleteKindTraitVisitor =
                new SatisfyDeleteKindTraitVisitor(context);
        List<StreamPhysicalRel> finalRoot = new ArrayList<>();
        if (!updateRoot.isEmpty()) {
            StreamPhysicalRel updated = updateRoot.get(0);
            for (DeleteKindTrait requiredDeleteKindTrait : requiredDeleteKindTraits) {
                deleteKindTraitVisitor
                        .visit(updated, requiredDeleteKindTrait)
                        .ifPresent(finalRoot::add);
            }
        }

        // step4: sanity check and return non-empty root
        if (finalRoot.isEmpty()) {
            String plan =
                    FlinkRelOptUtil.toString(
                            root,
                            SqlExplainLevel.DIGEST_ATTRIBUTES,
                            false,
                            true,
                            false,
                            false,
                            false,
                            false);
            throw new TableException(
                    "Can't generate a valid execution plan for the given query:\n" + plan);
        } else {
            return finalRoot.get(0);
        }
    }
}
