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

package org.apache.flink.table.planner.plan.trait;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link RelTraitDef} for {@link DuplicateChangesTrait}. */
public class DuplicateChangesTraitDef extends RelTraitDef<DuplicateChangesTrait> {

    public static final DuplicateChangesTraitDef INSTANCE = new DuplicateChangesTraitDef();

    @Override
    public Class<DuplicateChangesTrait> getTraitClass() {
        return DuplicateChangesTrait.class;
    }

    @Override
    public String getSimpleName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public @Nullable RelNode convert(
            RelOptPlanner planner,
            RelNode rel,
            DuplicateChangesTrait toTrait,
            boolean allowInfiniteCostConverters) {
        return rel.copy(rel.getTraitSet().plus(toTrait), rel.getInputs());
    }

    @Override
    public boolean canConvert(
            RelOptPlanner relOptPlanner,
            DuplicateChangesTrait duplicateChangesTrait,
            DuplicateChangesTrait t1) {
        return true;
    }

    @Override
    public DuplicateChangesTrait getDefault() {
        return DuplicateChangesTrait.NONE;
    }
}
