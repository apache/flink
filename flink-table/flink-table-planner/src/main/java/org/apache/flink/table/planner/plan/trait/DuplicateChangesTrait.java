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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

/**
 * This trait is used to describe whether the node can produce duplicated changes for downstream
 * operators to consume.
 */
public class DuplicateChangesTrait implements RelTrait {

    public static final DuplicateChangesTrait ALLOW =
            new DuplicateChangesTrait(DuplicateChanges.ALLOW);

    public static final DuplicateChangesTrait NONE =
            new DuplicateChangesTrait(DuplicateChanges.NONE);

    public static final DuplicateChangesTrait DISALLOW =
            new DuplicateChangesTrait(DuplicateChanges.DISALLOW);

    private final DuplicateChanges duplicateChanges;

    public DuplicateChangesTrait(DuplicateChanges duplicateChanges) {
        this.duplicateChanges = duplicateChanges;
    }

    public DuplicateChanges getDuplicateChanges() {
        return duplicateChanges;
    }

    @Override
    public boolean satisfies(RelTrait relTrait) {
        // should totally match
        if (relTrait instanceof DuplicateChangesTrait) {
            return this.duplicateChanges.equals(
                    ((DuplicateChangesTrait) relTrait).duplicateChanges);
        } else {
            return false;
        }
    }

    @Override
    public RelTraitDef<DuplicateChangesTrait> getTraitDef() {
        return DuplicateChangesTraitDef.INSTANCE;
    }

    @Override
    public int hashCode() {
        return duplicateChanges.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DuplicateChangesTrait that = (DuplicateChangesTrait) o;
        return duplicateChanges == that.duplicateChanges;
    }

    @Override
    public String toString() {
        return String.format("[%s]", duplicateChanges);
    }

    @Override
    public void register(RelOptPlanner relOptPlanner) {}
}
