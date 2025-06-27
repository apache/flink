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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.trait.DuplicateChanges;
import org.apache.flink.table.planner.plan.trait.DuplicateChangesTrait;
import org.apache.flink.table.planner.plan.trait.DuplicateChangesTraitDef;

import java.util.Optional;

/** Utils for {@link DuplicateChanges}. */
public class DuplicateChangesUtils {

    private DuplicateChangesUtils() {}

    /**
     * Get an optional {@link DuplicateChanges} from the given {@link StreamPhysicalRel}.
     *
     * <p>The {@link DuplicateChanges} is inferred from {@link DuplicateChangesTrait}.
     */
    public static Optional<DuplicateChanges> getDuplicateChanges(StreamPhysicalRel rel) {
        Optional<DuplicateChangesTrait> duplicateChangesTraitOp =
                Optional.ofNullable(rel.getTraitSet().getTrait(DuplicateChangesTraitDef.INSTANCE));
        return duplicateChangesTraitOp.stream()
                .map(DuplicateChangesTrait::getDuplicateChanges)
                .findFirst();
    }

    /**
     * Merge the given two {@link DuplicateChanges} as a new one.
     *
     * <p>The logic matrix is following:
     *
     * <pre>
     *       +-------------+-------------+---------------+
     *       | origin_1    | origin_2    | merge result  |
     *       +-------------+-------------+---------------+
     *       | NONE        |   *         |     *         |
     *       | `ANY`       |  NONE       |   `ANY`       |
     *       | DISALLOW    |   *         |   DISALLOW    |
     *       |   *         |  DISALLOW   |   DISALLOW    |
     *       | ALLOW       | ALLOW       |   ALLOW       |
     *       +-------------+-------------+---------------+
     * </pre>
     */
    public static DuplicateChanges mergeDuplicateChanges(
            DuplicateChanges duplicateChanges1, DuplicateChanges duplicateChanges2) {
        if (duplicateChanges1 == DuplicateChanges.NONE
                || duplicateChanges2 == DuplicateChanges.NONE) {
            return duplicateChanges1 == DuplicateChanges.NONE
                    ? duplicateChanges2
                    : duplicateChanges1;
        }
        if (duplicateChanges1 == DuplicateChanges.DISALLOW
                || duplicateChanges2 == DuplicateChanges.DISALLOW) {
            return DuplicateChanges.DISALLOW;
        }

        return DuplicateChanges.ALLOW;
    }
}
