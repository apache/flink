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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * In order to apply a {@link CastRule}, the runtime checks if a particular rule matches the tuple
 * of input and target type using this class. In particular, a rule is applied if:
 *
 * <ol>
 *   <li>{@link #getTargetTypeRoots()} includes the {@link LogicalTypeRoot} of target type and
 *       either
 *       <ol>
 *         <li>{@link #getInputTypeRoots()} includes the {@link LogicalTypeRoot} of input type or
 *         <li>{@link #getInputTypeFamilies()} includes one of the {@link LogicalTypeFamily} of
 *             input type
 *       </ol>
 *   <li>Or {@link #getTargetTypeFamilies()} includes one of the {@link LogicalTypeFamily} of target
 *       type and either
 *       <ol>
 *         <li>{@link #getInputTypeRoots()} includes the {@link LogicalTypeRoot} of input type or
 *         <li>{@link #getInputTypeFamilies()} includes one of the {@link LogicalTypeFamily} of
 *             input type
 *       </ol>
 *   <li>Or, if {@link #getCustomPredicate()} is not null, the input {@link LogicalType} and target
 *       {@link LogicalType} matches the predicate.
 * </ol>
 *
 * <p>The {@code customPredicate} should be used in cases where {@link LogicalTypeRoot} and {@link
 * LogicalTypeFamily} are not enough to identify whether a rule is applicable or not, for example
 * when the matching depends on a field of the provided input {@link LogicalType} instance.
 */
@Internal
public class CastRulePredicate {

    private final Set<LogicalType> targetTypes;

    private final Set<LogicalTypeRoot> inputTypeRoots;
    private final Set<LogicalTypeRoot> targetTypeRoots;

    private final Set<LogicalTypeFamily> inputTypeFamilies;
    private final Set<LogicalTypeFamily> targetTypeFamilies;

    private final BiPredicate<LogicalType, LogicalType> customPredicate;

    private CastRulePredicate(
            Set<LogicalType> targetTypes,
            Set<LogicalTypeRoot> inputTypeRoots,
            Set<LogicalTypeRoot> targetTypeRoots,
            Set<LogicalTypeFamily> inputTypeFamilies,
            Set<LogicalTypeFamily> targetTypeFamilies,
            BiPredicate<LogicalType, LogicalType> customPredicate) {
        this.targetTypes = targetTypes;
        this.inputTypeRoots = inputTypeRoots;
        this.targetTypeRoots = targetTypeRoots;
        this.inputTypeFamilies = inputTypeFamilies;
        this.targetTypeFamilies = targetTypeFamilies;
        this.customPredicate = customPredicate;
    }

    public Set<LogicalType> getTargetTypes() {
        return targetTypes;
    }

    public Set<LogicalTypeRoot> getInputTypeRoots() {
        return inputTypeRoots;
    }

    public Set<LogicalTypeRoot> getTargetTypeRoots() {
        return targetTypeRoots;
    }

    public Set<LogicalTypeFamily> getInputTypeFamilies() {
        return inputTypeFamilies;
    }

    public Set<LogicalTypeFamily> getTargetTypeFamilies() {
        return targetTypeFamilies;
    }

    public Optional<BiPredicate<LogicalType, LogicalType>> getCustomPredicate() {
        return Optional.ofNullable(customPredicate);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for the {@link CastRulePredicate}. */
    public static class Builder {
        private final Set<LogicalTypeRoot> inputTypeRoots = new HashSet<>();
        private final Set<LogicalTypeRoot> targetTypeRoots = new HashSet<>();
        private final Set<LogicalType> targetTypes = new HashSet<>();

        private final Set<LogicalTypeFamily> inputTypeFamilies = new HashSet<>();
        private final Set<LogicalTypeFamily> targetTypeFamilies = new HashSet<>();

        private BiPredicate<LogicalType, LogicalType> customPredicate;

        public Builder input(LogicalTypeRoot inputTypeRoot) {
            inputTypeRoots.add(inputTypeRoot);
            return this;
        }

        public Builder target(LogicalTypeRoot outputTypeRoot) {
            targetTypeRoots.add(outputTypeRoot);
            return this;
        }

        public Builder target(LogicalType outputType) {
            targetTypes.add(outputType);
            return this;
        }

        public Builder input(LogicalTypeFamily inputTypeFamily) {
            inputTypeFamilies.add(inputTypeFamily);
            return this;
        }

        public Builder target(LogicalTypeFamily outputTypeFamily) {
            targetTypeFamilies.add(outputTypeFamily);
            return this;
        }

        public Builder predicate(BiPredicate<LogicalType, LogicalType> customPredicate) {
            this.customPredicate = customPredicate;
            return this;
        }

        public CastRulePredicate build() {
            return new CastRulePredicate(
                    Collections.unmodifiableSet(targetTypes),
                    Collections.unmodifiableSet(inputTypeRoots),
                    Collections.unmodifiableSet(targetTypeRoots),
                    Collections.unmodifiableSet(inputTypeFamilies),
                    Collections.unmodifiableSet(targetTypeFamilies),
                    customPredicate);
        }
    }
}
