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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Declares traits for {@link StaticArgument}. They enable basic validation by the framework.
 *
 * <p>Some traits have dependencies to other traits, which is why this enum reflects a hierarchy in
 * which {@link #SCALAR}, {@link #TABLE}, and {@link #MODEL} are the top-level roots.
 */
@PublicEvolving
public enum StaticArgumentTrait {
    SCALAR(),
    TABLE(),
    MODEL(),
    TABLE_AS_ROW(TABLE),
    TABLE_AS_SET(TABLE),
    OPTIONAL_PARTITION_BY(TABLE_AS_SET);

    private final Set<StaticArgumentTrait> requirements;

    StaticArgumentTrait(StaticArgumentTrait... requirements) {
        this.requirements = Arrays.stream(requirements).collect(Collectors.toSet());
    }

    public static void checkIntegrity(EnumSet<StaticArgumentTrait> traits) {
        if (traits.stream().filter(t -> t.requirements.isEmpty()).count() != 1) {
            throw new ValidationException(
                    "Invalid argument traits. An argument must be declared as either scalar, table, or model.");
        }
        traits.forEach(
                trait ->
                        trait.requirements.forEach(
                                requirement -> {
                                    if (!traits.contains(requirement)) {
                                        throw new ValidationException(
                                                String.format(
                                                        "Invalid argument traits. Trait %s requires %s.",
                                                        trait, requirement));
                                    }
                                }));
    }
}
