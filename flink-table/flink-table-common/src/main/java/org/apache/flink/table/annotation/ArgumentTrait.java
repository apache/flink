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

package org.apache.flink.table.annotation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.inference.StaticArgumentTrait;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Declares traits for {@link ArgumentHint}. They enable basic validation by the framework.
 *
 * <p>Some traits have dependencies to other traits, which is why this enum reflects a hierarchy in
 * which {@link #SCALAR}, {@link #TABLE_AS_ROW}, and {@link #TABLE_AS_SET} are the top-level roots.
 */
@PublicEvolving
public enum ArgumentTrait {

    /**
     * An argument that accepts a scalar value. For example: f(1), f(true), f('Some string').
     *
     * <p>It's the default if no {@link ArgumentHint} is provided.
     */
    SCALAR(StaticArgumentTrait.SCALAR),

    /**
     * An argument that accepts a table "as row" (i.e. with row semantics). This trait only applies
     * to {@link ProcessTableFunction} (PTF).
     *
     * <p>For scalability, input tables are distributed across so-called "virtual processors". A
     * virtual processor, as defined by the SQL standard, executes a PTF instance and has access
     * only to a portion of the entire table. The argument declaration decides about the size of the
     * portion and co-location of data. Conceptually, tables can be processed either "as row" (i.e.
     * with row semantics) or "as set" (i.e. with set semantics).
     *
     * <p>A table with row semantics assumes that there is no correlation between rows and each row
     * can be processed independently. The framework is free in how to distribute rows across
     * virtual processors and each virtual processor has access only to the currently processed row.
     */
    TABLE_AS_ROW(StaticArgumentTrait.TABLE_AS_ROW),

    /**
     * An argument that accepts a table "as set" (i.e. with set semantics). This trait only applies
     * to {@link ProcessTableFunction} (PTF).
     *
     * <p>For scalability, input tables are distributed across so-called "virtual processors". A
     * virtual processor, as defined by the SQL standard, executes a PTF instance and has access
     * only to a portion of the entire table. The argument declaration decides about the size of the
     * portion and co-location of data. Conceptually, tables can be processed either "as row" (i.e.
     * with row semantics) or "as set" (i.e. with set semantics).
     *
     * <p>A table with set semantics assumes that there is a correlation between rows. When calling
     * the function, the PARTITION BY clause defines the columns for correlation. The framework
     * ensures that all rows belonging to same set are co-located. A PTF instance is able to access
     * all rows belonging to the same set. In other words: The virtual processor is scoped by a key
     * context.
     *
     * <p>It is also possible not to provide a key ({@link #OPTIONAL_PARTITION_BY}), in which case
     * only one virtual processor handles the entire table, thereby losing scalability benefits.
     */
    TABLE_AS_SET(StaticArgumentTrait.TABLE_AS_SET),

    /**
     * Defines that a PARTITION BY clause is optional for {@link #TABLE_AS_SET}. By default, it is
     * mandatory for improving the parallel execution by distributing the table by key.
     */
    OPTIONAL_PARTITION_BY(StaticArgumentTrait.OPTIONAL_PARTITION_BY, TABLE_AS_SET);

    private final StaticArgumentTrait staticTrait;
    private final Set<ArgumentTrait> requirements;

    ArgumentTrait(StaticArgumentTrait staticTrait, ArgumentTrait... requirements) {
        this.staticTrait = staticTrait;
        this.requirements = Arrays.stream(requirements).collect(Collectors.toSet());
    }

    public Set<ArgumentTrait> getRequirements() {
        return requirements;
    }

    public StaticArgumentTrait toStaticTrait() {
        return staticTrait;
    }
}
