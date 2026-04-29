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

/**
 * How a process table function's table argument contributes columns to the output.
 *
 * <p>Each table argument has exactly one mode, derived from its {@link StaticArgumentTrait} set.
 * The framework uses the mode to decide what (if anything) to prepend to that argument's emitted
 * rows. The mode is per-argument; a multi-table PTF may mix modes across its arguments.
 *
 * <p>Note: the system rowtime suffix (when {@link StaticArgumentTrait#REQUIRE_ON_TIME} applies) is
 * a single column shared across all arguments, so it remains a function-wide decision rather than
 * per-argument: it is suppressed if any argument uses {@link #NONE}.
 */
@PublicEvolving
public enum PassThroughMode {
    /** Default. The framework prepends the partition-key columns (empty for row semantics). */
    KEY,
    /**
     * Maps to {@link StaticArgumentTrait#PASS_COLUMNS_THROUGH}. The framework prepends every input
     * column.
     */
    ALL,
    /**
     * Maps to {@link StaticArgumentTrait#NO_PASS_THROUGH}. The framework prepends nothing; the
     * function owns its full output schema.
     */
    NONE
}
