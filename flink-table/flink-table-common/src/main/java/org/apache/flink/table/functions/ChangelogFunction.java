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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;

/**
 * An extension that allows a process table function (PTF) to emit results with changelog semantics.
 *
 * <p>By default, a {@link ProcessTableFunction} can only emit insert-only (append-only) results. By
 * implementing this interface, a function can declare the types of changes (e.g., inserts, updates,
 * deletes) that it may emit, allowing the planner to make informed decisions during query planning.
 *
 * <p>Note: This interface is intended for advanced use cases and should be implemented with care.
 * Emitting an incorrect changelog from the PTF may lead to undefined behavior in the overall query.
 * The `on_time` argument is unsupported for updating PTFs.
 *
 * <p>The resulting changelog mode can be influenced by:
 *
 * <ul>
 *   <li>The changelog mode of the input table arguments, accessible via {@link
 *       ChangelogContext#getTableChangelogMode(int)}.
 *   <li>The changelog mode required by downstream operators, accessible via {@link
 *       ChangelogContext#getRequiredChangelogMode()}.
 * </ul>
 *
 * <p>Changelog mode inference in the planner involves several steps. The {@link
 * #getChangelogMode(ChangelogContext)} method is called during each step:
 *
 * <ol>
 *   <li>The planner checks whether the PTF emits updates or inserts-only.
 *   <li>If updates are emitted, the planner determines whether the updates include {@link
 *       RowKind#UPDATE_BEFORE} messages (retract mode), or whether {@link RowKind#UPDATE_AFTER}
 *       messages are sufficient (upsert mode). For this, {@link #getChangelogMode} might be called
 *       twice to query both retract mode and upsert mode capabilities as indicated by {@link
 *       ChangelogContext#getRequiredChangelogMode()}.
 *   <li>If in upsert mode, the planner checks whether {@link RowKind#DELETE} messages contain all
 *       fields (full deletes) or only key fields (partial deletes). In the case of partial deletes,
 *       only the upsert key fields are set when a row is removed; all non-key fields are null,
 *       regardless of nullability constraints. {@link ChangelogContext#getRequiredChangelogMode()}
 *       indicates whether a downstream operator requires full deletes.
 * </ol>
 *
 * <p>Emitting changelogs is only valid for PTFs that take table arguments with set semantics (see
 * {@link ArgumentTrait#SET_SEMANTIC_TABLE}). In case of upserts, the upsert key must be equal to
 * the PARTITION BY key.
 *
 * <p>It is perfectly valid for a {@link ChangelogFunction} implementation to return a fixed {@link
 * ChangelogMode}, regardless of the {@link ChangelogContext}. This approach may be appropriate when
 * the PTF is designed for a specific scenario or pipeline setup, and does not need to adapt
 * dynamically to different input modes. Note that in such cases, the PTFs applicability is limited,
 * as it may only function correctly within the predefined context for which it was designed.
 *
 * <p>In some cases, this interface should be used in combination with {@link SpecializedFunction}
 * to reconfigure the PTF after the final changelog mode for the specific call location has been
 * determined. The final changelog mode is also available during runtime via {@link
 * ProcessTableFunction.Context#getChangelogMode()}.
 *
 * @see ChangelogMode
 */
@PublicEvolving
public interface ChangelogFunction extends FunctionDefinition {

    /**
     * Returns the {@link ChangelogMode} of the PTF, taking into account the table arguments and the
     * planner's requirements.
     */
    ChangelogMode getChangelogMode(ChangelogContext changelogContext);

    /** Context during changelog mode inference. */
    @PublicEvolving
    interface ChangelogContext {

        /**
         * Returns information about a table argument's changelog mode.
         *
         * @return {@link ChangelogMode} of the table with set semantics, or {@code null} if
         *     argument is not a table
         */
        ChangelogMode getTableChangelogMode(int pos);

        /**
         * Returns the {@link ChangelogMode} that the framework requires from the function.
         *
         * <p>In particular, it contains information whether {@link RowKind#UPDATE_BEFORE} messages
         * are required and {@link ChangelogMode#keyOnlyDeletes()} are supported.
         */
        ChangelogMode getRequiredChangelogMode();
    }
}
