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
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.functions.ProcessTableFunction;

/**
 * Kind of a {@link ProcessTableFunction}'s state entry.
 *
 * @see StateHint#value()
 */
@PublicEvolving
public enum StateKind {

    /**
     * Defines a state entry that is scoped to a set as defined by the PARTITION BY clause. It is
     * backed by Flink's keyed state.
     *
     * <p>During processing, a virtual processor can read and write this state for all rows sharing
     * the same key. However, the state can not be accessed by other keys.
     */
    PER_SET,

    /**
     * Defines a state entry that is shared across all sets, independent of the PARTITION BY clause.
     * It is backed by Flink's broadcast (operator) state.
     *
     * <p>During processing, only a table with broadcast semantics (i.e. {@link
     * ArgumentTrait#BROADCAST_SEMANTIC_TABLE}) can modify this state. However, all virtual
     * processors can read it, regardless of their key context.
     *
     * <p>{@link MapView}, {@link ValueView}, and eager value types are supported.
     */
    BROADCAST
}
