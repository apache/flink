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

import java.util.Optional;

/**
 * Read-only context provided to {@link TraitCondition} during trait resolution at planning time.
 *
 * <p>Allows conditions to inspect the SQL call (e.g., whether PARTITION BY was provided, or what
 * value a scalar argument has) to decide whether a conditional trait should be active.
 */
@PublicEvolving
public interface TraitContext {

    /** Whether PARTITION BY was provided on this table argument. */
    boolean hasPartitionBy();

    /**
     * Reads a scalar argument value by name.
     *
     * @return the argument value, or empty if the argument was not provided, is not a literal, or
     *     cannot be converted to the requested type
     */
    <T> Optional<T> getScalarArgument(String name, Class<T> clazz);
}
