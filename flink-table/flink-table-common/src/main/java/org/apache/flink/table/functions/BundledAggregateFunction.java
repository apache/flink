/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.agg.BundledKeySegment;
import org.apache.flink.table.functions.agg.BundledKeySegmentApplied;

import java.util.concurrent.CompletableFuture;

/** The bundled interface to be implemented by {@AggregateFunction}s that may support bundling. */
@PublicEvolving
public interface BundledAggregateFunction extends FunctionDefinition {

    /**
     * Whether the implementor supports bundling. This allows them to programatically decide whether
     * to use the bundling or non-bundling interface.
     */
    boolean canBundle();

    /** Whether the implementor supports retraction. */
    default boolean canRetract() {
        return false;
    }

    default void bundledAccumulateRetract(
            CompletableFuture<BundledKeySegmentApplied> future, BundledKeySegment segment)
            throws Exception {
        throw new UnsupportedOperationException(
                "This aggregate function does not support bundled calls.");
    }
}
