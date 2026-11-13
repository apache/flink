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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** Declares how the refresh pipeline of a materialized table runs. */
@PublicEvolving
public class RefreshStrategy {

    /** The kind of a {@link RefreshStrategy}. */
    @PublicEvolving
    public enum Kind {
        AUTOMATIC,
        CONTINUOUS,
        FULL
    }

    private static final RefreshStrategy AUTOMATIC = new RefreshStrategy(Kind.AUTOMATIC);
    private static final RefreshStrategy CONTINUOUS = new RefreshStrategy(Kind.CONTINUOUS);
    private static final RefreshStrategy FULL = new RefreshStrategy(Kind.FULL);

    private final Kind kind;

    protected RefreshStrategy(Kind kind) {
        this.kind = Preconditions.checkNotNull(kind, "Kind must not be null.");
    }

    /**
     * Lets the engine choose, based on the freshness. The default.
     *
     * <p>CONTINUOUS if the freshness is below {@code
     * materialized-table.refresh-mode.freshness-threshold}, FULL otherwise.
     *
     * <p>FULL is currently not supported from a plain {@link TableEnvironment}, so a freshness at
     * or above the threshold fails there when the materialized table is created. Altering an
     * existing materialized table ignores the freshness, see {@link
     * MaterializedPipeline#execute()}.
     */
    public static RefreshStrategy automatic() {
        return AUTOMATIC;
    }

    /** The table is refreshed by a continuously running streaming job. */
    public static RefreshStrategy continuous() {
        return CONTINUOUS;
    }

    /**
     * The table is refreshed by periodically scheduled batch jobs. Requires an environment with a
     * workflow scheduler.
     */
    public static RefreshStrategy full() {
        return FULL;
    }

    public Kind getKind() {
        return kind;
    }

    /** Returns the kind name, e.g. {@code CONTINUOUS}. */
    public String asSummaryString() {
        return kind.name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return kind == ((RefreshStrategy) o).kind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
