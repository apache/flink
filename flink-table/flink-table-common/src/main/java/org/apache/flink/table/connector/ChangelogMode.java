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

package org.apache.flink.table.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The set of changes contained in a changelog.
 *
 * @see RowKind
 */
@PublicEvolving
public final class ChangelogMode {

    private static final ChangelogMode INSERT_ONLY =
            ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();

    private static final ChangelogMode UPSERT =
            ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .addContainedKind(RowKind.DELETE)
                    .keyOnlyDeletes(true)
                    .build();

    private static final ChangelogMode UPSERT_WITH_FULL_DELETES =
            ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .addContainedKind(RowKind.DELETE)
                    .keyOnlyDeletes(false)
                    .build();

    private static final ChangelogMode ALL =
            ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_BEFORE)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .addContainedKind(RowKind.DELETE)
                    .build();

    private final Set<RowKind> kinds;
    private final boolean keyOnlyDeletes;

    private ChangelogMode(Set<RowKind> kinds, boolean keyOnlyDeletes) {
        Preconditions.checkArgument(
                kinds.size() > 0, "At least one kind of row should be contained in a changelog.");
        this.kinds = Collections.unmodifiableSet(kinds);
        this.keyOnlyDeletes = keyOnlyDeletes;
    }

    /** Shortcut for a simple {@link RowKind#INSERT}-only changelog. */
    public static ChangelogMode insertOnly() {
        return INSERT_ONLY;
    }

    /**
     * Shortcut for an upsert changelog that describes idempotent updates on a key and thus does not
     * contain {@link RowKind#UPDATE_BEFORE} rows.
     */
    public static ChangelogMode upsert() {
        return upsert(true);
    }

    /**
     * Shortcut for an upsert changelog that describes idempotent updates on a key and thus does not
     * contain {@link RowKind#UPDATE_BEFORE} rows.
     *
     * @param keyOnlyDeletes Tells the system the DELETEs contain just the key.
     */
    public static ChangelogMode upsert(boolean keyOnlyDeletes) {
        if (keyOnlyDeletes) {
            return UPSERT;
        } else {
            return UPSERT_WITH_FULL_DELETES;
        }
    }

    /** Shortcut for a changelog that can contain all {@link RowKind}s. */
    public static ChangelogMode all() {
        return ALL;
    }

    /** Builder for configuring and creating instances of {@link ChangelogMode}. */
    public static Builder newBuilder() {
        return new Builder();
    }

    public Set<RowKind> getContainedKinds() {
        return kinds;
    }

    public boolean contains(RowKind kind) {
        return kinds.contains(kind);
    }

    public boolean containsOnly(RowKind kind) {
        return kinds.size() == 1 && kinds.contains(kind);
    }

    public boolean keyOnlyDeletes() {
        return keyOnlyDeletes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ChangelogMode that = (ChangelogMode) o;
        return keyOnlyDeletes == that.keyOnlyDeletes && kinds.equals(that.kinds);
    }

    @Override
    public int hashCode() {
        int result = kinds.hashCode();
        result = 31 * result + Boolean.hashCode(keyOnlyDeletes);
        return result;
    }

    @Override
    public String toString() {
        if (!keyOnlyDeletes) {
            return kinds.toString();
        } else {
            return kinds.stream()
                    .map(kind -> kind == RowKind.DELETE ? "~DELETE" : kind.toString())
                    .collect(Collectors.joining(", ", "[", "]"));
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for configuring and creating instances of {@link ChangelogMode}. */
    @PublicEvolving
    public static class Builder {

        private final Set<RowKind> kinds = EnumSet.noneOf(RowKind.class);
        private boolean keyOnlyDeletes = false;

        private Builder() {
            // default constructor to allow a fluent definition
        }

        public Builder addContainedKind(RowKind kind) {
            this.kinds.add(kind);
            return this;
        }

        public Builder keyOnlyDeletes(boolean flag) {
            this.keyOnlyDeletes = flag;
            return this;
        }

        public ChangelogMode build() {
            return new ChangelogMode(kinds, keyOnlyDeletes);
        }
    }
}
