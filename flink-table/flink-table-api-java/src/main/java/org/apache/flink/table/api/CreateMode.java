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

/**
 * Controls what executing a materialized table declaration does when an object already exists at
 * its path. See {@link MaterializedPipeline#execute(CreateMode)}.
 */
@PublicEvolving
public final class CreateMode {

    /** The kind of a {@link CreateMode}. */
    @PublicEvolving
    public enum Kind {
        CREATE_OR_ALTER,
        FAIL_IF_EXISTS
    }

    private static final CreateMode CREATE_OR_ALTER = new CreateMode(Kind.CREATE_OR_ALTER);
    private static final CreateMode FAIL_IF_EXISTS = new CreateMode(Kind.FAIL_IF_EXISTS);

    private final Kind kind;

    private CreateMode(Kind kind) {
        this.kind = Preconditions.checkNotNull(kind, "Kind must not be null.");
    }

    /**
     * Creates the materialized table, or alters an existing materialized table to this declaration.
     * This is the default, used by {@link MaterializedPipeline#execute()}. See there for what an
     * existing materialized table keeps.
     *
     * <p>Fails with a {@link ValidationException} if a regular table, a view, or a temporary table
     * or view exists at the path.
     *
     * <p>Equivalent to {@code CREATE OR ALTER MATERIALIZED TABLE}.
     */
    public static CreateMode createOrAlter() {
        return CREATE_OR_ALTER;
    }

    /**
     * Creates the materialized table. Fails with a {@link ValidationException} if a materialized
     * table, a regular table, a view, or a temporary table or view exists at the path. The existing
     * object is not changed, and no refresh job is started.
     *
     * <p>Equivalent to {@code CREATE MATERIALIZED TABLE}.
     */
    public static CreateMode failIfExists() {
        return FAIL_IF_EXISTS;
    }

    public Kind getKind() {
        return kind;
    }

    /** Returns the kind name, e.g. {@code FAIL_IF_EXISTS}. */
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
        return kind == ((CreateMode) o).kind;
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
