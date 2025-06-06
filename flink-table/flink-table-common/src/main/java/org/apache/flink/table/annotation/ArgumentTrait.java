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
import org.apache.flink.table.functions.ProcessTableFunction.TimeContext;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.types.RowKind;

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
    SCALAR(true, StaticArgumentTrait.SCALAR),

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
    TABLE_AS_ROW(true, StaticArgumentTrait.TABLE_AS_ROW),

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
    TABLE_AS_SET(true, StaticArgumentTrait.TABLE_AS_SET),

    /**
     * Defines that a PARTITION BY clause is optional for {@link #TABLE_AS_SET}. By default, it is
     * mandatory for improving the parallel execution by distributing the table by key.
     *
     * <p>Note: This trait is only valid for {@link #TABLE_AS_SET} arguments.
     */
    OPTIONAL_PARTITION_BY(false, StaticArgumentTrait.OPTIONAL_PARTITION_BY),

    /**
     * Defines that all columns of a table argument (i.e. {@link #TABLE_AS_ROW} or {@link
     * #TABLE_AS_SET}) are included in the output of the PTF. By default, only columns of the
     * PARTITION BY clause are passed through.
     *
     * <p>Given a table t (containing columns k and v), and a PTF f() (producing columns c1 and c2),
     * the output of a {@code SELECT * FROM f(table_arg => TABLE t PARTITION BY k)} uses the
     * following order:
     *
     * <pre>
     * Default: | k | c1 | c2 |
     * With pass-through columns: | k | v | c1 | c2 |
     * </pre>
     *
     * <p>Pass-through columns are only available for append-only PTFs taking a single table
     * argument and don't use timers.
     *
     * <p>Note: This trait is valid for {@link #TABLE_AS_ROW} and {@link #TABLE_AS_SET} arguments.
     */
    PASS_COLUMNS_THROUGH(false, StaticArgumentTrait.PASS_COLUMNS_THROUGH),

    /**
     * Defines that updates are allowed as input to the given table argument. By default, a table
     * argument is insert-only and updates will be rejected.
     *
     * <p>Input tables become updating when sub queries such as aggregations or outer joins force an
     * incremental computation. For example, the following query only works if the function is able
     * to digest retraction messages:
     *
     * <pre>
     * // The change +I[1] followed by -U[1], +U[2], -U[2], +U[3] will enter the function
     * // if `table_arg` is declared with SUPPORTS_UPDATES
     * WITH UpdatingTable AS (
     *   SELECT COUNT(*) FROM (VALUES 1, 2, 3)
     * )
     * SELECT * FROM f(table_arg => TABLE UpdatingTable)
     * </pre>
     *
     * <p>If updates should be supported, ensure that the data type of the table argument is chosen
     * in a way that it can encode changes. In other words: choose a row type that exposes the
     * {@link RowKind} change flag.
     *
     * <p>The changelog of the backing input table decides which kinds of changes enter the
     * function. The function receives {+I} when the input table is append-only. The function
     * receives {+I,+U,-D} if the input table is upserting using the same upsert key as the
     * partition key. Otherwise, retractions {+I,-U,+U,-D} (i.e. including {@link
     * RowKind#UPDATE_BEFORE}) enter the function. Use {@link #REQUIRE_UPDATE_BEFORE} to enforce
     * retractions for all updating cases.
     *
     * <p>For upserting tables, if the changelog contains key-only deletions (also known as partial
     * deletions), only upsert key fields are set when a row enters the function. Non-key fields are
     * set to null, regardless of NOT NULL constraints. Use {@link #REQUIRE_FULL_DELETE} to enforce
     * that only full deletes enter the function.
     *
     * <p>This trait is intended for advanced use cases. Please note that inputs are always
     * insert-only in batch mode. Thus, if the PTF should produce the same results in both batch and
     * streaming mode, results should be emitted based on watermarks and event-time.
     *
     * <p>The trait {@link #PASS_COLUMNS_THROUGH} is not supported if this trait is declared.
     *
     * <p>The `on_time` argument is not supported if the PTF receives updates.
     *
     * <p>Note: This trait is valid for {@link #TABLE_AS_ROW} and {@link #TABLE_AS_SET} arguments.
     *
     * @see #REQUIRE_UPDATE_BEFORE
     * @see #REQUIRE_FULL_DELETE
     */
    SUPPORT_UPDATES(false, StaticArgumentTrait.SUPPORT_UPDATES),

    /**
     * Defines that a table argument which {@link #SUPPORT_UPDATES} should include a {@link
     * RowKind#UPDATE_BEFORE} message when encoding updates. In other words: it enforces presenting
     * the updating table in retract changelog mode.
     *
     * <p>This trait is intended for advanced use cases. By default, updates are encoded as emitted
     * by the input operation. Thus, the updating table might be encoded in upsert changelog mode
     * and deletes might only contain keys.
     *
     * <p>The following example shows how the input changelog encodes updates differently:
     *
     * <pre>
     * // Given a table UpdatingTable(name STRING PRIMARY KEY, score INT)
     * // backed by upsert changelog with changes
     * // +I[Alice, 42], +I[Bob, 0], +U[Bob, 2], +U[Bob, 100], -D[Bob, NULL].
     *
     * // Given a function `f` that declares `table_arg` with REQUIRE_UPDATE_BEFORE.
     * SELECT * FROM f(table_arg => TABLE UpdatingTable PARTITION BY name)
     *
     * // The following changes will enter the function:
     * // +I[Alice, 42], +I[Bob, 0], -U[Bob, 0], +U[Bob, 2], -U[Bob, 2], +U[Bob, 100], -U[Bob, 100]
     *
     * // In both encodings, a materialized table would only contain a row for Alice.
     * </pre>
     *
     * <p>Note: This trait is valid for {@link #TABLE_AS_SET} arguments that {@link
     * #SUPPORT_UPDATES}.
     *
     * @see #SUPPORT_UPDATES
     */
    REQUIRE_UPDATE_BEFORE(false, StaticArgumentTrait.REQUIRE_UPDATE_BEFORE),

    /**
     * Defines that a table argument which {@link #SUPPORT_UPDATES} should include all fields in the
     * {@link RowKind#DELETE} message if the updating table is backed by an upsert changelog.
     *
     * <p>This trait is intended for advanced use cases. For upserting tables, if the changelog
     * contains key-only deletes (also known as partial deletes), only upsert key fields are set
     * when a row enters the function. Non-key fields are set to null, regardless of NOT NULL
     * constraints.
     *
     * <p>The following example shows how the input changelog encodes updates differently:
     *
     * <pre>
     * // Given a table UpdatingTable(name STRING PRIMARY KEY, score INT)
     * // backed by upsert changelog with changes
     * // +I[Alice, 42], +I[Bob, 0], +U[Bob, 2], +U[Bob, 100], -D[Bob, NULL].
     *
     * // Given a function `f` that declares `table_arg` with REQUIRE_FULL_DELETE.
     * SELECT * FROM f(table_arg => TABLE UpdatingTable PARTITION BY name)
     *
     * // The following changes will enter the function:
     * // +I[Alice, 42], +I[Bob, 0], +U[Bob, 2], +U[Bob, 100], -D[Bob, 100].
     *
     * // In both encodings, a materialized table would only contain a row for Alice.
     * </pre>
     *
     * <p>Note: This trait is valid for {@link #TABLE_AS_SET} arguments that {@link
     * #SUPPORT_UPDATES}.
     *
     * @see #SUPPORT_UPDATES
     */
    REQUIRE_FULL_DELETE(false, StaticArgumentTrait.REQUIRE_FULL_DELETE),

    /**
     * Defines that an {@code on_time} argument must be provided, referencing a watermarked
     * timestamp column in the given table.
     *
     * <p>The {@code on_time} argument indicates which column provides the event-time timestamp. In
     * other words, it specifies the column that defines the timestamp for when a row was generated.
     * This timestamp is used within the PTF for timers and time-based operations when the watermark
     * progresses the logical clock.
     *
     * <p>By default, the {@code on_time} argument is optional. If no timestamp column is set for
     * the PTF, the {@link TimeContext#time()} will return null. If the {@code on_time} argument is
     * provided, {@link TimeContext#time()} will return it and the PTF will return a {@code rowtime}
     * column in the output, allowing subsequent operations to access and propagate the resulting
     * event-time timestamp.
     *
     * <p>For example:
     *
     * <pre>
     *     CREATE TABLE t (v STRING, ts TIMESTAMP_LTZ(3), WATERMARK FOR ts AS ts - INTERVAL '2' SECONDS);
     *
     *     SELECT v, rowtime FROM f(table_arg => TABLE t, on_time => DESCRIPTOR(ts));
     * </pre>
     *
     * <p>Note: This trait is valid for {@link #TABLE_AS_ROW} and {@link #TABLE_AS_SET} arguments.
     */
    REQUIRE_ON_TIME(false, StaticArgumentTrait.REQUIRE_ON_TIME);

    private final boolean isRoot;
    private final StaticArgumentTrait staticTrait;

    ArgumentTrait(boolean isRoot, StaticArgumentTrait staticTrait) {
        this.isRoot = isRoot;
        this.staticTrait = staticTrait;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public StaticArgumentTrait toStaticTrait() {
        return staticTrait;
    }
}
