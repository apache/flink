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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Defines the conflict resolution strategies for INSERT INTO statements when the query's upsert key
 * differs from the target table's primary key.
 *
 * <p>The upsert key is derived from the query's semantics (e.g., GROUP BY columns). When it differs
 * from the target table's primary key, multiple records from the query may map to the same primary
 * key, causing a conflict. For example:
 *
 * <pre>{@code
 * CREATE TABLE user_totals (
 *   user_id BIGINT,
 *   category STRING,
 *   total DECIMAL(10, 2),
 *   PRIMARY KEY (user_id) NOT ENFORCED
 * );
 *
 * -- Upsert key is (user_id, category), but primary key is just (user_id).
 * -- Updates for (user_id=1, category='A') and (user_id=1, category='B')
 * -- both target the same primary key row, causing a conflict.
 * INSERT INTO user_totals
 * SELECT user_id, category, SUM(amount) as total
 * FROM orders
 * GROUP BY user_id, category
 * ON CONFLICT DO NOTHING;
 * }</pre>
 *
 * <p>These strategies are used with the ON CONFLICT clause:
 *
 * <ul>
 *   <li>{@code ON CONFLICT DO ERROR} - Throw an exception on primary key constraint violation
 *   <li>{@code ON CONFLICT DO NOTHING} - Keep the first record, ignore subsequent conflicts
 *   <li>{@code ON CONFLICT DO DEDUPLICATE} - Maintain history for rollback (current behavior)
 * </ul>
 *
 * <p>Use the static factory methods to create instances:
 *
 * <pre>{@code
 * InsertConflictStrategy.error()
 * InsertConflictStrategy.nothing()
 * InsertConflictStrategy.deduplicate()
 * }</pre>
 *
 * <p>Or use the builder for more complex configurations:
 *
 * <pre>{@code
 * InsertConflictStrategy.newBuilder()
 *     .withBehavior(ConflictBehavior.DEDUPLICATE)
 *     .build()
 * }</pre>
 */
@PublicEvolving
public final class InsertConflictStrategy implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Pre-built instance for ERROR behavior. */
    private static final InsertConflictStrategy ERROR_INSTANCE =
            new InsertConflictStrategy(ConflictBehavior.ERROR);

    /** Pre-built instance for NOTHING behavior. */
    private static final InsertConflictStrategy NOTHING_INSTANCE =
            new InsertConflictStrategy(ConflictBehavior.NOTHING);

    /** Pre-built instance for DEDUPLICATE behavior. */
    private static final InsertConflictStrategy DEDUPLICATE_INSTANCE =
            new InsertConflictStrategy(ConflictBehavior.DEDUPLICATE);

    private final ConflictBehavior behavior;

    private InsertConflictStrategy(ConflictBehavior behavior) {
        this.behavior = Preconditions.checkNotNull(behavior, "behavior must not be null");
    }

    /**
     * Creates a strategy that throws an exception when multiple distinct records arrive for the
     * same primary key after watermark compaction.
     *
     * @return the ERROR conflict strategy
     */
    public static InsertConflictStrategy error() {
        return ERROR_INSTANCE;
    }

    /**
     * Creates a strategy that keeps the first record that arrives for a given primary key and
     * discards subsequent conflicts.
     *
     * @return the NOTHING conflict strategy
     */
    public static InsertConflictStrategy nothing() {
        return NOTHING_INSTANCE;
    }

    /**
     * Creates a strategy that maintains the full history of changes for each primary key to support
     * rollback on retraction. This is the current default behavior.
     *
     * @return the DEDUPLICATE conflict strategy
     */
    public static InsertConflictStrategy deduplicate() {
        return DEDUPLICATE_INSTANCE;
    }

    /**
     * Creates a new builder for constructing an {@link InsertConflictStrategy}.
     *
     * @return a new builder instance
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Returns the conflict behavior of this strategy.
     *
     * @return the conflict behavior
     */
    public ConflictBehavior getBehavior() {
        return behavior;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InsertConflictStrategy that = (InsertConflictStrategy) o;
        return behavior == that.behavior;
    }

    @Override
    public int hashCode() {
        return Objects.hash(behavior);
    }

    @Override
    public String toString() {
        return behavior.name();
    }

    // --------------------------------------------------------------------------------------------

    /** Defines the basic conflict resolution behaviors. */
    @PublicEvolving
    public enum ConflictBehavior {
        /**
         * Throw an exception when multiple distinct records arrive for the same primary key after
         * watermark compaction.
         */
        ERROR,

        /**
         * Keep the first record that arrives for a given primary key, discard subsequent conflicts.
         */
        NOTHING,

        /**
         * Maintain the full history of changes for each primary key to support rollback on
         * retraction. This is the current default behavior.
         */
        DEDUPLICATE
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for creating {@link InsertConflictStrategy} instances. */
    @PublicEvolving
    public static final class Builder {

        private @Nullable ConflictBehavior behavior;

        private Builder() {}

        /**
         * Sets the conflict behavior for the strategy.
         *
         * @param behavior the conflict behavior to use
         * @return this builder
         */
        public Builder withBehavior(ConflictBehavior behavior) {
            this.behavior = Preconditions.checkNotNull(behavior, "behavior must not be null");
            return this;
        }

        /**
         * Builds the {@link InsertConflictStrategy}.
         *
         * @return the configured InsertConflictStrategy
         * @throws IllegalStateException if behavior is not set
         */
        public InsertConflictStrategy build() {
            Preconditions.checkState(behavior != null, "behavior must be set");
            // Return cached instances for simple cases
            switch (behavior) {
                case ERROR:
                    return ERROR_INSTANCE;
                case NOTHING:
                    return NOTHING_INSTANCE;
                case DEDUPLICATE:
                    return DEDUPLICATE_INSTANCE;
                default:
                    return new InsertConflictStrategy(behavior);
            }
        }
    }
}
