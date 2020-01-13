/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Optional;

/**
 * JDBC exactly once sink options.
 *
 * <p><b>maxCommitAttempts</b> - maximum number of commit attempts to make per transaction; must be
 * > 0; state size is proportional to the product of max number of in-flight snapshots and this
 * number.
 *
 * <p><b>allowOutOfOrderCommits</b> - If true, all prepared transactions will be attempted to commit
 * regardless of any transient failures during this operation. This may lead to inconsistency.
 * Default: false.
 *
 * <p><b>recoveredAndRollback</b> - whether to rollback prepared transactions known to XA RM on
 * startup (after committing <b>known</b> transactions, i.e. restored from state).
 *
 * <p>NOTE that setting this parameter to true may:
 *
 * <ol>
 *   <li>interfere with other subtasks or applications (one subtask rolling back transactions
 *       prepared by the other one (and known to it))
 *   <li>block when using with some non-MVCC databases, if there are ended-not-prepared transactions
 * </ol>
 *
 * <p>See also {@link org.apache.flink.connector.jdbc.xa.XaFacade#recover()}
 */
@PublicEvolving
public class JdbcExactlyOnceOptions implements Serializable {

    private static final boolean DEFAULT_RECOVERED_AND_ROLLBACK = false;
    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 3;
    private static final boolean DEFAULT_ALLOW_OUT_OF_ORDER_COMMITS = false;

    private final boolean discoverAndRollbackOnRecovery;
    private final int maxCommitAttempts;
    private final boolean allowOutOfOrderCommits;
    private final Integer timeoutSec;

    private JdbcExactlyOnceOptions(
            boolean discoverAndRollbackOnRecovery,
            int maxCommitAttempts,
            boolean allowOutOfOrderCommits,
            Optional<Integer> timeoutSec) {
        this.discoverAndRollbackOnRecovery = discoverAndRollbackOnRecovery;
        this.maxCommitAttempts = maxCommitAttempts;
        this.allowOutOfOrderCommits = allowOutOfOrderCommits;
        this.timeoutSec = timeoutSec.orElse(null);
        Preconditions.checkArgument(this.maxCommitAttempts > 0, "maxCommitAttempts should be > 0");
    }

    public static JdbcExactlyOnceOptions defaults() {
        return builder().build();
    }

    public boolean isDiscoverAndRollbackOnRecovery() {
        return discoverAndRollbackOnRecovery;
    }

    public boolean isAllowOutOfOrderCommits() {
        return allowOutOfOrderCommits;
    }

    public int getMaxCommitAttempts() {
        return maxCommitAttempts;
    }

    public Integer getTimeoutSec() {
        return timeoutSec;
    }

    public static JDBCExactlyOnceOptionsBuilder builder() {
        return new JDBCExactlyOnceOptionsBuilder();
    }

    /** JDBCExactlyOnceOptionsBuilder. */
    public static class JDBCExactlyOnceOptionsBuilder {
        private boolean recoveredAndRollback = DEFAULT_RECOVERED_AND_ROLLBACK;
        private int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;
        private boolean allowOutOfOrderCommits = DEFAULT_ALLOW_OUT_OF_ORDER_COMMITS;
        private Optional<Integer> timeoutSec = Optional.empty();

        /** Toggle discovery and rollback of transactions upon recovery. */
        public JDBCExactlyOnceOptionsBuilder withRecoveredAndRollback(
                boolean recoveredAndRollback) {
            this.recoveredAndRollback = recoveredAndRollback;
            return this;
        }

        /**
         * Set the number of attempt to commit a transaction (takes effect only if transient failure
         * happens).
         */
        public JDBCExactlyOnceOptionsBuilder withMaxCommitAttempts(int maxCommitAttempts) {
            this.maxCommitAttempts = maxCommitAttempts;
            return this;
        }

        /**
         * Set whether transactions may be committed out-of-order in case of retries and this option
         * is enabled.
         */
        public JDBCExactlyOnceOptionsBuilder withAllowOutOfOrderCommits(
                boolean allowOutOfOrderCommits) {
            this.allowOutOfOrderCommits = allowOutOfOrderCommits;
            return this;
        }

        /** Set transaction timeout in seconds (vendor-specific). */
        public JDBCExactlyOnceOptionsBuilder withTimeoutSec(Optional<Integer> timeoutSec) {
            this.timeoutSec = timeoutSec;
            return this;
        }

        public JdbcExactlyOnceOptions build() {
            return new JdbcExactlyOnceOptions(
                    recoveredAndRollback, maxCommitAttempts, allowOutOfOrderCommits, timeoutSec);
        }
    }
}
