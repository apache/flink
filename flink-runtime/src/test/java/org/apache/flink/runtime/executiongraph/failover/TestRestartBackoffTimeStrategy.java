/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover;

import java.util.function.Supplier;

/** A RestartBackoffTimeStrategy implementation for tests. */
public class TestRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

    private boolean canRestart;

    private long backoffTime;

    private Supplier<Boolean> isNewAttempt;

    public TestRestartBackoffTimeStrategy(boolean canRestart, long backoffTime) {
        this(canRestart, backoffTime, () -> true);
    }

    public TestRestartBackoffTimeStrategy(
            boolean canRestart, long backoffTime, Supplier<Boolean> isNewAttempt) {
        this.canRestart = canRestart;
        this.backoffTime = backoffTime;
        this.isNewAttempt = isNewAttempt;
    }

    @Override
    public boolean canRestart() {
        return canRestart;
    }

    @Override
    public long getBackoffTime() {
        return backoffTime;
    }

    @Override
    public boolean notifyFailure(Throwable cause) {
        // ignore
        return isNewAttempt.get();
    }

    public void setCanRestart(final boolean canRestart) {
        this.canRestart = canRestart;
    }

    public void setBackoffTime(final long backoffTime) {
        this.backoffTime = backoffTime;
    }

    public void setIsNewAttempt(Supplier<Boolean> isNewAttempt) {
        this.isNewAttempt = isNewAttempt;
    }
}
